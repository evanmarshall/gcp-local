package iam

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the service-account control plane and account/key state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// New registers the IAM service-account provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("iam permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	if err := p.registerBackend(manager, engine, index); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend attaches the in-memory backend and its reset hook.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      provider.NoBackend,
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the IAM backend: %w", err)
	}
	return nil
}

// Reset clears the scope's accounts.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the iam scope: %w", err)
	}
	return nil
}

// handle is the gateway entry point for every IAM method. An addressed
// account's existence check and authorization share one scope lock, and
// delete removes the index entry under that lock, so a request racing a
// delete sees NOT_FOUND rather than an unverified IAM target.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid IAM request")
	}
	scope, target, account, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr, err := p.authorize(call, scope, target, account)
	if err != nil {
		return nil, err
	}
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return provider.Denied(decision, target.Name), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, target)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// authorize evaluates call on target. For an addressed account it checks
// existence and authorizes under one scope lock; a missing account is err.
func (p *Provider) authorize(call gateway.Call, scope state.Scope, target iam.Target, account string) (iam.Decision, error, error) {
	if account == "" {
		decision, authorizeErr := p.engine.Authorize(call, target, nil)
		return decision, authorizeErr, nil //nolint:wrapcheck // the caller maps the IAM status
	}
	var decision iam.Decision
	var authorizeErr error
	var exists bool
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if exists = p.accountExists(s, call.Method, scope, account); exists {
			decision, authorizeErr = p.engine.Authorize(call, target, nil)
		}
		return nil
	})
	if !exists {
		return decision, nil, provider.NotFound(kindAccount, account)
	}
	return decision, authorizeErr, nil //nolint:wrapcheck // the caller maps the IAM status
}

// resolve derives the scope and IAM target, and the account id a method
// addresses ("" for create and list). Account methods authorize against the
// account; key and list/create methods inherit via the account or project.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, string, error) {
	project := request.Path["project"]
	if call.Method == methodSignBlob && project == "-" {
		// IAM Credentials names accounts as projects/-/serviceAccounts/EMAIL;
		// the email's domain carries the owning project.
		_, domain, _ := strings.Cut(request.Path["serviceAccount"], "@")
		project = strings.TrimSuffix(domain, ".iam.gserviceaccount.com")
	}
	scope := provider.ProjectScope(project)
	target := iam.Target{Name: provider.ProjectResource(project)}
	// GCP addresses accounts by email; the provider keys them by account id.
	account := strings.TrimSuffix(request.Path["serviceAccount"], "@"+project+".iam.gserviceaccount.com")
	request.Path["serviceAccount"] = account
	if call.Method == methodAccountsCreate || call.Method == methodAccountsList {
		return scope, target, "", nil
	}
	// Every other method, including the key methods, acts on one existing account.
	if !uid.MatchString(account) {
		return scope, target, "", status.Errorf(codes.InvalidArgument, "Invalid service account id %q", account)
	}
	target.Name = accountResource(project, accountEmail(project, account))
	return scope, target, account, nil
}

// accountExists reports whether a method's account exists; it runs under the
// scope lock. signBlob also signs for accounts declared in the IAM
// configuration, which only the index holds; the account methods manage only
// accounts this provider created.
func (p *Provider) accountExists(s *scopeState, method string, scope state.Scope, account string) bool {
	if method == methodSignBlob {
		resource, found := p.index.Get(accountResource(scope.Project, accountEmail(scope.Project, account)))
		return found && resource.Kind == kindAccount && resource.Scope == scope
	}
	return s.accounts[account] != nil
}
