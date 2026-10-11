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
	if err := registerPermissions(engine); err != nil {
		return nil, err
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
		New:      func(state.Scope) (backend.Backend, error) { return emptyBackend{}, nil },
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

// emptyBackend satisfies the backend contract for an in-memory provider.
type emptyBackend struct{}

func (emptyBackend) Start(context.Context) error    { return nil }
func (emptyBackend) Ready(context.Context) error    { return nil }
func (emptyBackend) Stop(context.Context) error     { return nil }
func (emptyBackend) Reset(context.Context) error    { return nil }
func (emptyBackend) Snapshot(context.Context) error { return backend.ErrPersistenceUnsupported }
func (emptyBackend) Restore(context.Context) error  { return backend.ErrPersistenceUnsupported }

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

// registerPermissions binds every method to its permission.
func registerPermissions(engine *iam.Engine) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// handle is the gateway entry point for every IAM method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid IAM request")
	}
	scope, target, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return p.denied(call, decision, target), nil
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

// denied renders the denial shape.
func (p *Provider) denied(_ gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
	permission := "unspecified"
	for _, check := range decision.Checks {
		if !check.Allowed {
			permission = check.Permission
			break
		}
	}
	return wireError(403, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", "The caller", permission, target.Name))
}

// resolve derives the scope and IAM target. Account methods authorize against
// the account; key and list/create methods inherit via the account or project.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	// GCP addresses accounts by email; the provider keys them by account id.
	account := strings.TrimSuffix(request.Path["serviceAccount"], "@"+project+".iam.gserviceaccount.com")
	request.Path["serviceAccount"] = account
	switch call.Method {
	case methodAccountsCreate, methodAccountsList:
		return scope, target, nil
	case methodAccountsGet, methodAccountsUpdate, methodAccountsDelete, methodAccountsGetIam, methodAccountsSetIam:
		if !uid.MatchString(account) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid service account id %q", account)
		}
		if p.accountLocked(scope, account) == nil {
			return scope, target, notFound(kindAccount, account)
		}
		target.Name = accountResource(project, accountEmail(project, account))
		return scope, target, nil
	case methodKeysList:
		if !uid.MatchString(account) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid service account id %q", account)
		}
		if p.accountLocked(scope, account) == nil {
			return scope, target, notFound(kindAccount, account)
		}
		target.Name = accountResource(project, accountEmail(project, account))
		return scope, target, nil
	case methodKeysCreate:
		if !uid.MatchString(account) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid service account id %q", account)
		}
		if p.accountLocked(scope, account) == nil {
			return scope, target, notFound(kindAccount, account)
		}
		target.Name = accountResource(project, accountEmail(project, account))
		return scope, target, nil
	default:
		if !uid.MatchString(account) {
			return scope, target, status.Errorf(codes.InvalidArgument, "Invalid service account id %q", account)
		}
		if p.accountLocked(scope, account) == nil {
			return scope, target, notFound(kindAccount, account)
		}
		target.Name = accountResource(project, accountEmail(project, account))
		return scope, target, nil
	}
}
