package firestore

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the Firestore authentication and authorization gate. The
// official emulator owns documents; the provider owns collection-group field
// configuration, which the emulator does not implement.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	manager *backend.Manager
	scopes  *state.Store[scopeState]
	clock   func() time.Time
}

// New registers the Firestore provider backed by the official emulator.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	return newProvider(registry, manager, index, engine, func(scope state.Scope) (backend.Backend, error) {
		return &emulator{config: config, scope: scope}, nil
	})
}

func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil || factory == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine, manager: manager,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	if err := p.registerBackend(factory); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend attaches the per-project emulator and its reset hook.
func (p *Provider) registerBackend(factory func(state.Scope) (backend.Backend, error)) error {
	if err := p.manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      factory,
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			p.engine.ClearScope(serviceName, scope)
			p.index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the Firestore backend: %w", err)
	}
	return nil
}

// Reset clears the scope's provider-owned field configuration. The backend
// manager has already stopped the emulator, which discards its documents.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the firestore scope: %w", err)
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

// handle is the gateway entry point for every Firestore method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Firestore request")
	}
	scope, target, err := p.resolve(call, request)
	p.ensureDatabase(scope)
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
	var response *gateway.RESTResponse
	err = p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error {
		b, ok := active.(driver)
		if !ok {
			return status.Error(codes.Internal, "Invalid Firestore backend")
		}
		var executeErr error
		response, executeErr = p.execute(ctx, b, call, request, scope)
		return executeErr
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // keep the emulator or manager status unchanged on the wire
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// denied renders the Firestore denial shape.
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

// resolve derives the scope and IAM target for one request.
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := scopeOf(project)
	target := iam.Target{Name: projectResource(project)}
	collection, id := request.Path["collectionId"], request.Path["documentId"]
	if call.Method == methodFieldsGet || call.Method == methodFieldsUpdate {
		// Field methods are database-scoped like the documents.
		target.Name = databaseResource(project)
		return scope, target, nil
	}
	if !uid.MatchString(collection) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid collection id %q", collection)
	}
	// Firestore IAM attaches to the database: every document method
	// authorizes against the default database resource, which anchors the
	// hierarchy like a storage bucket anchors its objects.
	target.Name = databaseResource(project)
	if call.Method == methodDocumentsList || call.Method == methodDocumentsCreate {
		// The collection-level create takes the document id from the query.
		return scope, target, nil
	}
	if !uid.MatchString(id) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid document id %q", id)
	}
	return scope, target, nil
}

// ensureDatabase registers the project's default database resource once, so
// document authorization anchors on it. The call is idempotent per project.
func (p *Provider) ensureDatabase(scope state.Scope) {
	name := databaseResource(scope.Project)
	if _, ok := p.index.Get(name); ok {
		return
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: name, Service: serviceName,
		Kind: "database", Parent: projectResource(scope.Project), Scope: scope,
	}); err != nil {
		return
	}
}
