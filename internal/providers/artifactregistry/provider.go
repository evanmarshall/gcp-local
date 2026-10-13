package artifactregistry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// operationLatency is the minimum time from a request to its operation
// reporting done. GCP's create and delete operations start not done
// (recorded); local work finishes at once, so the latency makes clients poll.
const operationLatency = 500 * time.Millisecond

// Provider owns the Artifact Registry control plane and repository state.
type Provider struct {
	index      *state.Index
	engine     *iam.Engine
	gate       provider.Gate
	scopes     *state.Store[scopeState]
	operations *provider.Operations
	clock      func() time.Time
}

// New registers the Artifact Registry provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("artifactregistry permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine,
		gate:   provider.Gate{Engine: engine, Index: index, Message: denial, Domain: "artifactregistry.googleapis.com"},
		scopes: state.NewStore(newScopeState, validLocation),
		// Operation names share Memorystore's REST path; the prefix routes them.
		operations: provider.NewOperations(validLocation, operationLatency, operationMetadata).WithPrefix(operationPrefix),
		clock:      time.Now,
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

// registerBackend attaches the metadata-only backend and its reset hook.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: validLocation,
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
		return fmt.Errorf("register the Artifact Registry backend: %w", err)
	}
	return nil
}

// Reset clears one location's repositories and operations.
func (p *Provider) Reset(scope state.Scope) error {
	if err := errors.Join(p.scopes.Reset(scope), p.operations.Reset(scope)); err != nil {
		return fmt.Errorf("clear the artifactregistry scope: %w", err)
	}
	return nil
}

// handle is the gateway entry point for every Artifact Registry method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Artifact Registry request")
	}
	scope := state.Scope{Project: request.Path[pathProject], Location: request.Path[pathLocation]}
	if err := validLocation(scope); err != nil {
		return nil, gap(fmt.Sprintf("location %q", scope.Location))
	}
	decision, target, err := p.authorize(call, request, scope)
	if err != nil {
		return nil, err
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, target)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// authorize checks the call before any state changes. Create and list are
// checked on the project and reported on the location (recorded); a
// repository method is checked on the repository when it exists, else on the
// project, under the scope lock delete also holds, so a missing repository
// reads 404 only for a caller allowed to see it.
func (p *Provider) authorize(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (iam.Decision, iam.Target, error) {
	project := iam.Target{Name: provider.ProjectResource(scope.Project)}
	switch call.Method {
	case methodReposCreate, methodReposList:
		decision, err := p.gate.Check(call, project, provider.LocationName(scope))
		return decision, project, err
	case methodOpsGet:
		decision, err := p.gate.Check(call, project, provider.LocationName(scope))
		if status.Code(err) == codes.PermissionDenied {
			err = operationDenied(scope, operationPrefix+request.Path[pathOperation])
		}
		return decision, project, err
	default:
		repository := request.Path[pathRepository]
		if !uid.MatchString(repository) {
			return iam.Decision{}, project, status.Errorf(codes.InvalidArgument, "Invalid repository id %q", repository)
		}
		decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, repositoryName(scope, repository),
			repositoryResource(scope, repository), project.Name)
		return decision, target, err
	}
}

// execute dispatches one authorized request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	repository := request.Path[pathRepository]
	switch call.Method {
	case methodReposCreate:
		return p.repositoriesCreate(request, scope)
	case methodReposGet:
		return p.repositoriesGet(scope, repository)
	case methodReposList:
		return p.repositoriesList(request, scope)
	case methodReposUpdate:
		return p.repositoriesUpdate(request, scope)
	case methodReposDelete:
		return p.repositoriesDelete(scope, repository)
	case methodOpsGet:
		return p.operation(scope, operationPrefix+request.Path[pathOperation])
	default:
		// The policy is read or written under the repository's lock, so a
		// racing delete cannot unregister it in between.
		var response *gateway.RESTResponse
		err := p.withRepository(scope, repository, func(*scopeState, *repositoryRecord) error {
			var policyErr error
			if call.Method == methodReposGetIam {
				response, policyErr = p.getIam(request, target)
			} else {
				response, policyErr = p.setIam(request, target)
			}
			return policyErr
		})
		return response, err
	}
}
