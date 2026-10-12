// Package secretmanager implements the Secret Manager v1 control plane:
// secrets, versions, payload access, and per-secret IAM policy. Payloads stay
// in provider state and reset with the scope. No cloud recording covers these
// methods yet; issue #37 tracks the recording and the Terraform workflow pin.
package secretmanager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method name constants; the generated route table uses the same identifiers.
const (
	methodSecretsCreate   = "secretmanager.secrets.create"
	methodSecretsGet      = "secretmanager.secrets.get"
	methodSecretsList     = "secretmanager.secrets.list"
	methodSecretsUpdate   = "secretmanager.secrets.update"
	methodSecretsDelete   = "secretmanager.secrets.delete"
	methodSecretsGetIam   = "secretmanager.secrets.getIamPolicy"
	methodSecretsSetIam   = "secretmanager.secrets.setIamPolicy"
	methodVersionsAdd     = "secretmanager.versions.add"
	methodVersionsList    = "secretmanager.versions.list"
	methodVersionsGet     = "secretmanager.versions.get"
	methodVersionsAccess  = "secretmanager.versions.access"
	methodVersionsDestroy = "secretmanager.versions.destroy"
	methodVersionsDisable = "secretmanager.versions.disable"
	methodVersionsEnable  = "secretmanager.versions.enable"
	methodLocationsList   = "secretmanager.locations.list"
)

// permissions follows the documented Secret Manager access-control rows.
var permissions = map[string]string{
	methodSecretsCreate:   "secretmanager.secrets.create",
	methodSecretsGet:      "secretmanager.secrets.get",
	methodSecretsList:     "secretmanager.secrets.list",
	methodSecretsUpdate:   "secretmanager.secrets.update",
	methodSecretsDelete:   "secretmanager.secrets.delete",
	methodSecretsGetIam:   "secretmanager.secrets.getIamPolicy",
	methodSecretsSetIam:   "secretmanager.secrets.setIamPolicy",
	methodVersionsAdd:     "secretmanager.versions.add",
	methodVersionsList:    "secretmanager.versions.list",
	methodVersionsGet:     "secretmanager.versions.get",
	methodVersionsAccess:  "secretmanager.versions.access",
	methodVersionsDestroy: "secretmanager.versions.destroy",
	methodVersionsDisable: "secretmanager.versions.disable",
	methodVersionsEnable:  "secretmanager.versions.enable",
	methodLocationsList:   "secretmanager.locations.list",
}

// Provider owns the Secret Manager control plane and payload state.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("secretmanager requires registry, manager, index, and iam")

// New registers the Secret Manager provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("secretmanager permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine,
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
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
		return nil, fmt.Errorf("register the Secret Manager backend: %w", err)
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// Reset clears the scope's secrets, versions, and payloads.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the secretmanager scope: %w", err)
	}
	return nil
}

// secretRecord is one stored secret and its versions.
type secretRecord struct {
	Name        string
	Replication map[string]any
	Versions    map[string]*versionRecord
	Labels      map[string]string
	CreatedAt   time.Time
	Revision    int
}

// versionRecord is one stored secret version.
type versionRecord struct {
	Number      string
	Payload     []byte
	State       string // ENABLED, DISABLED, DESTROYED
	CreatedAt   time.Time
	DestroyedAt time.Time
	Revision    int
}

// scopeState is one project slice of provider state.
type scopeState struct {
	secrets map[string]*secretRecord
}

func newScopeState() scopeState { return scopeState{secrets: map[string]*secretRecord{}} }

func (s *scopeState) reset() { s.secrets = map[string]*secretRecord{} }

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Secret Manager")

// numericProject matches a project number in a resource path.
var numericProject = regexp.MustCompile(`^[0-9]+$`)

// wireProject is the project number GCP puts in returned resource names,
// falling back to the ID when the index has no number.
func (p *Provider) wireProject(project string) string {
	if resource, ok := p.index.Get(provider.ProjectResource(project)); ok && resource.Number != "" {
		return resource.Number
	}
	return project
}

// resolve derives the scope and IAM target for one request, and the secret id
// it addresses ("" for project-level methods).
func (p *Provider) resolve(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, string, error) {
	project := request.Path["project"]
	// Names GCP returns carry the project number; callers send them back.
	if resource, ok := p.index.ProjectByNumber(project); ok && numericProject.MatchString(project) {
		project = resource.Scope.Project
		request.Path["project"] = project
	}
	scope := provider.ProjectScope(project)
	target := iam.Target{Name: provider.ProjectResource(project)}
	switch call.Method {
	case methodSecretsCreate, methodSecretsList, methodLocationsList:
		return scope, target, "", nil
	default:
		id := request.Path["secret"]
		if !uidPattern.MatchString(id) {
			return scope, target, "", status.Errorf(codes.InvalidArgument, "Invalid secret name %q", id)
		}
		target.Name = secretResource(project, id)
		return scope, target, id, nil
	}
}

// withSecret runs use on one secret under the scope lock, or reports it
// missing. use must not retain the record or anything it points to.
func (p *Provider) withSecret(scope state.Scope, id string, use func(*scopeState, *secretRecord) error) error {
	var opened bool
	err := p.scopes.With(scope, func(s *scopeState) error {
		opened = true
		record := s.secrets[id]
		if record == nil {
			return provider.NotFound("secrets", id)
		}
		return use(s, record)
	})
	if err != nil && !opened {
		return fmt.Errorf("open the secretmanager scope: %w", err)
	}
	return err //nolint:wrapcheck // the callback's own error
}

// handle is the gateway entry point for every Secret Manager method. An
// addressed secret's existence check and authorization share one scope lock,
// and delete removes the index entry under that lock, so a request racing a
// delete sees NOT_FOUND rather than an unverified IAM target.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Secret Manager request")
	}
	scope, target, id, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	var decision iam.Decision
	var authorizeErr error
	authorize := func(*scopeState, *secretRecord) error {
		decision, authorizeErr = p.engine.Authorize(call, target, nil)
		return nil
	}
	if id == "" {
		_ = authorize(nil, nil)
	} else if err := p.withSecret(scope, id, authorize); err != nil {
		return nil, err
	}
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return p.denied(call, decision, target), nil
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// denied renders the Secret Manager denial shape.
func (p *Provider) denied(call gateway.Call, decision iam.Decision, target iam.Target) *gateway.RESTResponse {
	permission := provider.DeniedPermission(decision)
	actor := call.Principal.Subject
	code := 403
	if call.Principal.State != gateway.Authenticated {
		actor, code = "Anonymous caller", 401
	}
	return provider.WireError(code, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", actor, permission, target.Name))
}
