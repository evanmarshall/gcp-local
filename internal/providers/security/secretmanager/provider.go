// Package secretmanager implements the Secret Manager v1 control plane:
// secrets, versions, payload access, and per-secret IAM policy. Payloads stay
// in provider state and reset with the scope. Lifecycle policies and event
// notifications are in lifecycle.go.
package secretmanager

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
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

// Provider owns the Secret Manager control plane and payload state. clock
// and until are the time source of the lifecycle workers; tests replace them.
// until fires at an absolute instant, so a clock that moves between a
// worker's tick and its wait can't lose the wake-up.
type Provider struct {
	index    *state.Index
	engine   *iam.Engine
	gate     provider.Gate
	manager  *backend.Manager
	registry *gateway.Registry
	lifetime context.Context //nolint:containedctx // the lifecycle workers' process lifetime
	scopes   *state.Store[scopeState]
	clock    func() time.Time
	until    func(time.Time) <-chan time.Time
	mu       sync.Mutex
	workers  map[string]context.CancelFunc
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("secretmanager requires registry, manager, index, and iam")

// New registers the Secret Manager provider.
func New(ctx context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := errors.Join(provider.RegisterPermissions(engine, permissions),
		provider.RegisterPermissions(engine, map[string]string{methodTopicPublish: "pubsub.topics.publish"})); err != nil {
		return nil, fmt.Errorf("secretmanager permissions: %w", err)
	}
	p := &Provider{
		index: index, engine: engine, manager: manager, registry: registry, lifetime: context.WithoutCancel(ctx),
		until: func(at time.Time) <-chan time.Time { return time.After(time.Until(at)) }, workers: map[string]context.CancelFunc{},
		// Recorded: secretmanager-access-lifecycle "denied-access".
		gate: provider.Gate{Engine: engine, Index: index, Message: provider.PermissionDeniedUnnamed,
			Domain: "iam.googleapis.com", OmitResource: true},
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

// Reset stops the scope's workers and clears its secrets, versions, and
// payloads.
func (p *Provider) Reset(scope state.Scope) error {
	p.stopScope(scope)
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the secretmanager scope: %w", err)
	}
	return nil
}

// secretRecord is one stored secret, its lifecycle policies, and its
// versions.
type secretRecord struct {
	policies

	Name        string
	Versions    map[string]*versionRecord
	Labels      map[string]string
	Annotations map[string]string
	CreatedAt   time.Time
	Revision    int
}

// versionRecord is one stored secret version. ClientChecksum records that
// the add request carried a verified CRC32C; ScheduledDestroy is set while a
// delayed destruction is pending.
type versionRecord struct {
	Number           string
	Payload          []byte
	State            string // ENABLED, DISABLED, DESTROYED
	CreatedAt        time.Time
	DestroyedAt      time.Time
	ScheduledDestroy time.Time
	Revision         int
	ClientChecksum   bool
}

// scopeState is one project slice of provider state. outbox holds the
// notifications of the current mutation until they publish outside the lock.
type scopeState struct {
	secrets map[string]*secretRecord
	outbox  []event
}

func newScopeState() scopeState { return scopeState{secrets: map[string]*secretRecord{}} }

func (s *scopeState) reset() { s.secrets, s.outbox = map[string]*secretRecord{}, nil }

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

// handle is the gateway entry point for every Secret Manager method. Each
// request runs inside the manager session, which serializes it with a reset,
// and authorizes, looks up, changes, and renders the secret under one scope
// lock, so nothing changes between the check and the call.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Secret Manager request")
	}
	scope, target, id, err := p.resolve(call, request)
	if err != nil {
		return nil, err
	}
	var response *gateway.RESTResponse
	var events []event
	err = p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, _ backend.Backend) error {
		call.Context = ctx                                      //nolint:fatcontext // one request-scoped handoff of the manager session
		return p.scopes.With(scope, func(s *scopeState) error { //nolint:contextcheck // lifecycle workers outlive the request by design
			var serveErr error
			response, serveErr = p.serve(call, request, scope, target, id, s)
			events = s.drain()
			return serveErr
		})
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses pass through
	}
	p.publish(call.Context, scope.Project, events)
	return response, nil
}

// serve authorizes one request, then runs it. A secret that does not exist is
// authorized on its project, so a caller without permission is denied whether
// or not it exists and only a permitted caller learns it is missing
// (recorded: secretmanager-access-lifecycle "absent-denied").
func (p *Provider) serve(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target, id string, s *scopeState) (*gateway.RESTResponse, error) {
	record := s.secrets[id]
	missing := id != "" && record == nil
	checked := target
	if missing {
		checked.Name = provider.ProjectResource(scope.Project)
	}
	if id != "" {
		checked.Attributes = p.attributes(call.Method, scope.Project, id, request.Path["version"])
	}
	decision, err := p.gate.Check(call, checked, checked.Attributes.Name)
	if err != nil {
		return nil, err
	}
	if missing {
		return nil, provider.NotFound("secrets", id)
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope, s, record)
	if err != nil {
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// versionMethods are checked on the addressed version; the other secret
// methods on the secret.
var versionMethods = map[string]bool{
	methodVersionsGet: true, methodVersionsAccess: true, methodVersionsDestroy: true,
	methodVersionsDisable: true, methodVersionsEnable: true,
}

// attributes are the IAM condition attributes of the secret or version a
// method is checked on, named with the project number and the version as
// requested. Recorded "conditional-access": a SecretVersion condition on
// versions/2 grants access to versions/2 but not to versions/latest, and a
// Secret condition grants the secret and its version list.
func (p *Provider) attributes(method, project, id, version string) iam.ResourceAttributes {
	name := secretWireName(p.wireProject(project), id)
	if versionMethods[method] {
		return iam.ResourceAttributes{Name: name + "/versions/" + version,
			Type: "secretmanager.googleapis.com/SecretVersion", Service: "secretmanager.googleapis.com"}
	}
	return iam.ResourceAttributes{Name: name, Type: "secretmanager.googleapis.com/Secret", Service: "secretmanager.googleapis.com"}
}
