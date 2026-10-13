package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// Provider owns the first networking control-plane resources.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	gate    provider.Gate
	scopes  *state.Store[scopeState]
	clock   func() time.Time
	records map[string]*operationRecord
	mu      sync.RWMutex
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("compute requires registry, manager, index, and iam")

// New registers the compute provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{
		index: index, engine: engine,
		gate:    provider.Gate{Engine: engine, Index: index, Message: provider.PermissionDeniedOn, Attributes: attributes},
		scopes:  state.NewStore(newScopeState, state.GlobalScope),
		records: map[string]*operationRecord{},
		clock:   time.Now,
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
		return nil, fmt.Errorf("register the compute backend: %w", err)
	}
	return p, p.register(registry)
}

// register binds every served method to handle and every other Compute path
// to its explicit gap.
func (p *Provider) register(registry *gateway.Registry) error {
	for _, method := range methodNames() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	if err := registry.Register(methodUnmodeled, unmodeled); err != nil {
		return fmt.Errorf("register %s: %w", methodUnmodeled, err)
	}
	return nil
}

// methodUnmodeled is every Compute v1 path no other route serves.
const methodUnmodeled = "compute.unmodeled"

// unmodeled refuses a Compute method the provider doesn't serve, naming the
// feature when it is a known gap. It changes nothing, so it needs no
// authorization.
func unmodeled(_ gateway.Call, raw any) (any, error) {
	request, _ := raw.(*gateway.RESTRequest)
	path := ""
	if request != nil {
		path = request.Path["path"]
	}
	switch {
	case strings.Contains(path, "firewallPolicies"):
		return nil, gap("hierarchical and network firewall policies are not modeled")
	case strings.Contains(strings.ToLower(path), "xpn"):
		return nil, gap("Shared VPC is not modeled")
	default:
		return nil, gap("this Compute method is not implemented")
	}
}

// Reset clears the scope's topology state and its operation records.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the compute scope: %w", err)
	}
	p.mu.Lock()
	for name := range p.records {
		if strings.HasPrefix(name, scope.Project+"/") {
			delete(p.records, name)
		}
	}
	p.mu.Unlock()
	return nil
}

// handle is the gateway entry point for every compute method. It authorizes
// at the addressed resource when it exists (so its own policy and IAM
// Conditions apply) and at the project otherwise, under the scope lock that
// every index change holds.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid compute request")
	}
	// Regions are placement attributes, not namespaces (the storage
	// precedent): every compute resource lives in the project's global scope
	// so network references resolve across regions.
	project := request.Path["project"]
	scope := provider.ProjectScope(project)
	candidates := append(addressed(request.Path), provider.ProjectResource(project))
	decision, target, err := provider.Nearest(p.gate, p.scopes, scope, call, "", candidates...)
	if err != nil {
		if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
			return p.denied(call, decision, target)
		}
		return nil, err
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		if strings.HasPrefix(call.Method, "compute.") {
			if refusal := computeResponse(err); refusal != nil {
				return refusal, nil
			}
		}
		return nil, err
	}
	return provider.WithDecision(response, decision), nil
}

// addressed names the IAM target of the resource a request's path addresses,
// or none for a collection or project method.
func addressed(path map[string]string) []string {
	project := path["project"]
	switch {
	case path["instance"] != "":
		return []string{instanceName(project, path[keyZone], path["instance"])}
	case path["subnetwork"] != "":
		return []string{regionalName(project, path["region"], collectionSubnetworks, path["subnetwork"])}
	case path["router"] != "":
		return []string{regionalName(project, path["region"], collectionRouters, path["router"])}
	case path["network"] != "":
		return []string{globalName(project, collectionNetworks, path["network"])}
	case path["firewall"] != "":
		return []string{globalName(project, collectionFirewalls, path["firewall"])}
	case path["route"] != "":
		return []string{globalName(project, collectionRoutes, path["route"])}
	}
	return nil
}

// attributes supplies the IAM Conditions resource.name and resource.service
// of a Compute resource target, and nothing for the project. Compute's
// resource.type isn't "compute.googleapis.com/Subnetwork" for a subnetwork
// and isn't modeled, so a condition on it grants nothing (recorded:
// compute-lists-operations-iam).
func attributes(target, _ string) iam.ResourceAttributes {
	name, found := strings.CutPrefix(target, "//compute.googleapis.com/")
	if !found {
		return iam.ResourceAttributes{}
	}
	return iam.ResourceAttributes{Name: name, Service: "compute.googleapis.com"}
}

// denied renders the compute denial shape; Network Management methods use the
// standard Google API error shape instead.
func (p *Provider) denied(call gateway.Call, decision iam.Decision, target iam.Target) (*gateway.RESTResponse, error) {
	permission := provider.DeniedPermission(decision)
	if call.Principal.State != gateway.Authenticated {
		if strings.HasPrefix(call.Method, "networkmanagement.") {
			return nil, status.Error(codes.Unauthenticated, "The request does not have valid authentication credentials")
		}
		return wireError(401, "forbidden", "Anonymous caller does not have "+permission+" access to "+target.Name+"."), nil
	}
	if strings.HasPrefix(call.Method, "networkmanagement.") {
		return nil, status.Errorf(codes.PermissionDenied, "Permission '%s' denied on '%s'", permission, target.Name)
	}
	return wireError(403, "forbidden", fmt.Sprintf("%s does not have %s access to %s.", call.Principal.Subject, permission, target.Name)), nil
}
