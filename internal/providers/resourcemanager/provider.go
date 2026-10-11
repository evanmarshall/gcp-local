package resourcemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider owns the project-level IAM policy surface.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
}

// New registers the resourcemanager provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	p := &Provider{index: index, engine: engine}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
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

// handle is the gateway entry point.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid resourcemanager request")
	}
	project := request.Path["project"]
	if !projectIDPattern.MatchString(project) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid project id %q", project)
	}
	target := iam.Target{Name: projectResource(project)}
	if _, found := p.index.Get(target.Name); !found {
		return nil, notFound(project)
	}
	decision, authorizeErr := p.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		code := status.Code(authorizeErr)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		permission := "unspecified"
		for _, check := range decision.Checks {
			if !check.Allowed {
				permission = check.Permission
				break
			}
		}
		return wireError(403, "PERMISSION_DENIED", fmt.Sprintf("%s does not have %s access to %s.", "The caller", permission, target.Name)), nil
	}
	call.Resource = target.Name
	response, err := p.execute(call, request, target)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response, nil
}

// execute dispatches one authorized request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodProjectsGetIam:
		policy, err := p.engine.GetPolicy(target.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read the project policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy}, nil
	default:
		var envelope struct {
			Policy iam.Policy `json:"policy"`
		}
		if json.Unmarshal(request.JSON, &envelope) != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
		}
		policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "write the project policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy}, nil
	}
}
