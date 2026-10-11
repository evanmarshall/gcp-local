package artifactregistry

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodReposCreate:
		return p.repositoriesCreate(request, scope)
	case methodReposGet:
		return p.repositoriesGet(request, scope)
	case methodReposList:
		return p.repositoriesList(request, scope)
	case methodReposUpdate:
		return p.repositoriesUpdate(request, scope)
	case methodReposGetIam:
		return p.getIam(target)
	case methodReposSetIam:
		return p.setIam(request, target)
	default:
		return p.repositoriesDelete(request, scope)
	}
}

// repositoriesCreate validates and stores one repository. The real create
// takes the repositoryId as a query parameter.
func (p *Provider) repositoriesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		Name                string            `json:"name"`
		Format              string            `json:"format"`
		Description         string            `json:"description"`
		Mode                string            `json:"mode"`
		CleanupPolicyDryRun bool              `json:"cleanupPolicyDryRun"`
		Labels              map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid repository body")
	}
	name := request.Query.Get("repositoryId")
	if name == "" {
		name = request.Query.Get("repository_id")
	}
	if name == "" && body.Name != "" {
		if index := len(body.Name) - 1; index >= 0 {
			for i := len(body.Name) - 1; i >= 0; i-- {
				if body.Name[i] == '/' {
					name = body.Name[i+1:]
					break
				}
			}
			if name == "" {
				name = body.Name
			}
		}
	}
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid repository id %q", name)
	}
	format := body.Format
	if format == "" {
		format = "DOCKER"
	}
	if !supportedFormats[format] {
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported repository format %q", format)
	}
	location := request.Path["location"]
	now := p.clock()
	var created *repositoryRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.repositories[name] != nil {
			return status.Errorf(codes.AlreadyExists, "Repository %s already exists", name)
		}
		mode := body.Mode
		if mode == "" {
			mode = "STANDARD_REPOSITORY"
		}
		created = &repositoryRecord{
			Name: name, Location: location, Format: format, Mode: mode,
			Description: body.Description, CleanupPolicyDryRun: body.CleanupPolicyDryRun,
			Labels: body.Labels, CreatedAt: now,
		}
		s.repositories[name] = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: repositoryResource(scope.Project, name), Service: serviceName,
		Kind: kindRepository, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the repository: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"done": true, "response": repositoryWire(scope.Project, created),
	}}, nil
}

// repositoryWire renders one repository document.
func repositoryWire(project string, record *repositoryRecord) map[string]any {
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:               repositoryName(project, record.Location, record.Name),
		"format":              record.Format,
		"mode":                record.Mode,
		"cleanupPolicyDryRun": record.CleanupPolicyDryRun,
		"description":         record.Description,
		"labels":              labels,
		"createTime":          stamp(record.CreatedAt),
	}
}

// repositoryLocked reads one repository under the scope lock.
func (p *Provider) repositoryLocked(scope state.Scope, repository string) *repositoryRecord {
	var record *repositoryRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.repositories[repository]; return nil })
	return record
}

// repositoriesGet returns one repository.
func (p *Provider) repositoriesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.repositoryLocked(scope, request.Path["repository"])
	if record == nil {
		return nil, notFound(kindRepository, request.Path["repository"])
	}
	return &gateway.RESTResponse{JSON: repositoryWire(scope.Project, record)}, nil
}

// repositoriesList returns the location's repositories.
func (p *Provider) repositoriesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	location := request.Path["location"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.repositories {
			if record.Location == location {
				items = append(items, repositoryWire(scope.Project, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"repositories": items}}, nil
}

// repositoriesUpdate replaces description and labels.
func (p *Provider) repositoriesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.repositoryLocked(scope, request.Path["repository"])
	if record == nil {
		return nil, notFound(kindRepository, request.Path["repository"])
	}
	var body struct {
		Description string            `json:"description"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid repository update")
	}
	record.Description = body.Description
	record.Labels = body.Labels
	return &gateway.RESTResponse{JSON: repositoryWire(scope.Project, record)}, nil
}

// repositoriesDelete removes one repository.
func (p *Provider) repositoriesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	repository := request.Path["repository"]
	if p.repositoryLocked(scope, repository) == nil {
		return nil, notFound(kindRepository, repository)
	}
	var indexErr error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.repositories, repository)
		indexErr = p.index.Remove(serviceName, scope, repositoryResource(scope.Project, repository))
		return nil
	})
	if indexErr != nil {
		return nil, fmt.Errorf("unregister the repository: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: http.StatusNoContent}, nil
}

// getIam returns the stored policy.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores the policy.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
