package artifactregistry

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	case methodReposGetIam, methodReposSetIam:
		// The policy is read or written under the repository's lock, so a
		// racing delete cannot unregister it in between.
		var response *gateway.RESTResponse
		err := p.withRepository(scope, request.Path["repository"], func(*scopeState, *repositoryRecord) error {
			var policyErr error
			if call.Method == methodReposGetIam {
				response, policyErr = p.getIam(target)
			} else {
				response, policyErr = p.setIam(request, target)
			}
			return policyErr
		})
		return response, err
	default:
		return p.repositoriesDelete(request, scope)
	}
}

// repositoryID reads the new repository's id from the repositoryId query
// parameter (either spelling), falling back to the last segment of the body name.
func repositoryID(query url.Values, bodyName string) string {
	if id := cmp.Or(query.Get("repositoryId"), query.Get("repository_id")); id != "" {
		return id
	}
	if slash := strings.LastIndexByte(bodyName, '/'); slash >= 0 && slash < len(bodyName)-1 {
		return bodyName[slash+1:]
	}
	return bodyName
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
	name := repositoryID(request.Query, body.Name)
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid repository id %q", name)
	}
	format := cmp.Or(body.Format, "DOCKER")
	if !supportedFormats[format] {
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported repository format %q", format)
	}
	location := request.Path["location"]
	now := p.clock()
	var response map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.repositories[name] != nil {
			return status.Errorf(codes.AlreadyExists, "Repository %s already exists", name)
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: repositoryResource(scope.Project, name), Service: serviceName,
			Kind: kindRepository, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return fmt.Errorf("index the repository: %w", err)
		}
		record := &repositoryRecord{
			Name: name, Location: location, Format: format, Mode: cmp.Or(body.Mode, "STANDARD_REPOSITORY"),
			Description: body.Description, CleanupPolicyDryRun: body.CleanupPolicyDryRun,
			Labels: body.Labels, CreatedAt: now, UpdatedAt: now,
		}
		s.repositories[name] = record
		response = repositoryWire(scope.Project, record, viewCreate)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	response["@type"] = repositoryType
	return &gateway.RESTResponse{JSON: operationWire(scope.Project, location, response)}, nil
}

// Repository views: GCP returns different field sets per method.
const (
	viewCreate = iota // create operation response: no times or registry URI
	viewPatch         // patch response: times but no registry URI
	viewGet           // get/list: every field
)

// fieldDescription is the update-mask path of the repository description.
const fieldDescription = "description"

// Operation and response type URLs of the Artifact Registry LRO envelope.
const (
	repositoryType = "type.googleapis.com/google.devtools.artifactregistry.v1.Repository"
	emptyType      = "type.googleapis.com/google.protobuf.Empty"
	metadataType   = "type.googleapis.com/google.devtools.artifactregistry.v1.OperationMetadata"
)

// operationWire wraps a completed result in the long-running operation envelope.
func operationWire(project, location string, response map[string]any) map[string]any {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return map[string]any{
		keyName:    "projects/" + project + "/locations/" + location + "/operations/" + hex.EncodeToString(id),
		"metadata": map[string]any{"@type": metadataType},
		"done":     true,
		"response": response,
	}
}

// repositoryWire renders one repository document for the given view. The
// satisfiesPzi and scanning values are local stand-ins; the recording pins
// only that the fields are present.
func repositoryWire(project string, record *repositoryRecord, view int) map[string]any {
	labels := maps.Clone(record.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	document := map[string]any{
		keyName:                       repositoryName(project, record.Location, record.Name),
		"format":                      record.Format,
		"mode":                        record.Mode,
		"description":                 record.Description,
		"labels":                      labels,
		"satisfiesPzi":                false,
		"vulnerabilityScanningConfig": map[string]any{"enablementState": "SCANNING_DISABLED"},
	}
	if record.CleanupPolicyDryRun {
		document["cleanupPolicyDryRun"] = true
	}
	if view != viewCreate {
		document["createTime"] = provider.Stamp(record.CreatedAt)
		document["updateTime"] = provider.Stamp(record.UpdatedAt)
	}
	if view == viewGet {
		document["registryUri"] = registryURI(project, record)
	}
	return document
}

// registryURI renders the repository's package host path.
func registryURI(project string, record *repositoryRecord) string {
	return record.Location + "-" + strings.ToLower(record.Format) + ".pkg.dev/" + project + "/" + record.Name
}

// withRepository runs use on one repository under the scope lock, or reports
// it missing. use must not retain the record.
func (p *Provider) withRepository(scope state.Scope, repository string, use func(*scopeState, *repositoryRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback's own errors
		record := s.repositories[repository]
		if record == nil {
			return provider.NotFound(kindRepository, repository)
		}
		return use(s, record)
	})
}

func (p *Provider) repositoriesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withRepository(scope, request.Path["repository"], func(_ *scopeState, record *repositoryRecord) error {
		document = repositoryWire(scope.Project, record, viewGet)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// repositoriesList returns the location's repositories.
func (p *Provider) repositoriesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	location := request.Path["location"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.repositories {
			if record.Location == location {
				items = append(items, repositoryWire(scope.Project, record, viewGet))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"repositories": items}}, nil
}

// repositoryUpdate is a validated patch: the masked fields and their values.
type repositoryUpdate struct {
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
	fields      []string
}

// parseRepositoryUpdate validates the whole body and mask before any field
// changes. Only masked fields change; without a mask GCP updates every field sent.
func parseRepositoryUpdate(request *gateway.RESTRequest) (repositoryUpdate, error) {
	var update repositoryUpdate
	if json.Unmarshal(request.JSON, &update) != nil {
		return update, status.Error(codes.InvalidArgument, "Invalid repository update")
	}
	mask := request.Query.Get("updateMask")
	if mask == "" {
		mask = "description,labels"
	}
	for field := range strings.SplitSeq(mask, ",") {
		switch strings.TrimSpace(field) {
		case fieldDescription, "labels":
			update.fields = append(update.fields, strings.TrimSpace(field))
		default:
			return update, status.Errorf(codes.InvalidArgument, "Unsupported update mask field %q", field)
		}
	}
	return update, nil
}

// repositoriesUpdate applies a validated patch all-or-nothing. A missing
// repository is reported before an invalid patch, as before validation moved.
func (p *Provider) repositoriesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	update, invalid := parseRepositoryUpdate(request)
	now := p.clock()
	var document map[string]any
	if err := p.withRepository(scope, request.Path["repository"], func(_ *scopeState, record *repositoryRecord) error {
		if invalid != nil {
			return invalid
		}
		for _, field := range update.fields {
			if field == fieldDescription {
				record.Description = update.Description
			} else {
				record.Labels = update.Labels
			}
		}
		record.UpdatedAt = now
		document = repositoryWire(scope.Project, record, viewPatch)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// repositoriesDelete removes one repository and its index entry together.
func (p *Provider) repositoriesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	repository := request.Path["repository"]
	if err := p.withRepository(scope, repository, func(s *scopeState, _ *repositoryRecord) error {
		if err := p.index.Remove(serviceName, scope, repositoryResource(scope.Project, repository)); err != nil {
			return fmt.Errorf("unregister the repository: %w", err)
		}
		delete(s.repositories, repository)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: operationWire(scope.Project, request.Path["location"], map[string]any{"@type": emptyType})}, nil
}

// getIam returns the stored policy.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
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
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
