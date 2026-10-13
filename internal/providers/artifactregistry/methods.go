package artifactregistry

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// repositoryBody is the modeled part of the v1 Repository message.
type repositoryBody struct {
	Name                        string            `json:"name"`
	Format                      string            `json:"format"`
	Description                 string            `json:"description"`
	Labels                      map[string]string `json:"labels"`
	Mode                        string            `json:"mode"`
	CleanupPolicyDryRun         bool              `json:"cleanupPolicyDryRun"`
	VulnerabilityScanningConfig *struct{}         `json:"vulnerabilityScanningConfig"`
}

// repositoryFields lists the v1 Repository fields repositoryBody lacks
// (https://cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories).
var repositoryFields = provider.Fields{
	Unmodeled: []string{
		"kmsKeyName", "cleanupPolicies", "mavenConfig", "dockerConfig", "virtualRepositoryConfig",
		"remoteRepositoryConfig", "disallowUnspecifiedMode", "vulnerabilityScanningConfig.enablementConfig",
	},
	OutputOnly: []string{
		"createTime", "updateTime", "sizeBytes", "satisfiesPzs", "satisfiesPzi", "registryUri",
		"vulnerabilityScanningConfig.enablementState", "vulnerabilityScanningConfig.enablementStateReason",
		"vulnerabilityScanningConfig.lastEnableTime",
	},
}

// decodeRepository strictly decodes a Repository request body.
func decodeRepository(raw []byte) (repositoryBody, error) {
	var body repositoryBody
	err := provider.DecodeJSON(raw, kindRepository, &body, gap, repositoryFields)
	return body, err
}

// checkMode accepts a standard repository; remote and virtual ones are gaps.
func checkMode(mode string) error {
	switch {
	case mode == "", mode == "MODE_UNSPECIFIED", mode == modeStandard:
		return nil
	case unmodeledModes[mode]:
		return gap("mode " + mode + " is not modeled")
	default:
		return status.Errorf(codes.InvalidArgument, "Invalid repository mode %q", mode)
	}
}

// checkFormat accepts a modeled format; the other v1 formats are gaps.
func checkFormat(format string) error {
	switch {
	case modeledFormats[format]:
		return nil
	case unmodeledFormats[format]:
		return gap("format " + format + " is not modeled")
	default:
		return status.Errorf(codes.InvalidArgument, "Unsupported repository format %q", format)
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

// repositoriesCreate validates the repository, then stores and indexes it as
// one operation. The real create takes the repositoryId as a query parameter.
func (p *Provider) repositoriesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeRepository(request.JSON)
	if err != nil {
		return nil, err
	}
	id := repositoryID(request.Query, body.Name)
	if !uid.MatchString(id) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid repository id %q", id)
	}
	format := cmp.Or(body.Format, "DOCKER")
	if err := checkFormat(format); err != nil {
		return nil, err
	}
	if err := checkMode(body.Mode); err != nil {
		return nil, err
	}
	now := p.clock()
	record := &repositoryRecord{
		Name: id, Format: format, Mode: modeStandard, Description: body.Description,
		CleanupPolicyDryRun: body.CleanupPolicyDryRun, Labels: body.Labels, CreatedAt: now, UpdatedAt: now,
	}
	return p.operate(scope, repositoryName(scope, id), "create", func() (map[string]any, error) {
		var response map[string]any
		err := p.scopes.With(scope, func(s *scopeState) error {
			if s.repositories[id] != nil {
				return status.Errorf(codes.AlreadyExists, "Repository %s already exists", id)
			}
			if err := p.index.Put(serviceName, state.Resource{
				Name: repositoryResource(scope, id), Service: serviceName,
				Kind: kindRepository, Parent: provider.ProjectResource(scope.Project), Scope: scope,
			}); err != nil {
				return fmt.Errorf("index the repository: %w", err)
			}
			s.repositories[id] = record
			response = repositoryWire(scope, record, viewCreate)
			return nil
		})
		if response != nil {
			response["@type"] = repositoryType
		}
		return response, err //nolint:wrapcheck // a wire status or the wrapped index error
	})
}

// Repository views: GCP returns different field sets per method (recorded).
const (
	viewCreate = iota // create operation response: no times or registry URI
	viewPatch         // patch response: times but no registry URI
	viewGet           // get/list: every field
)

// repositoryWire renders one repository document for the given view. The
// satisfiesPzi and scanning values are local stand-ins; the recording pins
// only that the fields are present.
func repositoryWire(scope state.Scope, record *repositoryRecord, view int) map[string]any {
	labels := maps.Clone(record.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	document := map[string]any{
		keyName:                       repositoryName(scope, record.Name),
		"format":                      record.Format,
		"mode":                        record.Mode,
		fieldDescription:              record.Description,
		fieldLabels:                   labels,
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
		document["registryUri"] = scope.Location + "-" + strings.ToLower(record.Format) + ".pkg.dev/" + scope.Project + "/" + record.Name
	}
	return document
}

// withRepository runs use on one repository under the scope lock, or reports
// it missing. use must not retain the record.
func (p *Provider) withRepository(scope state.Scope, repository string, use func(*scopeState, *repositoryRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback's own errors
		record := s.repositories[repository]
		if record == nil {
			return errNotFound
		}
		return use(s, record)
	})
}

func (p *Provider) repositoriesGet(scope state.Scope, repository string) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withRepository(scope, repository, func(_ *scopeState, record *repositoryRecord) error {
		document = repositoryWire(scope, record, viewGet)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// maxPageSize is the documented largest page; absent or 0 returns as many.
const maxPageSize = 1000

// paging holds the recorded list refusals; a negative size is checked first
// because its message names the value.
var paging = provider.Paging{Default: maxPageSize, Max: maxPageSize,
	InvalidToken: provider.Invalid("Request contains an invalid argument.")}

// listOrder reads orderBy: name ascending (the default, recorded) or
// descending. Ordering by other fields is a gap.
func listOrder(orderBy string) (bool, error) {
	switch strings.Join(strings.Fields(orderBy), " ") {
	case "", keyName, keyName + " asc":
		return false, nil
	case keyName + " desc":
		return true, nil
	default:
		return false, gap("list orderBy " + strconv.Quote(orderBy))
	}
}

// repositoriesList returns one page of the location's repositories in name
// order. filter is a gap.
func (p *Provider) repositoriesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if size, err := strconv.Atoi(request.Query.Get("pageSize")); err == nil && size < 0 {
		return nil, provider.Invalid("Invalid page size: " + strconv.Itoa(size))
	}
	descending, err := listOrder(request.Query.Get("orderBy"))
	if err != nil {
		return nil, err
	}
	var documents []map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, id := range slices.Sorted(maps.Keys(s.repositories)) {
			documents = append(documents, repositoryWire(scope, s.repositories[id], viewGet))
		}
		return nil
	})
	if descending {
		slices.Reverse(documents)
	}
	page, next, err := provider.Page(documents, request.Query, paging, gap, "orderBy")
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: provider.ListBody("repositories", page, next)}, nil
}

// Repository field names shared by the wire document and the update mask.
const (
	fieldDescription = "description"
	fieldLabels      = "labels"
)

// Update-mask paths of the Repository: modeled ones change, unmodeled ones
// are gaps, and GCP ignores any other path (recorded).
var (
	maskModeled   = map[string]bool{fieldDescription: true, fieldLabels: true, "cleanupPolicyDryRun": true}
	maskUnmodeled = map[string]bool{
		"cleanupPolicies": true, "kmsKeyName": true, "mavenConfig": true, "dockerConfig": true,
		"virtualRepositoryConfig": true, "remoteRepositoryConfig": true, "vulnerabilityScanningConfig": true,
		"disallowUnspecifiedMode": true,
	}
)

// maskPaths reads updateMask in either spelling: the modeled paths, or a gap
// for an unmodeled one. An empty mask returns nil.
func maskPaths(mask string) ([]string, error) {
	var paths []string
	for field := range strings.SplitSeq(mask, ",") {
		path := lowerCamel(strings.TrimSpace(field))
		top, _, _ := strings.Cut(path, ".")
		switch {
		case maskUnmodeled[top] || (maskModeled[top] && top != path):
			return nil, gap("update mask path " + strconv.Quote(path))
		case maskModeled[path]:
			paths = append(paths, path)
		}
	}
	return paths, nil
}

// lowerCamel spells a snake_case mask path in lowerCamelCase.
func lowerCamel(path string) string {
	words := strings.Split(path, "_")
	for i := 1; i < len(words); i++ {
		if words[i] != "" {
			words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
		}
	}
	return strings.Join(words, "")
}

// repositoriesUpdate applies a validated patch all-or-nothing. With a mask
// only its modeled paths change; without one GCP replaces every field, so a
// body whose format differs is refused (recorded).
func (p *Provider) repositoriesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeRepository(request.JSON)
	if err != nil {
		return nil, err
	}
	mask := request.Query.Get("updateMask")
	paths, err := maskPaths(mask)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	var document map[string]any
	err = p.withRepository(scope, request.Path[pathRepository], func(_ *scopeState, record *repositoryRecord) error {
		if mask == "" {
			if err := replaceable(record, body); err != nil {
				return err
			}
			paths = slices.Sorted(maps.Keys(maskModeled))
		}
		apply(record, body, paths)
		record.UpdatedAt = now
		document = repositoryWire(scope, record, viewPatch)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// replaceable refuses a maskless patch, which replaces every field, that
// would change the format (recorded) or the mode.
func replaceable(record *repositoryRecord, body repositoryBody) error {
	if body.Format != record.Format {
		return errFormatChange
	}
	if body.Mode != "" && body.Mode != record.Mode {
		return gap("changing a repository's mode")
	}
	return nil
}

// apply copies the masked modeled fields from body.
func apply(record *repositoryRecord, body repositoryBody, paths []string) {
	for _, path := range paths {
		switch path {
		case fieldDescription:
			record.Description = body.Description
		case fieldLabels:
			record.Labels = body.Labels
		default:
			record.CleanupPolicyDryRun = body.CleanupPolicyDryRun
		}
	}
}

// repositoriesDelete removes one repository and its index entry together, as
// one operation.
func (p *Provider) repositoriesDelete(scope state.Scope, repository string) (*gateway.RESTResponse, error) {
	return p.operate(scope, repositoryName(scope, repository), "delete", func() (map[string]any, error) {
		err := p.withRepository(scope, repository, func(s *scopeState, _ *repositoryRecord) error {
			if err := p.index.Remove(serviceName, scope, repositoryResource(scope, repository)); err != nil {
				return fmt.Errorf("unregister the repository: %w", err)
			}
			delete(s.repositories, repository)
			return nil
		})
		return map[string]any{"@type": emptyType}, err
	})
}

// getIam returns the stored policy.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// decodePolicy reads a SetIamPolicyRequest strictly. Repository policies
// take no conditions (recorded).
func decodePolicy(raw []byte) (iam.Policy, error) {
	policy, err := provider.DecodePolicy(raw, gap)
	if err != nil {
		return iam.Policy{}, err
	}
	for _, binding := range policy.Bindings {
		if condition := bytes.TrimSpace(binding.Condition); len(condition) > 0 && string(condition) != "null" {
			return iam.Policy{}, errConditional
		}
	}
	return policy, nil
}

// setIam stores the policy.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := decodePolicy(request.JSON)
	if err != nil {
		return nil, err
	}
	stored, err := p.engine.SetPolicy(target.Name, policy)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the Artifact Registry policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: stored}, nil
}
