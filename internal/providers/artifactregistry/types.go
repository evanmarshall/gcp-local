// Package artifactregistry implements the Artifact Registry v1 control plane:
// repositories and per-repository IAM. Image upload/download (GCR data
// plane) is out of scope. No cloud recording covers these methods yet, so
// nothing here claims recorded conformance.
package artifactregistry

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the Artifact Registry backend registration.
const serviceName = "artifactregistry"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodReposCreate = "artifactregistry.repositories.create"
	methodReposList   = "artifactregistry.repositories.list"
	methodReposGet    = "artifactregistry.repositories.get"
	methodReposUpdate = "artifactregistry.repositories.update"
	methodReposDelete = "artifactregistry.repositories.delete"
	methodReposGetIam = "artifactregistry.repositories.getIamPolicy"
	methodReposSetIam = "artifactregistry.repositories.setIamPolicy"
)

// permissions follows the documented Artifact Registry access-control rows.
var permissions = map[string]string{
	methodReposCreate: "artifactregistry.repositories.create",
	methodReposList:   "artifactregistry.repositories.list",
	methodReposGet:    "artifactregistry.repositories.get",
	methodReposUpdate: "artifactregistry.repositories.update",
	methodReposDelete: "artifactregistry.repositories.delete",
	methodReposGetIam: "artifactregistry.repositories.getIamPolicy",
	methodReposSetIam: "artifactregistry.repositories.setIamPolicy",
}

// Wire keys and resource kinds shared by the handlers.
const (
	keyName        = "name"
	kindRepository = "repository"
)

var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,50}[a-z0-9]$`)

var supportedFormats = map[string]bool{
	"DOCKER": true, "NPM": true, "MAVEN": true, "PYTHON": true, "GO": true,
}

// repositoryRecord is one stored repository.
type repositoryRecord struct {
	Name                string
	Location            string
	Format              string
	Mode                string
	Description         string
	CleanupPolicyDryRun bool
	Labels              map[string]string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// scopeState is one project slice of provider state.
type scopeState struct {
	repositories map[string]*repositoryRecord
}

func newScopeState() scopeState {
	return scopeState{repositories: map[string]*repositoryRecord{}}
}

func (s *scopeState) reset() {
	s.repositories = map[string]*repositoryRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("artifactregistry requires registry, manager, index, and iam")

// wireError builds the error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// scopeOf names one project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// repositoryResource names one repository in the IAM index.
func repositoryResource(project, repository string) string {
	return "//artifactregistry.googleapis.com/projects/" + project + "/locations/us-central1/repositories/" + repository
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// repositoryName renders the canonical repository name.
func repositoryName(project, location, repository string) string {
	return "projects/" + project + "/locations/" + location + "/repositories/" + repository
}

// stamp renders one RFC3339 timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
