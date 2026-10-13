// Package artifactregistry implements the Artifact Registry v1 control plane:
// repositories, their operations, and per-repository IAM. Repositories are
// keyed by project and location. Image upload/download (the pkg.dev data
// plane) is out of scope. Recording artifactregistry-repository-lifecycle
// pins the wire behavior.
package artifactregistry

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	methodOpsGet      = "artifactregistry.projects.locations.operations.get"
)

// permissions follows the documented Artifact Registry access-control rows.
// No role grants an operations permission; operations.get is modeled with
// repositories.get on the project.
var permissions = map[string]string{
	methodReposCreate: "artifactregistry.repositories.create",
	methodReposList:   "artifactregistry.repositories.list",
	methodReposGet:    "artifactregistry.repositories.get",
	methodReposUpdate: "artifactregistry.repositories.update",
	methodReposDelete: "artifactregistry.repositories.delete",
	methodReposGetIam: "artifactregistry.repositories.getIamPolicy",
	methodReposSetIam: "artifactregistry.repositories.setIamPolicy",
	methodOpsGet:      "artifactregistry.repositories.get",
}

// Wire keys and resource kinds shared by the handlers.
const (
	keyName        = "name"
	kindRepository = "repository"
	pathRepository = "repository"
	pathLocation   = "location"
	pathProject    = "project"
)

// gap reports a request shape the local model refuses explicitly.
var gap = provider.CoverageGap("Artifact Registry")

// Recorded refusals (artifactregistry-repository-lifecycle).
var (
	// errNotFound is GCP's 404 for a missing repository.
	errNotFound = status.Error(codes.NotFound, "Requested entity was not found.")
	// errConditional refuses a repository policy with a condition.
	errConditional = provider.Invalid("Can't set conditional policy on this resource")
	// errFormatChange refuses a patch without a mask whose format differs.
	errFormatChange = provider.Invalid("Repository format cannot be changed")
)

// denial is the recorded 403 wording; the gate adds ErrorInfo with this domain.
const denial = "Permission '%s' denied on resource '//artifactregistry.googleapis.com/%s' (or it may not exist)."

var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,50}[a-z0-9]$`)

// Repository formats from the v1 Format enum. The modeled ones are stored
// and returned; the others are explicit gaps.
var (
	modeledFormats   = map[string]bool{"DOCKER": true, "NPM": true, "MAVEN": true, "PYTHON": true, "GO": true}
	unmodeledFormats = map[string]bool{"APT": true, "YUM": true, "GOOGET": true, "KFP": true, "GENERIC": true}
)

// Repository modes from the v1 Mode enum; only standard repositories are modeled.
const modeStandard = "STANDARD_REPOSITORY"

var unmodeledModes = map[string]bool{"REMOTE_REPOSITORY": true, "VIRTUAL_REPOSITORY": true}

type repositoryRecord struct {
	Name                string
	Format              string
	Mode                string
	Description         string
	CleanupPolicyDryRun bool
	Labels              map[string]string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// scopeState is one project and location's repositories, keyed by ID.
type scopeState struct {
	repositories map[string]*repositoryRecord
}

func newScopeState() scopeState {
	return scopeState{repositories: map[string]*repositoryRecord{}}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("artifactregistry requires registry, manager, index, and iam")

// Location names: regions ("us-east1") and multi-regions.
var (
	regionName   = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)
	multiRegions = map[string]bool{"us": true, "europe": true, "asia": true}
)

// validLocation accepts Artifact Registry locations; there is no global one.
func validLocation(scope state.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("artifactregistry scope: %w", err)
	}
	if !regionName.MatchString(scope.Location) && !multiRegions[scope.Location] {
		return errLocation
	}
	return nil
}

var errLocation = errors.New("artifact registry locations are regions or multi-regions")

// repositoryName is the relative name of one repository.
func repositoryName(scope state.Scope, repository string) string {
	return provider.LocationName(scope) + "/repositories/" + repository
}

// repositoryResource names one repository in the IAM index.
func repositoryResource(scope state.Scope, repository string) string {
	return "//artifactregistry.googleapis.com/" + repositoryName(scope, repository)
}
