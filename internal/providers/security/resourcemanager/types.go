// Package resourcemanager implements Resource Manager: organizations,
// folders, and projects over v3, with project IAM on v1. Folders are created,
// moved, renamed, and soft-deleted here; organizations and projects exist
// only as index entries seeded at startup, and projects can move.
package resourcemanager

import (
	"errors"
	"regexp"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method name constants; the generated route table uses the same identifiers.
// The v1 rows keep the "resourcemanager." prefix, the v3 rows use the
// cloudresourcemanager v3 discovery method IDs.
const (
	methodProjectsGetIam     = "resourcemanager.projects.getIamPolicy"
	methodProjectsSetIam     = "resourcemanager.projects.setIamPolicy"
	methodProjectsTestIam    = "resourcemanager.projects.testIamPermissions"
	methodProjectsGetV1      = "resourcemanager.projects.get"
	methodOperationsGetV1    = "resourcemanager.operations.get"
	methodOrganizationsGet   = "cloudresourcemanager.organizations.get"
	methodOrganizationsFind  = "cloudresourcemanager.organizations.search"
	methodOrganizationsGetP  = "cloudresourcemanager.organizations.getIamPolicy"
	methodOrganizationsSetP  = "cloudresourcemanager.organizations.setIamPolicy"
	methodOrganizationsTestP = "cloudresourcemanager.organizations.testIamPermissions"
	methodFoldersCreate      = "cloudresourcemanager.folders.create"
	methodFoldersList        = "cloudresourcemanager.folders.list"
	methodFoldersFind        = "cloudresourcemanager.folders.search"
	methodFoldersGet         = "cloudresourcemanager.folders.get"
	methodFoldersPatch       = "cloudresourcemanager.folders.patch"
	methodFoldersDelete      = "cloudresourcemanager.folders.delete"
	methodFoldersMove        = "cloudresourcemanager.folders.move"
	methodFoldersUndelete    = "cloudresourcemanager.folders.undelete"
	methodFoldersGetP        = "cloudresourcemanager.folders.getIamPolicy"
	methodFoldersSetP        = "cloudresourcemanager.folders.setIamPolicy"
	methodFoldersTestP       = "cloudresourcemanager.folders.testIamPermissions"
	methodProjectsGet        = "cloudresourcemanager.projects.get"
	methodProjectsList       = "cloudresourcemanager.projects.list"
	methodProjectsFind       = "cloudresourcemanager.projects.search"
	methodProjectsMove       = "cloudresourcemanager.projects.move"
	methodProjectsGetP       = "cloudresourcemanager.projects.getIamPolicy"
	methodProjectsSetP       = "cloudresourcemanager.projects.setIamPolicy"
	methodProjectsTestP      = "cloudresourcemanager.projects.testIamPermissions"
	methodOperationsGet      = "cloudresourcemanager.operations.get"
)

// permissions is each authorized method's permission, from the v3 reference
// ("The caller must have ..."). A search checks it on every candidate and
// returns those the caller may get. testIamPermissions requires none, and an
// operation is readable by the principal that started it.
var permissions = map[string]string{
	methodProjectsGetIam:    "resourcemanager.projects.getIamPolicy",
	methodProjectsSetIam:    "resourcemanager.projects.setIamPolicy",
	methodProjectsGetV1:     permissionProjectsGet,
	methodOrganizationsGet:  "resourcemanager.organizations.get",
	methodOrganizationsFind: "resourcemanager.organizations.get",
	methodOrganizationsGetP: "resourcemanager.organizations.getIamPolicy",
	methodOrganizationsSetP: "resourcemanager.organizations.setIamPolicy",
	methodFoldersCreate:     "resourcemanager.folders.create",
	methodFoldersList:       "resourcemanager.folders.list",
	methodFoldersFind:       "resourcemanager.folders.get",
	methodFoldersGet:        "resourcemanager.folders.get",
	methodFoldersPatch:      "resourcemanager.folders.update",
	methodFoldersDelete:     "resourcemanager.folders.delete",
	methodFoldersMove:       "resourcemanager.folders.move",
	methodFoldersUndelete:   "resourcemanager.folders.undelete",
	methodFoldersGetP:       "resourcemanager.folders.getIamPolicy",
	methodFoldersSetP:       "resourcemanager.folders.setIamPolicy",
	methodProjectsGet:       permissionProjectsGet,
	methodProjectsList:      "resourcemanager.projects.list",
	methodProjectsFind:      permissionProjectsGet,
	methodProjectsMove:      "resourcemanager.projects.move",
	methodProjectsGetP:      "resourcemanager.projects.getIamPolicy",
	methodProjectsSetP:      "resourcemanager.projects.setIamPolicy",
}

// unplanned are the served methods without a permission plan.
var unplanned = []string{methodProjectsTestIam, methodOrganizationsTestP, methodFoldersTestP, methodProjectsTestP,
	methodOperationsGetV1, methodOperationsGet}

// Index kinds, collections, and name prefixes of the hierarchy nodes.
const (
	collectionFolders       = "folders"
	collectionOrganizations = "organizations"
	service                 = "resourcemanager"
	kindOrganization        = "organization"
	kindFolder              = "folder"
	kindProject             = "project"
	namePrefix              = "//cloudresourcemanager.googleapis.com/"
)

// Wire field names rendered in several places.
const (
	fieldName        = "name"
	fieldDisplayName = "displayName"
	fieldCreateTime  = "createTime"
	fieldUpdateTime  = "updateTime"
	fieldState       = "state"
)

// permissionProjectsGet reads a project, through get or a search.
const permissionProjectsGet = "resourcemanager.projects.get"

// Folder lifecycle states (v3 Folder.State).
const (
	stateActive          = "ACTIVE"
	stateDeleteRequested = "DELETE_REQUESTED"
)

// maxFolderDepth is GCP's folder nesting limit: "Folders can be nested up to
// 10 levels" (https://cloud.google.com/resource-manager/docs/creating-managing-folders).
const maxFolderDepth = 10

var (
	projectIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,28}[a-z0-9]$`)
	// displayNamePattern is the v3 Folder.display_name rule: letters and
	// digits at both ends, letters, digits, spaces, hyphens, and underscores
	// between, at most 30 characters.
	displayNamePattern = regexp.MustCompile(`^[\p{L}\p{N}]([\p{L}\p{N}_\- ]{0,28}[\p{L}\p{N}])?$`)
	// nodeIDPattern is one organization, folder, or project path segment.
	nodeIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
)

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("resourcemanager requires registry, manager, index, and iam")

// gap refuses an unmodeled request shape with UNIMPLEMENTED.
var gap = provider.CoverageGap("Resource Manager")

// Recorded refusals (resourcemanager-hierarchy-visibility).
var (
	// errInvalidArgument is GCP's bare 400: a bad page token, an unknown
	// operation, a move of an unknown folder.
	errInvalidArgument = status.Error(codes.InvalidArgument, "Request contains an invalid argument.")
	// errNoPermission is the 403 without details that list, move, and a
	// create without a parent return.
	errNoPermission = status.Error(codes.PermissionDenied, "The caller does not have permission")
	// errInvalidParent refuses a project list without an organization or
	// folder parent.
	errInvalidParent = provider.BadRequest("field [parent] has issue [invalid parent name]", "parent", "invalid parent name")
	// errNegativePageSize refuses a negative page size.
	errNegativePageSize = provider.BadRequest("field [pageSize] has issue [invalid page size]", "pageSize", "invalid page size")
)

// paging is every list and search method's page bounds.
var paging = provider.Paging{Default: 100, Max: 1000, InvalidToken: errInvalidArgument, NegativeSize: errNegativePageSize}

// notFound uses Resource Manager's quoted-project text, not the shared kind/name form.
func notFound(project string) error {
	return status.Errorf(codes.NotFound, "The project %q was not found", project)
}

// failed is a business-rule refusal (FAILED_PRECONDITION).
func failed(message string) error { return status.Error(codes.FailedPrecondition, message) }
