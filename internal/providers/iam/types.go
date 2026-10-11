// Package iam implements the IAM service-account control plane: service
// accounts with their per-resource IAM and key records. Key material is
// synthesized locally; token minting lives in the identity service.
package iam

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the backend registration.
const serviceName = "iam"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodAccountsCreate = "iam.serviceAccounts.create"
	methodAccountsList   = "iam.serviceAccounts.list"
	methodAccountsGet    = "iam.serviceAccounts.get"
	methodAccountsUpdate = "iam.serviceAccounts.update"
	methodAccountsDelete = "iam.serviceAccounts.delete"
	methodAccountsGetIam = "iam.serviceAccounts.getIamPolicy"
	methodAccountsSetIam = "iam.serviceAccounts.setIamPolicy"
	methodKeysCreate     = "iam.serviceAccountKeys.create"
	methodKeysList       = "iam.serviceAccountKeys.list"
	methodKeysDelete     = "iam.serviceAccountKeys.delete"
)

// permissions follows the documented access-control rows.
var permissions = map[string]string{
	methodAccountsCreate: "iam.serviceAccounts.create",
	methodAccountsList:   "iam.serviceAccounts.list",
	methodAccountsGet:    "iam.serviceAccounts.get",
	methodAccountsUpdate: "iam.serviceAccounts.update",
	methodAccountsDelete: "iam.serviceAccounts.delete",
	methodAccountsGetIam: "iam.serviceAccounts.getIamPolicy",
	methodAccountsSetIam: "iam.serviceAccounts.setIamPolicy",
	methodKeysCreate:     "iam.serviceAccountKeys.create",
	methodKeysList:       "iam.serviceAccountKeys.list",
	methodKeysDelete:     "iam.serviceAccountKeys.delete",
}

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,28}[a-z0-9]$`)

// Wire keys and resource kinds shared by the handlers.
const (
	keyName     = "name"
	kindAccount = "serviceAccount"
	kindKey     = "serviceAccountKey"
)

// keyRecord is one synthesized key of a service account.
type keyRecord struct {
	ID        string
	CreatedAt time.Time
}

// accountRecord is one stored service account.
type accountRecord struct {
	Project     string
	ID          string
	DisplayName string
	Description string
	Keys        map[string]*keyRecord
	CreatedAt   time.Time
}

// scopeState is one project slice of provider state.
type scopeState struct {
	accounts map[string]*accountRecord
}

func newScopeState() scopeState {
	return scopeState{accounts: map[string]*accountRecord{}}
}

func (s *scopeState) reset() {
	s.accounts = map[string]*accountRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("iam requires registry, manager, index, and iam")

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

// accountResource names one service account in the IAM index.
func accountResource(project, account string) string {
	return "//iam.googleapis.com/projects/" + project + "/serviceAccounts/" + account
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// accountName renders the canonical service-account name.
func accountName(project, account string) string {
	return "projects/" + project + "/serviceAccounts/" + account
}

// accountEmail renders the account email.
func accountEmail(project, account string) string {
	return account + "@" + project + ".iam.gserviceaccount.com"
}

// stamp renders one RFC3339 timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
