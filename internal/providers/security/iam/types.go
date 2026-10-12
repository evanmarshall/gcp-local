// Package iam implements the IAM service-account control plane: service
// accounts with their per-resource IAM and key records, and signBlob. Keys are
// real local RSA keys that verify Storage signed URLs; token minting lives in
// the identity service.
package iam

import (
	"crypto/rsa"
	"errors"
	"regexp"
	"time"
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
	methodKeysGet        = "iam.serviceAccountKeys.get"
	methodSignBlob       = "iamcredentials.projects.serviceAccounts.signBlob"
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
	methodKeysGet:        "iam.serviceAccountKeys.get",
	methodSignBlob:       "iam.serviceAccounts.signBlob",
}

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,28}[a-z0-9]$`)

// Wire keys and resource kinds shared by the handlers.
const (
	keyName     = "name"
	kindAccount = "serviceAccount"
	kindKey     = "serviceAccountKey"
)

// keyRecord is one user-managed key of a service account. The private key
// leaves only in the create response; the public half lives in the IAM engine.
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
	Revision    int
}

// systemKey is the Google-managed key signBlob signs with, keyed by account
// email. Its public half is published to the IAM engine for verifiers.
type systemKey struct {
	ID      string
	Private *rsa.PrivateKey
}

// scopeState is one project slice of provider state.
type scopeState struct {
	accounts   map[string]*accountRecord
	systemKeys map[string]*systemKey
}

func newScopeState() scopeState {
	return scopeState{accounts: map[string]*accountRecord{}, systemKeys: map[string]*systemKey{}}
}

func (s *scopeState) reset() {
	s.accounts = map[string]*accountRecord{}
	s.systemKeys = map[string]*systemKey{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("iam requires registry, manager, index, and iam")

// accountResource names one service account in the IAM index.
func accountResource(project, account string) string {
	return "//iam.googleapis.com/projects/" + project + "/serviceAccounts/" + account
}

// accountName renders the canonical service-account name, keyed by email.
func accountName(project, account string) string {
	return "projects/" + project + "/serviceAccounts/" + accountEmail(project, account)
}

func accountEmail(project, account string) string {
	return account + "@" + project + ".iam.gserviceaccount.com"
}
