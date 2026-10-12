package iam

import (
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// oauthScope is what one published Google OAuth scope lets a token call. GCP
// checks a method's accepted scopes before IAM; gcp-local approximates each
// method by the permissions its plan resolves to.
type oauthScope struct {
	// prefixes are the permission prefixes the scope covers; nil covers none.
	prefixes []string
	// excluded are permission prefixes or verb suffixes ("*.update") the
	// scope does not cover even inside prefixes.
	excluded []string
	// readOnly limits coverage to get and list permissions.
	readOnly bool
}

// Bigtable's data-plane permissions: the bigtable.data scopes cover them and
// the bigtable.admin scopes do not (bigtableadmin v2 and bigtable v2
// discovery documents).
var bigtableData = []string{"bigtable.tables.readRows", "bigtable.tables.sampleRowKeys", "bigtable.tables.mutateRows", "bigtable.tables.checkAndMutateRow", "bigtable.tables.readModifyWriteRow"}

// bigtableReadData are the data-plane reads bigtable.data.readonly covers.
var bigtableReadData = []string{"bigtable.tables.readRows", "bigtable.tables.sampleRowKeys"}

// bigtableAdmin is the admin-scope coverage: every Bigtable permission except
// the data plane.
var bigtableAdmin = oauthScope{prefixes: []string{"bigtable."}, excluded: bigtableData}

// bigtableTableAdmin covers the table admin API only.
var bigtableTableAdmin = oauthScope{prefixes: []string{"bigtable.tables.", "bigtable.backups."}, excluded: bigtableData}

// bigtableInstanceAdmin covers the instance admin API only.
var bigtableInstanceAdmin = oauthScope{prefixes: []string{"bigtable.instances.", "bigtable.clusters.", "bigtable.appProfiles."}}

// storagePermissions is the prefix of every Storage permission.
var storagePermissions = []string{"storage."}

// spannerData are the permissions of the Spanner data API, which accepts the
// spanner.data scope; spanner.admin covers the admin APIs.
var spannerData = []string{"spanner.sessions.", "spanner.databases.select", "spanner.databases.read", "spanner.databases.write", "spanner.databases.beginOrRollbackReadWriteTransaction", "spanner.databases.beginReadOnlyTransaction", "spanner.databases.beginPartitionedDmlTransaction", "spanner.databases.partitionQuery", "spanner.databases.partitionRead", "spanner.databases.useDataBoost"}

// oauthScopes is the allowlist of OAuth scopes the local token service issues,
// from Google's OAuth 2.0 scope reference
// (https://developers.google.com/identity/protocols/oauth2/scopes). Each is a
// subset of cloud-platform or an identity scope that grants no API call. A
// scope's coverage follows the scopes its APIs' discovery documents accept;
// docs/local-identity.md lists the mapping and its approximations.
var oauthScopes = map[string]oauthScope{
	// Identity scopes: gcloud and user flows request them; no API accepts them.
	"openid": {},
	"email":  {},
	"https://www.googleapis.com/auth/userinfo.email":   {},
	"https://www.googleapis.com/auth/userinfo.profile": {},
	// Cloud SQL IAM database login, not the Admin API (sqladmin v1beta4).
	"https://www.googleapis.com/auth/sqlservice.login": {},

	"https://www.googleapis.com/auth/cloud-platform":           {prefixes: []string{""}},
	"https://www.googleapis.com/auth/cloud-platform.read-only": {prefixes: []string{""}, readOnly: true},

	// storage v1 discovery: read_write omits patch/update, IAM policy, and
	// HMAC-key writes, which accept only full_control.
	"https://www.googleapis.com/auth/devstorage.full_control": {prefixes: storagePermissions},
	"https://www.googleapis.com/auth/devstorage.read_write":   {prefixes: storagePermissions, excluded: []string{"*.update", "*.getIamPolicy", "*.setIamPolicy", "storage.hmacKeys."}},
	"https://www.googleapis.com/auth/devstorage.read_only":    {prefixes: storagePermissions, readOnly: true},

	"https://www.googleapis.com/auth/pubsub":    {prefixes: []string{"pubsub."}},
	"https://www.googleapis.com/auth/datastore": {prefixes: []string{"datastore."}},
	"https://www.googleapis.com/auth/cloudkms":  {prefixes: []string{"cloudkms."}},
	"https://www.googleapis.com/auth/iam":       {prefixes: []string{"iam."}},

	"https://www.googleapis.com/auth/bigquery":            {prefixes: []string{"bigquery."}},
	"https://www.googleapis.com/auth/bigquery.readonly":   {prefixes: []string{"bigquery."}, readOnly: true},
	"https://www.googleapis.com/auth/bigquery.insertdata": {prefixes: []string{"bigquery.tables.updateData"}},

	"https://www.googleapis.com/auth/spanner.admin": {prefixes: []string{"spanner."}, excluded: spannerData},
	"https://www.googleapis.com/auth/spanner.data":  {prefixes: spannerData},

	"https://www.googleapis.com/auth/bigtable.admin":               bigtableAdmin,
	"https://www.googleapis.com/auth/bigtable.admin.table":         bigtableTableAdmin,
	"https://www.googleapis.com/auth/bigtable.admin.cluster":       bigtableInstanceAdmin,
	"https://www.googleapis.com/auth/bigtable.admin.instance":      bigtableInstanceAdmin,
	"https://www.googleapis.com/auth/cloud-bigtable.admin":         bigtableAdmin,
	"https://www.googleapis.com/auth/cloud-bigtable.admin.table":   bigtableTableAdmin,
	"https://www.googleapis.com/auth/cloud-bigtable.admin.cluster": bigtableInstanceAdmin,
	"https://www.googleapis.com/auth/bigtable.data":                {prefixes: bigtableData},
	"https://www.googleapis.com/auth/bigtable.data.readonly":       {prefixes: bigtableReadData},
	"https://www.googleapis.com/auth/cloud-bigtable.data":          {prefixes: bigtableData},
	"https://www.googleapis.com/auth/cloud-bigtable.data.readonly": {prefixes: bigtableReadData},

	"https://www.googleapis.com/auth/logging.admin": {prefixes: []string{"logging."}},
	"https://www.googleapis.com/auth/logging.read":  {prefixes: []string{"logging."}, readOnly: true},
	"https://www.googleapis.com/auth/logging.write": {prefixes: []string{"logging.logEntries.create"}},

	"https://www.googleapis.com/auth/compute":          {prefixes: []string{"compute."}},
	"https://www.googleapis.com/auth/compute.readonly": {prefixes: []string{"compute."}, readOnly: true},
	"https://www.googleapis.com/auth/sqlservice.admin": {prefixes: []string{"cloudsql."}},
	"https://www.googleapis.com/auth/appengine.admin":  {prefixes: []string{"appengine."}},
}

// KnownScope reports whether the local token service may issue scope.
func KnownScope(scope string) bool {
	_, known := oauthScopes[scope]
	return known
}

// covers reports whether the scope accepts a call that needs permission.
func (s oauthScope) covers(permission string) bool {
	matches := func(pattern string) bool {
		if suffix, ok := strings.CutPrefix(pattern, "*"); ok {
			return strings.HasSuffix(permission, suffix)
		}
		return strings.HasPrefix(permission, pattern)
	}
	if !slices.ContainsFunc(s.prefixes, matches) || slices.ContainsFunc(s.excluded, matches) {
		return false
	}
	if s.readOnly {
		verb := permission[strings.LastIndex(permission, ".")+1:]
		return verb == "get" || verb == "list"
	}
	return true
}

// scopesCover reports whether one of the token's scopes covers every planned
// permission, as one accepted scope admits a GCP method. Scopes outside the
// allowlist cover nothing.
func scopesCover(scopes, permissions []string) bool {
	for _, name := range scopes {
		scope, known := oauthScopes[name]
		if known && len(scope.prefixes) > 0 && !slices.ContainsFunc(permissions, func(p string) bool { return !scope.covers(p) }) {
			return true
		}
	}
	return false
}

// scopeInsufficientMessage is GCP's message for a token without an accepted scope.
const scopeInsufficientMessage = "Request had insufficient authentication scopes."

// scopeInsufficient is GCP's refusal of a token whose scopes do not include
// one the method accepts: 403 with reason ACCESS_TOKEN_SCOPE_INSUFFICIENT.
func scopeInsufficient() error {
	s := status.New(codes.PermissionDenied, scopeInsufficientMessage)
	if detailed, err := s.WithDetails(&errdetails.ErrorInfo{Reason: "ACCESS_TOKEN_SCOPE_INSUFFICIENT", Domain: "googleapis.com"}); err == nil {
		s = detailed
	}
	return s.Err()
}
