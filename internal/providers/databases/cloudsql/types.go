// Package cloudsql serves a Cloud SQL Admin v1beta4 slice for PostgreSQL and
// MySQL. The provider authenticates and authorizes each call with the shared
// IAM engine before any backend effect. Each Cloud SQL instance is one real
// PostgreSQL or MySQL server, started after authorization inside the
// project's backend lifecycle; databases and built-in users are real objects
// in that server. A local connector forwards wire traffic only after checking
// cloudsql.instances.connect for the identity bound to it. The raw server is
// reachable only through a private Unix socket, never a TCP port.
package cloudsql

import (
	"crypto/rand"
	"errors"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// serviceName owns the backend registration and index entries.
const serviceName = "cloudsql"

// Method names; the generated route table uses the same identifiers.
const (
	methodInstancesInsert = "sql.instances.insert"
	methodInstancesGet    = "sql.instances.get"
	methodInstancesList   = "sql.instances.list"
	methodInstancesPatch  = "sql.instances.patch"
	methodInstancesDelete = "sql.instances.delete"
	methodDatabasesInsert = "sql.databases.insert"
	methodDatabasesGet    = "sql.databases.get"
	methodDatabasesList   = "sql.databases.list"
	methodDatabasesDelete = "sql.databases.delete"
	methodUsersInsert     = "sql.users.insert"
	methodUsersList       = "sql.users.list"
	methodUsersDelete     = "sql.users.delete"
	methodOperationsGet   = "sql.operations.get"
	methodOperationsList  = "sql.operations.list"
	// methodConnect is the connector's plan. It is not an API route: the real
	// Cloud SQL Auth Proxy obtains the same permission check through
	// connectSettings/generateEphemeralCert, which are not served here.
	methodConnect = "cloudsql.connector.connect"
)

// permInstancesGet also authorizes operation reads.
const permInstancesGet = "cloudsql.instances.get"

// permissions follows the Cloud SQL Admin API access-control table.
var permissions = map[string]string{
	methodInstancesInsert: "cloudsql.instances.create",
	methodInstancesGet:    permInstancesGet,
	methodInstancesList:   "cloudsql.instances.list",
	methodInstancesPatch:  "cloudsql.instances.update",
	methodInstancesDelete: "cloudsql.instances.delete",
	methodDatabasesInsert: "cloudsql.databases.create",
	methodDatabasesGet:    "cloudsql.databases.get",
	methodDatabasesList:   "cloudsql.databases.list",
	methodDatabasesDelete: "cloudsql.databases.delete",
	methodUsersInsert:     "cloudsql.users.create",
	methodUsersList:       "cloudsql.users.list",
	methodUsersDelete:     "cloudsql.users.delete",
	methodOperationsGet:   permInstancesGet,
	methodOperationsList:  permInstancesGet,
	methodConnect:         "cloudsql.instances.connect",
}

// Path variables, resource kinds, and wire constants.
const (
	pathProject    = "project"
	pathInstance   = "instance"
	pathDatabase   = "database"
	pathOperation  = "operation"
	kindProject    = "project"
	kindInstance   = "instance"
	ownerProjects  = "resourcemanager"
	apiBase        = "https://sqladmin.googleapis.com/sql/v1beta4/"
	cloudPlatform  = "https://www.googleapis.com/auth/cloud-platform"
	defaultRegion  = "us-central1"
	adminRole      = "cloudsqladmin"
	superuserRole  = "cloudsqlsuperuser"
	defaultUser    = "postgres"
	loopbackIPv4   = "127.0.0.1"
	statusDone     = "DONE"
	statusRunnable = "RUNNABLE"
)

// JSON keys repeated across the wire renderers.
const (
	keyKind               = "kind"
	keyName               = "name"
	keySelfLink           = "selfLink"
	keySettings           = "settings"
	keySettingsVersion    = "settingsVersion"
	keyDeletionProtection = "deletionProtectionEnabled"
)

// Errors returned by provider construction.
var (
	errProviderShape = errors.New("cloud SQL requires registry, manager, index, and iam")
	// ErrRolesUnrecorded reports that no pinned predefined role grants the
	// Cloud SQL permissions yet. The roles/cloudsql.* snapshots must be
	// recorded read-only (scripts/live/record_iam_roles.py --service cloudsql)
	// before the provider can register; snapshots are never hand-written.
	ErrRolesUnrecorded = errors.New("cloud SQL role snapshots are not recorded")
)

// instanceResource names an instance in the IAM index.
func instanceResource(project, instance string) string {
	return "//cloudsql.googleapis.com/projects/" + project + "/instances/" + instance
}

// instanceLink renders an instance selfLink.
func instanceLink(project, instance string) string {
	return apiBase + "projects/" + project + "/instances/" + instance
}

// apiError builds the Cloud SQL JSON error envelope. reason is included only
// where the service's reason code is known; it is omitted otherwise. Recorded:
// v1beta4 errors carry no status field, so statusText documents intent only.
func apiError(code int, statusText, reason, message string) *gateway.RESTResponse {
	_ = statusText
	body := map[string]any{"code": code, "message": message}
	if reason != "" {
		body["errors"] = []any{map[string]any{"domain": "global", "reason": reason, "message": message}}
	}
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{"error": body}}
}

// notFound renders Cloud SQL's instanceDoesNotExist body, which has its own reason field.
func notFound() *gateway.RESTResponse {
	return apiError(http.StatusNotFound, "NOT_FOUND", "instanceDoesNotExist", "The Cloud SQL instance does not exist.")
}

// gap reports an unsupported request shape explicitly.
var gap = provider.CoverageGap("Cloud SQL")

func randomBytes(n int) []byte {
	value := make([]byte, n)
	_, _ = rand.Read(value) // crypto/rand.Read never returns an error
	return value
}
