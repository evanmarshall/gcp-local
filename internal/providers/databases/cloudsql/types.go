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
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the backend registration and index entries.
const serviceName = "cloudsql"

// Method names; the generated route table uses the same identifiers.
const (
	methodInstancesInsert  = "sql.instances.insert"
	methodInstancesGet     = "sql.instances.get"
	methodInstancesList    = "sql.instances.list"
	methodInstancesPatch   = "sql.instances.patch"
	methodInstancesDelete  = "sql.instances.delete"
	methodDatabasesInsert  = "sql.databases.insert"
	methodDatabasesGet     = "sql.databases.get"
	methodDatabasesList    = "sql.databases.list"
	methodDatabasesDelete  = "sql.databases.delete"
	methodUsersInsert      = "sql.users.insert"
	methodUsersList        = "sql.users.list"
	methodUsersDelete      = "sql.users.delete"
	methodOperationsGet    = "sql.operations.get"
	methodOperationsList   = "sql.operations.list"
	methodOperationsCancel = "sql.operations.cancel"
	// methodConnect is the connector's plan. It is not an API route: the real
	// Cloud SQL Auth Proxy obtains the same permission check through
	// connectSettings/generateEphemeralCert, which are not served here.
	methodConnect = "cloudsql.connector.connect"
)

// permInstancesGet also authorizes operation reads.
const permInstancesGet = "cloudsql.instances.get"

// permissions follows the Cloud SQL Admin API access-control table.
var permissions = map[string]string{
	methodInstancesInsert:  "cloudsql.instances.create",
	methodInstancesGet:     permInstancesGet,
	methodInstancesList:    "cloudsql.instances.list",
	methodInstancesPatch:   "cloudsql.instances.update",
	methodInstancesDelete:  "cloudsql.instances.delete",
	methodDatabasesInsert:  "cloudsql.databases.create",
	methodDatabasesGet:     "cloudsql.databases.get",
	methodDatabasesList:    "cloudsql.databases.list",
	methodDatabasesDelete:  "cloudsql.databases.delete",
	methodUsersInsert:      "cloudsql.users.create",
	methodUsersList:        "cloudsql.users.list",
	methodUsersDelete:      "cloudsql.users.delete",
	methodOperationsGet:    permInstancesGet,
	methodOperationsList:   permInstancesGet,
	methodOperationsCancel: "cloudsql.instances.update",
	methodConnect:          "cloudsql.instances.connect",
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
	statusPending  = "PENDING"
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
	keyETag               = "etag"
	keyMessage            = "message"
	domainGlobal          = "global"
	reasonInvalid         = "invalid"
	statusInvalid         = "INVALID_ARGUMENT"
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
	return failure{code: code, reason: reason, domain: domainGlobal, message: message}.render()
}

// notFound renders Cloud SQL's instanceDoesNotExist body, which has its own reason field.
func notFound() *gateway.RESTResponse {
	return apiError(http.StatusNotFound, "NOT_FOUND", "instanceDoesNotExist", "The Cloud SQL instance does not exist.")
}

// gap reports an unsupported request shape explicitly.
var gap = provider.CoverageGap("Cloud SQL")

// operationNotFound is the missing-operation error.
func operationNotFound() *gateway.RESTResponse {
	return apiError(http.StatusNotFound, "NOT_FOUND", "", "The Cloud SQL operation does not exist.")
}

// failure is a handler refusal in the v1beta4 error shape; wireFailure
// renders it. Most recorded refusals have an errors[] entry in domain
// "global" and no status; a few also carry a status, and the transcoder's
// also a BadRequest on field, with no domain.
type failure struct {
	code                    int
	reason, message, domain string
	status, field           string
}

func (f failure) Error() string { return f.message }

// Recorded refusals (cloudsql-postgres-instance-admin).
var (
	errInvalidPageToken = failure{code: http.StatusBadRequest, reason: reasonInvalid, domain: domainGlobal, message: "Invalid request: Invalid page token."}
	errMissingTier      = failure{code: http.StatusBadRequest, reason: "required", domain: domainGlobal, message: "Missing parameter: Tier."}
)

// render builds the error body.
func (f failure) render() *gateway.RESTResponse {
	body := map[string]any{"code": f.code, keyMessage: f.message}
	if f.reason != "" {
		entry := map[string]any{"reason": f.reason, keyMessage: f.message}
		if f.domain != "" {
			entry["domain"] = f.domain
		}
		body["errors"] = []any{entry}
	}
	if f.status != "" {
		body["status"] = f.status
	}
	if f.field != "" {
		body["details"] = []any{map[string]any{"@type": "type.googleapis.com/google.rpc.BadRequest",
			"fieldViolations": []any{map[string]any{"field": f.field, "description": f.message}}}}
	}
	return &gateway.RESTResponse{Status: f.code, JSON: map[string]any{"error": body}}
}

// pageQuery maps v1beta4's maxResults (uint32) onto the shared pager's
// pageSize; a value that isn't one is the transcoder's refusal (recorded).
func pageQuery(query url.Values) (url.Values, error) {
	paged := url.Values{"pageToken": query["pageToken"], "filter": query["filter"]}
	if text := query.Get("maxResults"); text != "" {
		if _, err := strconv.ParseUint(text, 10, 32); err != nil {
			return nil, failure{code: http.StatusBadRequest, reason: reasonInvalid, status: statusInvalid, field: "max_results",
				message: fmt.Sprintf("Invalid value at 'max_results' (TYPE_UINT32), %q", text)}
		}
		paged.Set("pageSize", text)
	}
	return paged, nil
}

// listBody renders a v1beta4 list: the items and the next page's token,
// with kind when the method returns one.
func listBody(kind string, items []map[string]any, next string) map[string]any {
	body := provider.ListBody("items", items, next)
	if kind != "" {
		body[keyKind] = kind
	}
	return body
}

// wireFailure renders a handler's error. v1beta4 errors carry no status
// field (recorded); a coverage gap stays UNIMPLEMENTED.
func wireFailure(err error) (*gateway.RESTResponse, error) {
	if refused, ok := errors.AsType[failure](err); ok {
		return refused.render(), nil
	}
	wire, ok := status.FromError(err)
	code, mapped := httpCodes[wire.Code()]
	if !ok || !mapped {
		return nil, err
	}
	return apiError(code, wire.Code().String(), "", wire.Message()), nil
}

// httpCodes maps the statuses a handler returns onto HTTP codes.
var httpCodes = map[codes.Code]int{codes.InvalidArgument: http.StatusBadRequest, codes.FailedPrecondition: http.StatusBadRequest,
	codes.NotFound: http.StatusNotFound, codes.AlreadyExists: http.StatusConflict}

func randomBytes(n int) []byte {
	value := make([]byte, n)
	_, _ = rand.Read(value) // crypto/rand.Read never returns an error
	return value
}
