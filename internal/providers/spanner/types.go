// Package spanner implements the Spanner admin v1 control plane: instance
// configs (as modeled fixtures), instances, databases, and per-resource IAM.
// Data-plane sessions/queries are out of scope. The
// spanner-instance-database-admin recording covers the instance/database
// lifecycle, instance IAM, and the DDL slice; other methods are unrecorded.
package spanner

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the Spanner backend registration.
const serviceName = "spanner"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodConfigsGet        = "spanner.instanceConfigs.get"
	methodConfigsList       = "spanner.instanceConfigs.list"
	methodInstancesCreate   = "spanner.instances.create"
	methodInstancesGet      = "spanner.instances.get"
	methodInstancesList     = "spanner.instances.list"
	methodInstancesUpdate   = "spanner.instances.update"
	methodInstancesDelete   = "spanner.instances.delete"
	methodInstancesGetIam   = "spanner.instances.getIamPolicy"
	methodInstancesSetIam   = "spanner.instances.setIamPolicy"
	methodDatabasesCreate   = "spanner.databases.create"
	methodDatabasesGet      = "spanner.databases.get"
	methodDatabasesList     = "spanner.databases.list"
	methodDatabasesUpdate   = "spanner.databases.update"
	methodDatabasesDelete   = "spanner.databases.delete"
	methodDatabasesGetIam   = "spanner.databases.getIamPolicy"
	methodDatabasesSetIam   = "spanner.databases.setIamPolicy"
	methodDatabasesApplyDdl = "spanner.databases.applyDdl"
	methodDatabasesGetDdl   = "spanner.databases.getDdl"
	methodBackupsList       = "spanner.backups.list"
)

// permissions follows the documented Spanner access-control rows.
var permissions = map[string]string{
	methodConfigsGet:        "spanner.instanceConfigs.get",
	methodConfigsList:       "spanner.instanceConfigs.list",
	methodInstancesCreate:   "spanner.instances.create",
	methodInstancesGet:      "spanner.instances.get",
	methodInstancesList:     "spanner.instances.list",
	methodInstancesUpdate:   "spanner.instances.update",
	methodInstancesDelete:   "spanner.instances.delete",
	methodInstancesGetIam:   "spanner.instances.getIamPolicy",
	methodInstancesSetIam:   "spanner.instances.setIamPolicy",
	methodDatabasesCreate:   "spanner.databases.create",
	methodDatabasesGet:      "spanner.databases.get",
	methodDatabasesList:     "spanner.databases.list",
	methodDatabasesUpdate:   "spanner.databases.update",
	methodDatabasesDelete:   "spanner.databases.drop",
	methodDatabasesGetIam:   "spanner.databases.getIamPolicy",
	methodDatabasesSetIam:   "spanner.databases.setIamPolicy",
	methodDatabasesApplyDdl: "spanner.databases.updateDdl",
	methodDatabasesGetDdl:   "spanner.databases.getDdl",
	methodBackupsList:       "spanner.backups.list",
}

// Wire keys and resource kinds shared by the handlers.
const (
	keyName        = "name"
	keyDisplayName = "displayName"
	kindInstance   = "instance"
	kindDatabase   = "database"
)

// uid is the instance-id matcher.
var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,30}[a-z0-9]$`)

// databaseID is Spanner's database-id rule: 2-30 characters,
// [a-z][a-z0-9_-]*[a-z0-9]; underscores are valid (the recording uses gl_db).
var databaseID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,28}[a-z0-9]$`)

// instanceConfig is one modeled regional instance config.
type instanceConfig struct {
	Name        string
	DisplayName string
}

// instanceRecord is one stored Spanner instance.
type instanceRecord struct {
	Name            string
	Config          string
	DisplayName     string
	ProcessingUnits int
	Labels          map[string]string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// databaseRecord is one stored Spanner database.
type databaseRecord struct {
	Instance  string
	Name      string
	Tables    []ddlTable
	State     string
	CreatedAt time.Time
}

// scopeState is one project slice of provider state.
type scopeState struct {
	instances map[string]*instanceRecord
	databases map[string]*databaseRecord
}

func newScopeState() scopeState {
	return scopeState{
		instances: map[string]*instanceRecord{},
		databases: map[string]*databaseRecord{},
	}
}

func (s *scopeState) reset() {
	s.instances = map[string]*instanceRecord{}
	s.databases = map[string]*databaseRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("spanner requires registry, manager, index, and iam")

// wireError builds the Spanner error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the Spanner not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// scopeOf names one project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// instanceResource names one instance in the IAM index.
func instanceResource(project, instance string) string {
	return "//spanner.googleapis.com/projects/" + project + "/instances/" + instance
}

// databaseResource names one database in the IAM index.
func databaseResource(project, instance, database string) string {
	return instanceResource(project, instance) + "/databases/" + database
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// instanceName renders the canonical instance name.
func instanceName(project, instance string) string {
	return "projects/" + project + "/instances/" + instance
}

// databaseName renders the canonical database name.
func databaseName(project, instance, database string) string {
	return instanceName(project, instance) + "/databases/" + database
}

// stamp renders one RFC3339 timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
