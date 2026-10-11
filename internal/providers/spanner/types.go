// Package spanner fronts Google's official Cloud Spanner emulator with the
// Spanner admin v1 REST surface. The provider authenticates and authorizes
// each call with the shared IAM engine, then forwards it over gRPC to one
// emulator per project scope and renders the response as proto3 JSON. The
// emulator owns instances, databases, schemas, and operations; the provider
// owns IAM policies and the shared resource index entries used for policy
// inheritance. Data-plane sessions/queries are not exposed.
package spanner

import (
	"errors"
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the Spanner backend registration and index entries.
const serviceName = "spanner"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodConfigsGet           = "spanner.instanceConfigs.get"
	methodConfigsList          = "spanner.instanceConfigs.list"
	methodInstancesCreate      = "spanner.instances.create"
	methodInstancesGet         = "spanner.instances.get"
	methodInstancesList        = "spanner.instances.list"
	methodInstancesUpdate      = "spanner.instances.update"
	methodInstancesDelete      = "spanner.instances.delete"
	methodInstancesGetIam      = "spanner.instances.getIamPolicy"
	methodInstancesSetIam      = "spanner.instances.setIamPolicy"
	methodInstanceOperationGet = "spanner.instances.operations.get"
	methodDatabasesCreate      = "spanner.databases.create"
	methodDatabasesGet         = "spanner.databases.get"
	methodDatabasesList        = "spanner.databases.list"
	methodDatabasesUpdate      = "spanner.databases.update"
	methodDatabasesDelete      = "spanner.databases.delete"
	methodDatabasesGetIam      = "spanner.databases.getIamPolicy"
	methodDatabasesSetIam      = "spanner.databases.setIamPolicy"
	methodDatabasesApplyDdl    = "spanner.databases.applyDdl"
	methodDatabasesGetDdl      = "spanner.databases.getDdl"
	methodDatabaseOperationGet = "spanner.databases.operations.get"
	methodBackupsList          = "spanner.backups.list"
)

// permissions follows the documented Spanner access-control rows.
var permissions = map[string]string{
	methodConfigsGet:           "spanner.instanceConfigs.get",
	methodConfigsList:          "spanner.instanceConfigs.list",
	methodInstancesCreate:      "spanner.instances.create",
	methodInstancesGet:         "spanner.instances.get",
	methodInstancesList:        "spanner.instances.list",
	methodInstancesUpdate:      "spanner.instances.update",
	methodInstancesDelete:      "spanner.instances.delete",
	methodInstancesGetIam:      "spanner.instances.getIamPolicy",
	methodInstancesSetIam:      "spanner.instances.setIamPolicy",
	methodInstanceOperationGet: "spanner.instanceOperations.get",
	methodDatabasesCreate:      "spanner.databases.create",
	methodDatabasesGet:         "spanner.databases.get",
	methodDatabasesList:        "spanner.databases.list",
	methodDatabasesUpdate:      "spanner.databases.update",
	methodDatabasesDelete:      "spanner.databases.drop",
	methodDatabasesGetIam:      "spanner.databases.getIamPolicy",
	methodDatabasesSetIam:      "spanner.databases.setIamPolicy",
	methodDatabasesApplyDdl:    "spanner.databases.updateDdl",
	methodDatabasesGetDdl:      "spanner.databases.getDdl",
	methodDatabaseOperationGet: "spanner.databaseOperations.get",
	methodBackupsList:          "spanner.backups.list",
}

// Path variables and resource kinds shared by the handlers.
const (
	pathProject   = "project"
	pathInstance  = "instance"
	pathDatabase  = "database"
	kindInstance  = "instance"
	kindDatabase  = "database"
	kindProject   = "project"
	ownerProjects = "resourcemanager"
)

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("spanner requires registry, manager, index, and iam")

// wireError builds the Spanner error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// scopeOf names one project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
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

// fullName prefixes a canonical Spanner name for the IAM index.
func fullName(name string) string {
	return "//spanner.googleapis.com/" + name
}

// gap reports an unsupported request shape explicitly.
func gap(format string, args ...any) error {
	return status.Error(codes.Unimplemented, "gcp-local Spanner coverage gap: "+fmt.Sprintf(format, args...))
}
