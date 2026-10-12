// Package bigtable implements the Bigtable admin v2 control plane. The provider
// owns instances, their one modeled cluster, and per-resource IAM; tables and
// column families are served by Google's Bigtable emulator (bttest) running
// in-process per project. The v2 data plane (row reads and writes) is served
// over gRPC after authorization against the table. The
// bigtable-instance-table-admin recording covers the instance, cluster, table,
// column-family, and instance IAM shapes.
package bigtable

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the Bigtable backend registration.
const serviceName = "bigtable"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodInstancesCreate = "bigtable.instances.create"
	methodInstancesGet    = "bigtable.instances.get"
	methodInstancesList   = "bigtable.instances.list"
	methodInstancesUpdate = "bigtable.instances.update"
	methodInstancesDelete = "bigtable.instances.delete"
	methodInstancesGetIam = "bigtable.instances.getIamPolicy"
	methodInstancesSetIam = "bigtable.instances.setIamPolicy"
	methodClustersList    = "bigtable.clusters.list"
	methodClustersGet     = "bigtable.clusters.get"
	methodTablesCreate    = "bigtable.tables.create"
	methodTablesGet       = "bigtable.tables.get"
	methodTablesList      = "bigtable.tables.list"
	methodTablesUpdate    = "bigtable.tables.update"
	methodTablesDelete    = "bigtable.tables.delete"
	methodTablesModifyFam = "bigtable.tables.modifyColumnFamilies"
	methodTablesGetIam    = "bigtable.tables.getIamPolicy"
	methodTablesSetIam    = "bigtable.tables.setIamPolicy"
)

// permissions follows the documented Bigtable access-control rows.
var permissions = map[string]string{
	methodInstancesCreate: "bigtable.instances.create",
	methodInstancesGet:    "bigtable.instances.get",
	methodInstancesList:   "bigtable.instances.list",
	methodInstancesUpdate: "bigtable.instances.update",
	methodInstancesDelete: "bigtable.instances.delete",
	methodInstancesGetIam: "bigtable.instances.getIamPolicy",
	methodInstancesSetIam: "bigtable.instances.setIamPolicy",
	methodClustersList:    "bigtable.clusters.list",
	methodClustersGet:     "bigtable.clusters.get",
	methodTablesCreate:    "bigtable.tables.create",
	methodTablesGet:       "bigtable.tables.get",
	methodTablesList:      "bigtable.tables.list",
	methodTablesUpdate:    "bigtable.tables.update",
	methodTablesDelete:    "bigtable.tables.delete",
	methodTablesModifyFam: "bigtable.tables.update",
	methodTablesGetIam:    "bigtable.tables.getIamPolicy",
	methodTablesSetIam:    "bigtable.tables.setIamPolicy",
}

// Wire keys and resource kinds shared by the handlers.
const (
	keyName      = "name"
	kindInstance = "instance"
	kindCluster  = "cluster"
	kindTable    = "table"
)

// uid matches instance and cluster ids.
var uid = regexp.MustCompile(`^[a-z][a-z0-9-]{1,30}[a-z0-9]$`)

// tableID matches Bigtable table ids: [_a-zA-Z0-9][-_.a-zA-Z0-9]*, 1-50 chars.
var tableID = regexp.MustCompile(`^[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,49}$`)

// Long-running operation type URLs for instance creation.
const (
	typeCreateInstanceMetadata = "type.googleapis.com/google.bigtable.admin.v2.CreateInstanceMetadata"
	typeInstance               = "type.googleapis.com/google.bigtable.admin.v2.Instance"
)

// defaultEdition is the instance edition GCP assigns when none is requested.
const defaultEdition = "ENTERPRISE"

// clusterRecord is the single modeled cluster of one instance.
type clusterRecord struct {
	Name        string
	Location    string
	ServeNodes  int
	StorageType string
}

// instanceRecord is one stored Bigtable instance.
type instanceRecord struct {
	Name        string
	DisplayName string
	Type        string
	Edition     string
	Labels      map[string]string
	Cluster     *clusterRecord
	CreatedAt   time.Time
}

// scopeState is one project slice of provider-owned state: instances and their
// clusters. Tables and column families live in the scope's emulator backend.
type scopeState struct {
	instances map[string]*instanceRecord
}

func newScopeState() scopeState {
	return scopeState{instances: map[string]*instanceRecord{}}
}

func (s *scopeState) reset() {
	s.instances = map[string]*instanceRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("bigtable requires registry, manager, index, and iam")

// wireError builds the Bigtable error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the Bigtable not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// scopeOf names one project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// instanceResource names one instance in the IAM index.
func instanceResource(project, instance string) string {
	return "//bigtableadmin.googleapis.com/projects/" + project + "/instances/" + instance
}

// tableResource names one table in the IAM index.
func tableResource(project, instance, table string) string {
	return instanceResource(project, instance) + "/tables/" + table
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// instanceName renders the canonical instance name.
func instanceName(project, instance string) string {
	return "projects/" + project + "/instances/" + instance
}

// tableName renders the canonical table name.
func tableName(project, instance, table string) string {
	return instanceName(project, instance) + "/tables/" + table
}

// clusterName renders the canonical cluster name.
func clusterName(project, instance, cluster string) string {
	return instanceName(project, instance) + "/clusters/" + cluster
}

// stamp renders one RFC3339 timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
