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
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	methodOperationsList  = "bigtable.operations.list"
	methodOperationGet    = "bigtable.operations.get"
	methodOperationDelete = "bigtable.operations.delete"
	methodOperationCancel = "bigtable.operations.cancel"
	methodTableOpGet      = "bigtable.tableOperations.get"
	methodTableOpDelete   = "bigtable.tableOperations.delete"
	methodTableOpCancel   = "bigtable.tableOperations.cancel"
)

// Update permissions several methods share.
const (
	permInstancesUpdate = "bigtable.instances.update"
	permTablesUpdate    = "bigtable.tables.update"
)

// permissions follows the documented Bigtable access-control rows.
var permissions = map[string]string{
	methodInstancesCreate: "bigtable.instances.create",
	methodInstancesGet:    "bigtable.instances.get",
	methodInstancesList:   "bigtable.instances.list",
	methodInstancesUpdate: permInstancesUpdate,
	methodInstancesDelete: "bigtable.instances.delete",
	methodInstancesGetIam: "bigtable.instances.getIamPolicy",
	methodInstancesSetIam: "bigtable.instances.setIamPolicy",
	methodClustersList:    "bigtable.clusters.list",
	methodClustersGet:     "bigtable.clusters.get",
	methodTablesCreate:    "bigtable.tables.create",
	methodTablesGet:       "bigtable.tables.get",
	methodTablesList:      "bigtable.tables.list",
	methodTablesUpdate:    permTablesUpdate,
	methodTablesDelete:    "bigtable.tables.delete",
	methodTablesModifyFam: permTablesUpdate,
	methodTablesGetIam:    "bigtable.tables.getIamPolicy",
	methodTablesSetIam:    "bigtable.tables.setIamPolicy",
	// Operation permissions are not documented or recorded; an operation is
	// read like its resource and cancelled or deleted like an update of it.
	methodOperationsList:  "bigtable.instances.list",
	methodOperationGet:    "bigtable.instances.get",
	methodOperationDelete: permInstancesUpdate,
	methodOperationCancel: permInstancesUpdate,
	methodTableOpGet:      "bigtable.tables.get",
	methodTableOpDelete:   permTablesUpdate,
	methodTableOpCancel:   permTablesUpdate,
}

// rpcNames are the admin RPCs the REST methods transcode, which GCP names in
// a missing-credential 401 (recorded for GetInstance).
var rpcNames = map[string]string{
	methodInstancesCreate: instanceRPC + "CreateInstance", methodInstancesGet: instanceRPC + "GetInstance",
	methodInstancesList: instanceRPC + "ListInstances", methodInstancesUpdate: instanceRPC + "PartialUpdateInstance",
	methodInstancesDelete: instanceRPC + "DeleteInstance", methodInstancesGetIam: instanceRPC + "GetIamPolicy",
	methodInstancesSetIam: instanceRPC + "SetIamPolicy", methodClustersList: instanceRPC + "ListClusters",
	methodClustersGet: instanceRPC + "GetCluster", methodTablesCreate: tableRPC + "CreateTable",
	methodTablesGet: tableRPC + "GetTable", methodTablesList: tableRPC + "ListTables",
	methodTablesUpdate: tableRPC + "UpdateTable", methodTablesDelete: tableRPC + "DeleteTable",
	methodTablesModifyFam: tableRPC + "ModifyColumnFamilies", methodTablesGetIam: tableRPC + "GetIamPolicy",
	methodTablesSetIam: tableRPC + "SetIamPolicy", methodOperationsList: operationsRPC + "ListOperations",
	methodOperationGet: operationsRPC + "GetOperation", methodOperationDelete: operationsRPC + "DeleteOperation",
	methodOperationCancel: operationsRPC + "CancelOperation", methodTableOpGet: operationsRPC + "GetOperation",
	methodTableOpDelete: operationsRPC + "DeleteOperation", methodTableOpCancel: operationsRPC + "CancelOperation",
}

// RPC name prefixes.
const (
	instanceRPC   = "google.bigtable.admin.v2.BigtableInstanceAdmin."
	tableRPC      = "google.bigtable.admin.v2.BigtableTableAdmin."
	operationsRPC = "google.longrunning.Operations."
	// adminHost is the API host conditions and 401s name.
	adminHost = "bigtableadmin.googleapis.com"
)

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

// Long-running operation type URLs (recorded: bigtable-instance-table-admin).
const (
	typeCreateInstanceMetadata = "type.googleapis.com/google.bigtable.admin.v2.CreateInstanceMetadata"
	typeUpdateInstanceMetadata = "type.googleapis.com/google.bigtable.admin.v2.UpdateInstanceMetadata"
	typeUpdateTableMetadata    = "type.googleapis.com/google.bigtable.admin.v2.UpdateTableMetadata"
	typeInstance               = "type.googleapis.com/google.bigtable.admin.v2.Instance"
	typeTable                  = "type.googleapis.com/google.bigtable.admin.v2.Table"
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
// clusters. Tables and column families live in the scope's emulator backend;
// operations in the provider's operation store.
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

// attributes are the IAM Condition attributes of an instance or table IAM
// target, as Bigtable documents them (resource.type
// bigtableadmin.googleapis.com/Instance or /Table, resource.name
// projects/P/instances/I[/tables/T]); recorded for an instance-only grant.
func attributes(target string) iam.ResourceAttributes {
	name, ok := strings.CutPrefix(target, "//"+adminHost+"/")
	switch {
	case !ok:
		return iam.ResourceAttributes{}
	case strings.Contains(name, "/tables/"):
		return iam.ResourceAttributes{Name: name, Type: adminHost + "/Table", Service: adminHost}
	default:
		return iam.ResourceAttributes{Name: name, Type: adminHost + "/Instance", Service: adminHost}
	}
}

// instanceResource names one instance in the IAM index.
func instanceResource(project, instance string) string {
	return "//" + adminHost + "/projects/" + project + "/instances/" + instance
}

// tableResource names one table in the IAM index.
func tableResource(project, instance, table string) string {
	return instanceResource(project, instance) + "/tables/" + table
}

func tableName(project, instance, table string) string {
	return provider.InstanceName(project, instance) + "/tables/" + table
}

func clusterName(project, instance, cluster string) string {
	return provider.InstanceName(project, instance) + "/clusters/" + cluster
}
