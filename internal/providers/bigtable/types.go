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

// instanceResource names one instance in the IAM index.
func instanceResource(project, instance string) string {
	return "//bigtableadmin.googleapis.com/projects/" + project + "/instances/" + instance
}

// tableResource names one table in the IAM index.
func tableResource(project, instance, table string) string {
	return instanceResource(project, instance) + "/tables/" + table
}

func instanceName(project, instance string) string {
	return "projects/" + project + "/instances/" + instance
}

func tableName(project, instance, table string) string {
	return instanceName(project, instance) + "/tables/" + table
}

func clusterName(project, instance, cluster string) string {
	return instanceName(project, instance) + "/clusters/" + cluster
}
