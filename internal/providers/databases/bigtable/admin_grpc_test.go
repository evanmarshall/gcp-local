package bigtable

import (
	"strings"

	"context"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refused requires one gRPC status code and message.
func refused(t *testing.T, step string, err error, code codes.Code, message string) {
	t.Helper()
	if s := status.Convert(err); s.Code() != code || s.Message() != message {
		t.Fatalf("%s: got %v, want %s %q", step, err, code, message)
	}
}

// succeeded fails the test at one step's error.
func succeeded(t *testing.T, step string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", step, err)
	}
}

// TestAdminClientsOverGRPC drives the official Go instance and table admin
// clients through the gRPC gateway onto the REST provider methods: instance
// create and reads, table create, list, info, deletion protection, IAM
// policies, a denial, and deletes.
func TestAdminClientsOverGRPC(t *testing.T) {
	h := newHarness(t)
	d := serveData(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instances, err := bigtable.NewInstanceAdminClient(ctx, testProject, option.WithGRPCConn(d.conn(t, testActor)))
	succeeded(t, "instance admin client", err)
	succeeded(t, "CreateInstance", instances.CreateInstance(ctx, &bigtable.InstanceConf{InstanceId: testInstance, DisplayName: "demo", ClusterId: testCluster,
		Zone: testZone, NumNodes: 1, StorageType: bigtable.SSD, InstanceType: bigtable.PRODUCTION}))
	info, err := instances.InstanceInfo(ctx, testInstance)
	succeeded(t, "InstanceInfo", err)
	if info.DisplayName != "demo" || info.InstanceType != bigtable.PRODUCTION {
		t.Fatalf("InstanceInfo = %+v", info)
	}
	clusters, err := instances.Clusters(ctx, testInstance)
	succeeded(t, "Clusters", err)
	if len(clusters) != 1 || clusters[0].Zone != testZone || clusters[0].ServeNodes != 1 {
		t.Fatalf("Clusters = %+v", clusters)
	}
	cluster, err := instances.GetCluster(ctx, testInstance, testCluster)
	succeeded(t, "GetCluster", err)
	if cluster.Name != testCluster {
		t.Fatalf("GetCluster = %+v", cluster)
	}
	_, err = instances.InstanceIAM(testInstance).Policy(ctx)
	succeeded(t, "instance policy", err)
	tablesOverGRPC(ctx, t, d)
	succeeded(t, "DeleteInstance", instances.DeleteInstance(ctx, testInstance))
	_, err = instances.InstanceInfo(ctx, testInstance)
	refused(t, "deleted instance", err, codes.NotFound, "Instance "+provider.InstanceName(testProject, testInstance)+" not found.")
}

// tablesOverGRPC creates, reads, protects, and deletes a table with the Go
// table admin client after an ungranted create is denied.
func tablesOverGRPC(ctx context.Context, t *testing.T, d *dataGateway) {
	t.Helper()
	tables, err := bigtable.NewAdminClient(ctx, testProject, testInstance, option.WithGRPCConn(d.conn(t, testActor)))
	succeeded(t, "table admin client", err)
	viewer, err := bigtable.NewAdminClient(ctx, testProject, testInstance, option.WithGRPCConn(d.conn(t, viewerActor)))
	succeeded(t, "viewer table admin client", err)
	refused(t, "viewer create", viewer.CreateTable(ctx, testTable), codes.PermissionDenied,
		"Access denied. Missing IAM permission: bigtable.tables.create.")
	succeeded(t, "CreateTable", tables.CreateTableFromConf(ctx, &bigtable.TableConf{TableID: testTable,
		Families: map[string]bigtable.GCPolicy{familyOne: bigtable.MaxVersionsPolicy(1)}}))
	names, err := tables.Tables(ctx)
	succeeded(t, "Tables", err)
	if len(names) != 1 || names[0] != testTable {
		t.Fatalf("Tables = %v", names)
	}
	succeeded(t, "ModifyColumnFamilies", tables.CreateColumnFamily(ctx, testTable, familyTwo))
	info, err := tables.TableInfo(ctx, testTable)
	succeeded(t, "TableInfo", err)
	if len(info.FamilyInfos) != 2 {
		t.Fatalf("TableInfo = %+v", info)
	}
	succeeded(t, "protect", tables.UpdateTableWithDeletionProtection(ctx, testTable, bigtable.Protected))
	refused(t, "protected delete", tables.DeleteTable(ctx, testTable), codes.FailedPrecondition,
		"Unable to delete table "+tableName(testProject, testInstance, testTable)+" because the deletion protection field is set to true.")
	succeeded(t, "unprotect", tables.UpdateTableWithDeletionProtection(ctx, testTable, bigtable.Unprotected))
	_, err = tables.TableIAM(testTable).Policy(ctx)
	succeeded(t, "table policy", err)
	succeeded(t, "DeleteTable", tables.DeleteTable(ctx, testTable))
}

// TestAdminGapsOverGRPC asserts each gRPC admin refusal and its message.
func TestAdminGapsOverGRPC(t *testing.T) {
	h := newHarness(t)
	d := serveData(t, h)
	ctx := context.Background()
	conn := d.conn(t, testActor)
	instances, tables := adminpb.NewBigtableInstanceAdminClient(conn), adminpb.NewBigtableTableAdminClient(conn)
	name := provider.InstanceName(testProject, testInstance)
	_, err := instances.CreateAppProfile(ctx, &adminpb.CreateAppProfileRequest{Parent: name, AppProfileId: "p"})
	refused(t, "CreateAppProfile", err, codes.Unimplemented, "No provider implements /google.bigtable.admin.v2.BigtableInstanceAdmin/CreateAppProfile")
	_, err = tables.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name + "/tables/t"})
	refused(t, "TestIamPermissions", err, codes.Unimplemented, "No provider implements /google.bigtable.admin.v2.BigtableTableAdmin/TestIamPermissions")
	_, err = instances.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name + "/materializedViews/v"})
	refused(t, "view policy", err, codes.Unimplemented,
		"No provider implements /google.bigtable.admin.v2.BigtableInstanceAdmin/GetIamPolicy for projects/*/instances/*/materializedViews/* resources")
	// Operations are served only under a Bigtable instance or table name.
	_, err = longrunningpb.NewOperationsClient(conn).GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "operations/projects/p/elsewhere/1"})
	refused(t, "GetOperation", err, codes.Unimplemented, "No provider implements /google.longrunning.Operations/GetOperation")
}

// TestPartialUpdateOverGRPC changes only the masked display name and polls
// the returned operation, which is already done, over gRPC.
func TestPartialUpdateOverGRPC(t *testing.T) {
	h := newHarness(t)
	h.instance()
	d := serveData(t, h)
	ctx := context.Background()
	conn := d.conn(t, testActor)
	name := provider.InstanceName(testProject, testInstance)
	operation, err := adminpb.NewBigtableInstanceAdminClient(conn).PartialUpdateInstance(ctx, &adminpb.PartialUpdateInstanceRequest{
		Instance:   &adminpb.Instance{Name: name, DisplayName: "renamed", Labels: map[string]string{"dropped": "x"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}})
	succeeded(t, "PartialUpdateInstance", err)
	if !operation.GetDone() || !strings.HasPrefix(operation.GetName(), "operations/"+name+"/locations/"+testZone+"/operations/") {
		t.Fatalf("operation = %v", operation)
	}
	polled, err := longrunningpb.NewOperationsClient(conn).GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: operation.GetName()})
	succeeded(t, "GetOperation", err)
	updated := &adminpb.Instance{}
	succeeded(t, "unpack", polled.GetResponse().UnmarshalTo(updated))
	if updated.GetDisplayName() != "renamed" || updated.GetLabels()["env"] != "dev" || updated.GetLabels()["dropped"] != "" {
		t.Fatalf("updated = %v", updated)
	}
}
