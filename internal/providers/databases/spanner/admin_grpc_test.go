package spanner

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// adminConn dials the harness gRPC gateway as one token.
func adminConn(t *testing.T, endpoint, token string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(bearer(token)))
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// refused requires one gRPC status code and message.
func refused(t *testing.T, step string, err error, code codes.Code, message string) {
	t.Helper()
	if s := status.Convert(err); s.Code() != code || s.Message() != message {
		t.Fatalf("%s: got %v, want %s %q", step, err, code, message)
	}
}

// TestAdminClientsOverGRPC drives the Go InstanceAdmin and DatabaseAdmin
// clients through the gRPC gateway onto the REST provider methods: decorated
// instance and database operations, polling through the Operations mixin,
// IAM policies, a denial before the backend, and drop and delete.
func TestAdminClientsOverGRPC(t *testing.T) {
	h := newHarness(t)
	endpoint := h.serveGRPC()
	ctx := context.Background()
	conn := adminConn(t, endpoint, adminToken)
	instances, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	must(t, err)
	databases, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	must(t, err)
	name := "projects/" + testProject + "/instances/" + testInstance
	creating := createInstanceOverGRPC(t, instances)
	peer, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(adminConn(t, endpoint, peerToken)))
	must(t, err)
	_, err = peer.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: name, CreateStatement: "CREATE DATABASE `" + testDatabase + "`"})
	refused(t, "ungranted create", err, codes.PermissionDenied,
		"Operation denied by [IAM permission 'spanner.databases.create' on resource '//spanner.googleapis.com/"+name+"'].")
	making, err := databases.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: name, CreateStatement: "CREATE DATABASE `" + testDatabase + "`"})
	must(t, err)
	db, err := making.Wait(ctx)
	if err != nil || db.GetName() != name+"/databases/"+testDatabase || db.GetCreateTime() == nil || db.GetVersionRetentionPeriod() != defaultVersionRetention {
		t.Fatalf("create database = %v, %v", db, err)
	}
	pollAndList(t, conn, databases, db.GetName(), making.Name(), creating)
	must(t, databases.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: db.GetName()}))
	must(t, instances.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: name}))
	_, err = instances.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: name})
	refused(t, "deleted instance", err, codes.NotFound, "Instance not found")
}

// pollAndList polls both operations through the Operations mixin, lists the
// database, and reads its policy.
func pollAndList(t *testing.T, conn *grpc.ClientConn, databases *database.DatabaseAdminClient, name string, operations ...string) {
	t.Helper()
	ctx := context.Background()
	for _, operation := range operations {
		polled, err := longrunningpb.NewOperationsClient(conn).GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: operation})
		if err != nil || !polled.GetDone() {
			t.Fatalf("poll %s = %v, %v", operation, polled, err)
		}
	}
	listed, err := databases.ListDatabases(ctx, &databasepb.ListDatabasesRequest{Parent: name[:strings.LastIndex(name, "/databases/")]}).Next()
	if err != nil || listed.GetName() != name {
		t.Fatalf("list databases = %v, %v", listed, err)
	}
	if policy, err := databases.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name}); err != nil || len(policy.GetEtag()) == 0 {
		t.Fatalf("database policy = %v, %v", policy, err)
	}
}

// createInstanceOverGRPC creates and reads the test instance with the Go
// client and returns the create operation's name.
func createInstanceOverGRPC(t *testing.T, instances *instance.InstanceAdminClient) string {
	t.Helper()
	ctx := context.Background()
	project := "projects/" + testProject
	creating, err := instances.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: project, InstanceId: testInstance,
		Instance: &instancepb.Instance{Config: project + "/instanceConfigs/regional-us-central1", DisplayName: "demo", ProcessingUnits: 100}})
	must(t, err)
	created, err := creating.Wait(ctx)
	if err != nil || created.GetName() != project+"/instances/"+testInstance || created.GetInstanceType() != instancepb.Instance_PROVISIONED {
		t.Fatalf("create instance = %v, %v", created, err)
	}
	if got, err := instances.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: created.GetName()}); err != nil || got.GetEdition() != instancepb.Instance_STANDARD {
		t.Fatalf("get instance = %v, %v", got, err)
	}
	return creating.Name()
}

// TestAdminGapsOverGRPC asserts each gRPC admin refusal and its message:
// unpaired methods, bindings no route serves, and operation methods Spanner
// does not serve.
func TestAdminGapsOverGRPC(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	conn := adminConn(t, h.serveGRPC(), adminToken)
	instances, databases := instancepb.NewInstanceAdminClient(conn), databasepb.NewDatabaseAdminClient(conn)
	operations := longrunningpb.NewOperationsClient(conn)
	project := "projects/" + testProject
	name := project + "/instances/" + testInstance
	_, err := instances.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name})
	refused(t, "TestIamPermissions", err, codes.Unimplemented, "No provider implements /google.spanner.admin.instance.v1.InstanceAdmin/TestIamPermissions")
	_, err = databases.CreateBackup(ctx, &databasepb.CreateBackupRequest{Parent: name, BackupId: "b"})
	refused(t, "CreateBackup", err, codes.Unimplemented, "No provider implements /google.spanner.admin.database.v1.DatabaseAdmin/CreateBackup")
	_, err = databases.InternalUpdateGraphOperation(ctx, &databasepb.InternalUpdateGraphOperationRequest{Database: name + "/databases/d"})
	refused(t, "unannotated method", err, codes.Unimplemented, "No provider implements /google.spanner.admin.database.v1.DatabaseAdmin/InternalUpdateGraphOperation")
	_, err = databases.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name + "/backups/b"})
	refused(t, "backup policy", err, codes.Unimplemented,
		"No provider implements /google.spanner.admin.database.v1.DatabaseAdmin/GetIamPolicy for projects/*/instances/*/backups/* resources")
	_, err = operations.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: name + "/operations/o"})
	refused(t, "cancel instance operation", err, codes.Unimplemented,
		"No provider implements /google.longrunning.Operations/CancelOperation for projects/*/instances/*/operations/* resources")
	_, err = operations.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: name + "/databases/d/operations"})
	refused(t, "list database operations", err, codes.Unimplemented, "No provider implements /google.longrunning.Operations/ListOperations")
}
