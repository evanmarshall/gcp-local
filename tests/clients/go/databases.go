package main

import (
	"bytes"
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

// runSpanner creates an instance and database with the admin clients, writes
// and queries a row with the data client, lists databases, and drops both.
func runSpanner(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	instances, err := instance.NewInstanceAdminClient(ctx, conn)
	if err != nil {
		return step("new instance admin client", err)
	}
	defer instances.Close()
	project := "projects/" + e.project
	created, err := instances.CreateInstance(ctx, &instancepb.CreateInstanceRequest{Parent: project, InstanceId: "go-matrix", Instance: &instancepb.Instance{
		Config: project + "/instanceConfigs/emulator-config", DisplayName: "Go matrix", NodeCount: 1,
	}})
	if err != nil {
		return step("create instance", err)
	}
	if _, err := created.Wait(ctx); err != nil {
		return step("wait for instance", err)
	}
	instanceName := project + "/instances/go-matrix"
	databases, err := database.NewDatabaseAdminClient(ctx, conn)
	if err != nil {
		return step("new database admin client", err)
	}
	defer databases.Close()
	operation, err := databases.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent: instanceName, CreateStatement: "CREATE DATABASE `matrix`",
		ExtraStatements: []string{"CREATE TABLE Singers (Id INT64 NOT NULL, Name STRING(100)) PRIMARY KEY (Id)"},
	})
	if err != nil {
		return step("create database", err)
	}
	if _, err := operation.Wait(ctx); err != nil {
		return step("wait for database", err)
	}
	databaseName := instanceName + "/databases/matrix"
	// Native metrics would export to monitoring.googleapis.com; nothing may leave loopback.
	config := spanner.ClientConfig{DisableNativeMetrics: true}
	client, err := spanner.NewClientWithConfig(ctx, databaseName, config, conn)
	if err != nil {
		return step("new data client", err)
	}
	defer client.Close()
	if _, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert("Singers", []string{"Id", "Name"}, []any{1, "Ada"})}); err != nil {
		return step("insert row", err)
	}
	row, err := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT Name FROM Singers WHERE Id = 1"}).Next()
	var name string
	if err != nil || row.Columns(&name) != nil || name != "Ada" {
		return fmt.Errorf("query: got %q, %v", name, err)
	}
	if err := listed(databases.ListDatabases(ctx, &databasepb.ListDatabasesRequest{Parent: instanceName}).Next, databaseName, (*databasepb.Database).GetName); err != nil {
		return step("list databases", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := spanner.NewClientWithConfig(ctx, databaseName, config, deniedConn)
	if err == nil {
		defer denied.Close()
		_, err = denied.Single().ReadRow(ctx, "Singers", spanner.Key{1}, []string{"Name"})
	}
	if err := expectDenied("ungranted read", err); err != nil {
		return err
	}
	client.Close()
	if err := databases.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: databaseName}); err != nil {
		return step("drop database", err)
	}
	return step("delete instance", instances.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: instanceName}))
}

// runBigtable creates an instance and table with the admin clients, writes and
// reads a row with the data client, lists tables, and deletes both.
func runBigtable(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	instances, err := bigtable.NewInstanceAdminClient(ctx, e.project, conn)
	if err != nil {
		return step("new instance admin client", err)
	}
	defer instances.Close()
	if err := instances.CreateInstance(ctx, &bigtable.InstanceConf{
		InstanceId: "go-matrix", DisplayName: "Go matrix", ClusterId: "go-matrix-c1", Zone: "us-central1-b",
		NumNodes: 1, StorageType: bigtable.SSD, InstanceType: bigtable.PRODUCTION,
	}); err != nil {
		return step("create instance", err)
	}
	tables, err := bigtable.NewAdminClient(ctx, e.project, "go-matrix", conn)
	if err != nil {
		return step("new table admin client", err)
	}
	defer tables.Close()
	if err := tables.CreateTableFromConf(ctx, &bigtable.TableConf{TableID: "matrix", Families: map[string]bigtable.GCPolicy{"cf": bigtable.MaxVersionsPolicy(1)}}); err != nil {
		return step("create table", err)
	}
	// The metrics exporter would call monitoring.googleapis.com; nothing may leave loopback.
	config := bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}}
	client, err := bigtable.NewClientWithConfig(ctx, e.project, "go-matrix", config, conn)
	if err != nil {
		return step("new data client", err)
	}
	defer client.Close()
	mutation := bigtable.NewMutation()
	mutation.Set("cf", "greeting", bigtable.Now(), []byte("hello from go"))
	if err := client.Open("matrix").Apply(ctx, "row-1", mutation); err != nil {
		return step("write row", err)
	}
	row, err := client.Open("matrix").ReadRow(ctx, "row-1")
	if err != nil || len(row["cf"]) != 1 || !bytes.Equal(row["cf"][0].Value, []byte("hello from go")) {
		return fmt.Errorf("read row: got %v, %v", row, err)
	}
	names, err := tables.Tables(ctx)
	if err != nil || len(names) != 1 || names[0] != "matrix" {
		return fmt.Errorf("list tables: got %v, %v", names, err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := bigtable.NewClientWithConfig(ctx, e.project, "go-matrix", config, deniedConn)
	if err == nil {
		defer denied.Close()
		_, err = denied.Open("matrix").ReadRow(ctx, "row-1")
	}
	if err := expectDenied("ungranted read", err); err != nil {
		return err
	}
	if err := tables.DeleteTable(ctx, "matrix"); err != nil {
		return step("delete table", err)
	}
	return step("delete instance", instances.DeleteInstance(ctx, "go-matrix"))
}
