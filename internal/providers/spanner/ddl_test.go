package spanner

import (
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Statements and canonical getDdl results pinned by the
// spanner-instance-database-admin recording.
const (
	recordedTable       = "CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)"
	recordedColumn      = "ALTER TABLE Items ADD COLUMN Note STRING(64)"
	recordedCanonical   = "CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n) PRIMARY KEY(Id)"
	recordedAfterColumn = "CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n  Note STRING(64),\n) PRIMARY KEY(Id)"
)

func TestDDLCanonicalFormMatchesRecording(t *testing.T) {
	tables, err := applyDDL(nil, []string{recordedTable})
	if err != nil {
		t.Fatalf("apply the table: %v", err)
	}
	if got := renderDDL(tables); !reflect.DeepEqual(got, []string{recordedCanonical}) {
		t.Fatalf("canonical DDL = %q", got)
	}
	altered, err := applyDDL(tables, []string{recordedColumn})
	if err != nil {
		t.Fatalf("apply the column: %v", err)
	}
	if got := renderDDL(altered); !reflect.DeepEqual(got, []string{recordedAfterColumn}) {
		t.Fatalf("altered DDL = %q", got)
	}
	if got := renderDDL(tables); !reflect.DeepEqual(got, []string{recordedCanonical}) {
		t.Fatalf("applyDDL mutated its input: %q", got)
	}
}

func TestDDLCanonicalizesTerraformStatement(t *testing.T) {
	tables, err := applyDDL(nil, []string{"CREATE TABLE entries (id INT64) PRIMARY KEY(id)"})
	if err != nil {
		t.Fatalf("apply the Terraform table: %v", err)
	}
	want := []string{"CREATE TABLE entries (\n  id INT64,\n) PRIMARY KEY(id)"}
	if got := renderDDL(tables); !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical DDL = %q", got)
	}
}

func TestDDLRejectsUnmodeledStatements(t *testing.T) {
	cases := map[string]codes.Code{
		"CREATE INDEX ItemsByName ON Items(Name)":                                codes.Unimplemented,
		"ALTER TABLE Items DROP COLUMN Name":                                     codes.Unimplemented,
		"CREATE TABLE T (Id INT64 DEFAULT (1)) PRIMARY KEY (Id)":                 codes.Unimplemented,
		"CREATE TABLE C (Id INT64) PRIMARY KEY (Id), INTERLEAVE IN PARENT Items": codes.Unimplemented,
		"CREATE TABLE Items (Id INT64) PRIMARY KEY (Id)":                         codes.FailedPrecondition,
		"ALTER TABLE Missing ADD COLUMN Note STRING(64)":                         codes.FailedPrecondition,
		"CREATE TABLE Bad (Id WIDGET) PRIMARY KEY (Id)":                          codes.InvalidArgument,
		"CREATE TABLE NoKey (Id INT64) PRIMARY KEY (Other)":                      codes.FailedPrecondition,
	}
	base, err := applyDDL(nil, []string{recordedTable})
	if err != nil {
		t.Fatal(err)
	}
	for statement, want := range cases {
		_, err := applyDDL(base, []string{statement})
		if got := status.Code(err); got != want {
			t.Errorf("%s: code = %s, want %s (%v)", statement, got, want, err)
		}
	}
}

func TestDDLBatchIsAtomic(t *testing.T) {
	base, err := applyDDL(nil, []string{recordedTable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyDDL(base, []string{recordedColumn, "DROP TABLE Items"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("batch error = %v", err)
	}
	if got := renderDDL(base); !reflect.DeepEqual(got, []string{recordedCanonical}) {
		t.Fatalf("failed batch changed the schema: %q", got)
	}
}

func TestDatabaseIDRules(t *testing.T) {
	statements := map[string]string{
		createGlDB:                  "gl_db",
		"CREATE DATABASE events":    "events",
		"create database `my-db`":   "my-db",
		"CREATE DATABASE `a` extra": "",
	}
	for statement, want := range statements {
		if got := databaseIDFromStatement(statement); got != want {
			t.Errorf("%q -> %q, want %q", statement, got, want)
		}
	}
	for id, valid := range map[string]bool{
		"gl_db": true, "ab": true, "a": false, "1db": false, "db_": false, "db-": false,
		"abcdefghijklmnopqrstuvwxyz0123": true, "abcdefghijklmnopqrstuvwxyz01234": false, "Db": false,
	} {
		if databaseID.MatchString(id) != valid {
			t.Errorf("database id %q valid = %t", id, !valid)
		}
	}
}
