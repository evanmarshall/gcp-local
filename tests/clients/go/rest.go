package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// runStorage uses the JSON API client: bucket and object create, read, list, delete.
func runStorage(ctx context.Context, e *env) error {
	endpoint := option.WithEndpoint(e.http + "/storage/v1/")
	client, err := storage.NewClient(ctx, endpoint, option.WithAuthCredentials(e.owner))
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	bucket := client.Bucket("go-matrix-" + e.project)
	if err := bucket.Create(ctx, e.project, &storage.BucketAttrs{
		Location:                 "US-CENTRAL1",
		UniformBucketLevelAccess: storage.UniformBucketLevelAccess{Enabled: true},
		PublicAccessPrevention:   storage.PublicAccessPreventionEnforced,
		SoftDeletePolicy:         &storage.SoftDeletePolicy{RetentionDuration: 0},
	}); err != nil {
		return step("create bucket", err)
	}
	object := bucket.Object("hello.txt")
	writer := object.NewWriter(ctx)
	if _, err := writer.Write([]byte("hello from go")); err != nil {
		return step("write object", err)
	}
	if err := writer.Close(); err != nil {
		return step("upload object", err)
	}
	reader, err := object.NewReader(ctx)
	if err != nil {
		return step("open object", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(data, []byte("hello from go")) {
		return fmt.Errorf("read object: got %q, %v", data, err)
	}
	objects := bucket.Objects(ctx, nil)
	attrs, err := objects.Next()
	if err != nil || attrs.Name != "hello.txt" {
		return fmt.Errorf("list objects: got %v, %v", attrs, err)
	}
	denied, err := storage.NewClient(ctx, endpoint, option.WithAuthCredentials(e.denied))
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.Bucket(bucket.BucketName()).Object("hello.txt").Attrs(ctx)
	if err := expectDenied("ungranted object get", err); err != nil {
		return err
	}
	if err := object.Delete(ctx); err != nil {
		return step("delete object", err)
	}
	return step("delete bucket", bucket.Delete(ctx))
}

type person struct {
	Name string `bigquery:"name"`
	Age  int64  `bigquery:"age"`
}

// runBigQuery creates a dataset and table, streams rows, queries, lists, and deletes.
func runBigQuery(ctx context.Context, e *env) error {
	endpoint := option.WithEndpoint(e.http + "/bigquery/v2/")
	client, err := bigquery.NewClient(ctx, e.project, endpoint, option.WithAuthCredentials(e.owner))
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	dataset := client.Dataset("go_matrix")
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		return step("create dataset", err)
	}
	table := dataset.Table("people")
	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		return step("create table", err)
	}
	if err := table.Inserter().Put(ctx, []*person{{"ada", 36}, {"grace", 45}}); err != nil {
		return step("insert rows", err)
	}
	query := client.Query("SELECT name, age FROM `" + e.project + ".go_matrix.people` WHERE name = 'ada'")
	rows, err := query.Read(ctx)
	if err != nil {
		return step("query", err)
	}
	var got person
	if err := rows.Next(&got); err != nil || got != (person{"ada", 36}) {
		return fmt.Errorf("query rows: got %+v, %v", got, err)
	}
	tables := dataset.Tables(ctx)
	listed, err := tables.Next()
	if err != nil || listed.TableID != "people" {
		return fmt.Errorf("list tables: got %v, %v", listed, err)
	}
	denied, err := bigquery.NewClient(ctx, e.project, endpoint, option.WithAuthCredentials(e.denied))
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.Dataset("go_matrix").Table("people").Metadata(ctx)
	if err := expectDenied("ungranted table get", err); err != nil {
		return err
	}
	return step("delete dataset", dataset.DeleteWithContents(ctx))
}
