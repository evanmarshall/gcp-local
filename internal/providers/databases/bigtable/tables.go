package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// tableAdmin translates one authorized REST table request into the emulator's
// table-admin gRPC API. Table schemas live only in the emulator; the provider
// publishes canonical table names to the shared index for IAM ancestry.
type tableAdmin struct {
	provider *Provider
	admin    adminpb.BigtableTableAdminClient
	scope    state.Scope
	request  *gateway.RESTRequest
}

// instance is the canonical parent of the request's tables.
func (t tableAdmin) instance() string {
	return provider.InstanceName(t.scope.Project, t.request.Path["instance"])
}

// table is the canonical name of the request's table.
func (t tableAdmin) table() string {
	return tableName(t.scope.Project, t.request.Path["instance"], t.request.Path["table"])
}

// tableIndexed builds the shared-index entry of one table.
func (t tableAdmin) tableIndexed(table string) state.Resource {
	project, instance := t.scope.Project, t.request.Path["instance"]
	return state.Resource{
		Name: tableResource(project, instance, table), Service: serviceName,
		Kind: kindTable, Parent: instanceResource(project, instance), Scope: t.scope,
	}
}

// Table fields the provider doesn't model, by JSON path (adminpb
// CreateTableRequest, Table); each is refused with a gap when set.
var unmodeledTable = []string{
	"table.changeStreamConfig", "table.automatedBackupPolicy", "table.tieredStorageConfig", "table.rowKeySchema",
}

// create forwards CreateTable, then publishes the table. A failed publication
// removes the table the emulator just created.
func (t tableAdmin) create(ctx context.Context) (*gateway.RESTResponse, error) {
	request := &adminpb.CreateTableRequest{}
	if err := provider.DecodeProto(t.request.JSON, "", request, gap, unmodeledTable...); err != nil {
		return nil, err
	}
	if request.GetParent() != "" && request.GetParent() != t.instance() {
		return nil, status.Error(codes.InvalidArgument, "The parent does not match the request path")
	}
	if request.GetTableId() == "" {
		return nil, status.Error(codes.InvalidArgument, "A table requires tableId")
	}
	if !tableID.MatchString(request.GetTableId()) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid table id %q", request.GetTableId())
	}
	request.Parent = t.instance()
	name := t.instance() + "/tables/" + request.GetTableId()
	created, err := t.admin.CreateTable(ctx, request)
	if err != nil {
		return nil, tableStatus(err, name)
	}
	if indexErr := t.provider.index.Put(serviceName, t.tableIndexed(request.GetTableId())); indexErr != nil {
		if _, undoErr := t.admin.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: created.GetName()}); undoErr != nil {
			return nil, fmt.Errorf("index the table (%w) and roll back (%w)", indexErr, undoErr)
		}
		return nil, fmt.Errorf("index the table: %w", indexErr)
	}
	return render(created)
}

// schemaView refuses a table view other than the schema view the emulator
// returns (GetTable's default).
func schemaView(query url.Values) error {
	switch view := query.Get("view"); view {
	case "", "VIEW_UNSPECIFIED", "SCHEMA_VIEW":
		return nil
	default:
		return gap("table view " + view)
	}
}

// get forwards GetTable; the emulator returns the schema view.
func (t tableAdmin) get(ctx context.Context) (*gateway.RESTResponse, error) {
	if err := schemaView(t.request.Query); err != nil {
		return nil, err
	}
	table, err := t.admin.GetTable(ctx, &adminpb.GetTableRequest{Name: t.table()})
	if err != nil {
		return nil, tableStatus(err, t.table())
	}
	return render(table)
}

// tablePaging is ListTables's paging; GCP's refusals are recorded. A page
// size of 0 lets the server choose, which is not recorded.
var tablePaging = provider.Paging{
	Default: 1000, Max: 1000,
	InvalidToken: status.Error(codes.InvalidArgument, "Invalid token"),
	NegativeSize: status.Error(codes.InvalidArgument, "Invalid request: Page size cannot be negative"),
}

// list forwards ListTables. The emulator returns names in map order, so the
// provider sorts them by name, as GCP does (recorded), and pages them.
func (t tableAdmin) list(ctx context.Context) (*gateway.RESTResponse, error) {
	if view := t.request.Query.Get("view"); view != "" && view != "NAME_ONLY" && view != "VIEW_UNSPECIFIED" {
		return nil, gap("table list view " + view)
	}
	listing, err := t.admin.ListTables(ctx, &adminpb.ListTablesRequest{Parent: t.instance()})
	if err != nil {
		return nil, backendStatus(err)
	}
	tables := listing.GetTables()
	slices.SortFunc(tables, func(a, b *adminpb.Table) int { return strings.Compare(a.GetName(), b.GetName()) })
	page, next, err := provider.Page(tables, t.request.Query, tablePaging, gap)
	if err != nil {
		return nil, err
	}
	return render(&adminpb.ListTablesResponse{Tables: page, NextPageToken: next})
}

// modifyFamilies forwards ModifyColumnFamilies. A drop or update of a family
// the table lacks is GCP's 404 (recorded for drop); the emulator would fail
// it as an internal error.
func (t tableAdmin) modifyFamilies(ctx context.Context) (*gateway.RESTResponse, error) {
	request := &adminpb.ModifyColumnFamiliesRequest{}
	if err := provider.DecodeProto(t.request.JSON, "", request, gap); err != nil {
		return nil, err
	}
	request.Name = t.table()
	current, err := t.admin.GetTable(ctx, &adminpb.GetTableRequest{Name: t.table()})
	if err != nil {
		return nil, tableStatus(err, t.table())
	}
	for _, change := range request.GetModifications() {
		_, exists := current.GetColumnFamilies()[change.GetId()]
		switch {
		case change.GetDrop() && !exists:
			return nil, status.Errorf(codes.NotFound, "Cannot drop non-existent family %s", change.GetId())
		case change.GetUpdate() != nil && !exists:
			return nil, status.Errorf(codes.NotFound, "Cannot update non-existent family %s", change.GetId())
		}
	}
	table, err := t.admin.ModifyColumnFamilies(ctx, request)
	if err != nil {
		return nil, tableStatus(err, t.table())
	}
	return render(table)
}

// update forwards UpdateTable (PATCH with an updateMask query parameter) for
// deletionProtection, the one field the emulator applies, and answers with a
// stored operation that is already done, as GCP does (recorded).
func (t tableAdmin) update(ctx context.Context) (*gateway.RESTResponse, error) {
	table := &adminpb.Table{}
	if err := provider.DecodeProto(t.request.JSON, "table", table, gap, unmodeledTable...); err != nil {
		return nil, err
	}
	mask := t.request.Query.Get("updateMask")
	for path := range strings.SplitSeq(mask, ",") {
		if path = strings.TrimSpace(path); path != "deletionProtection" && path != "deletion_protection" {
			return nil, gap("table update of " + strconv.Quote(path))
		}
	}
	table.Name = t.table()
	request := &adminpb.UpdateTableRequest{Table: table, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"deletion_protection"}}}
	if _, err := t.admin.UpdateTable(ctx, request); err != nil {
		return nil, tableStatus(err, t.table())
	}
	result, err := t.typedTable(ctx)
	if err != nil {
		return nil, err
	}
	var zone string
	if err := t.provider.withInstance(t.scope, t.request.Path["instance"], func(_ *scopeState, record *instanceRecord) error {
		zone = record.Cluster.Location
		return nil
	}); err != nil {
		return nil, err
	}
	id, err := t.provider.operations.StartImmediate(t.scope, operationTarget(t.table(), zone), verbUpdateTable)
	if err != nil {
		return nil, err //nolint:wrapcheck // the store's scope refusal
	}
	return t.provider.started(t.scope, id, result, nil)
}

// typedTable reads the request's table as an operation response: its JSON
// with "@type".
func (t tableAdmin) typedTable(ctx context.Context) (map[string]any, error) {
	updated, err := t.admin.GetTable(ctx, &adminpb.GetTableRequest{Name: t.table()})
	if err != nil {
		return nil, tableStatus(err, t.table())
	}
	encoded, err := protojson.Marshal(updated)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode the Bigtable table: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, status.Errorf(codes.Internal, "encode the Bigtable table: %v", err)
	}
	result["@type"] = typeTable
	return result, nil
}

// remove forwards DeleteTable, then unpublishes the table and drops its policy.
func (t tableAdmin) remove(ctx context.Context) (*gateway.RESTResponse, error) {
	if _, err := t.admin.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: t.table()}); err != nil {
		return nil, tableStatus(err, t.table())
	}
	if err := t.provider.forgetTable(t.scope, t.request.Path["instance"], t.request.Path["table"]); err != nil {
		return nil, err
	}
	return emptyResponse(), nil
}

// tableStatus renders the emulator's table refusals in GCP's words
// (recorded): a missing table, a duplicate, and a protected delete.
func tableStatus(err error, name string) error {
	switch status.Code(err) {
	case codes.NotFound:
		return tableNotFound(name)
	case codes.AlreadyExists:
		return status.Errorf(codes.AlreadyExists, "Table already exists: %s : Table %s already exists.", name, provider.LastSegment(name))
	case codes.FailedPrecondition:
		return status.Errorf(codes.FailedPrecondition,
			"Unable to delete table %s because the deletion protection field is set to true.", name)
	default:
		return backendStatus(err)
	}
}

// tableNotFound is GCP's 404 for a missing table (recorded).
func tableNotFound(name string) error {
	return status.Errorf(codes.NotFound, "Not found: %s", name)
}

// deleteInstance removes the provider-owned instance and, as in GCP, its
// tables. A table with deletion protection blocks the whole delete before any
// table is removed.
func (t tableAdmin) deleteInstance(ctx context.Context) (*gateway.RESTResponse, error) {
	instance := t.request.Path["instance"]
	if err := t.provider.existingInstance(t.scope, instance); err != nil {
		return nil, err
	}
	listing, err := t.admin.ListTables(ctx, &adminpb.ListTablesRequest{Parent: t.instance()})
	if err != nil {
		return nil, backendStatus(err)
	}
	for _, listed := range listing.GetTables() {
		table, getErr := t.admin.GetTable(ctx, &adminpb.GetTableRequest{Name: listed.GetName()})
		if getErr != nil {
			return nil, backendStatus(getErr)
		}
		if table.GetDeletionProtection() {
			return nil, status.Errorf(codes.FailedPrecondition, "Table %s is protected from deletion", listed.GetName())
		}
	}
	for _, listed := range listing.GetTables() {
		if _, deleteErr := t.admin.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: listed.GetName()}); deleteErr != nil {
			return nil, backendStatus(deleteErr)
		}
		id := listed.GetName()[strings.LastIndexByte(listed.GetName(), '/')+1:]
		if forgetErr := t.provider.forgetTable(t.scope, instance, id); forgetErr != nil {
			return nil, forgetErr
		}
	}
	if err := t.provider.removeInstance(t.scope, instance); err != nil {
		return nil, err
	}
	return emptyResponse(), nil
}

// removeInstance drops the record and its index entry together, as on create.
func (p *Provider) removeInstance(scope state.Scope, instance string) error {
	return p.withInstance(scope, instance, func(s *scopeState, _ *instanceRecord) error {
		if err := p.forget(scope, instanceResource(scope.Project, instance)); err != nil {
			return err
		}
		delete(s.instances, instance)
		return nil
	})
}

// render encodes one admin response message in the REST JSON mapping.
func render(message proto.Message) (*gateway.RESTResponse, error) {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode the Bigtable response: %v", err)
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded)}, nil
}

// backendStatus keeps the emulator's status code and message without the
// client-side wrapping. A non-status failure is an internal error.
func backendStatus(err error) error {
	if converted, ok := status.FromError(err); ok {
		return status.Error(converted.Code(), converted.Message())
	}
	return status.Error(codes.Internal, "Bigtable backend failure")
}
