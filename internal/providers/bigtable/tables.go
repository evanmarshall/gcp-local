package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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
	return instanceName(t.scope.Project, t.request.Path["instance"])
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

// create forwards CreateTable, then publishes the table. A failed publication
// removes the table the emulator just created.
func (t tableAdmin) create(ctx context.Context) (*gateway.RESTResponse, error) {
	request := &adminpb.CreateTableRequest{}
	if err := decodeBody(t.request.JSON, request); err != nil {
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
	created, err := t.admin.CreateTable(ctx, request)
	if err != nil {
		return nil, backendStatus(err)
	}
	if indexErr := t.provider.index.Put(serviceName, t.tableIndexed(request.GetTableId())); indexErr != nil {
		if _, undoErr := t.admin.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: created.GetName()}); undoErr != nil {
			return nil, fmt.Errorf("index the table (%w) and roll back (%w)", indexErr, undoErr)
		}
		return nil, fmt.Errorf("index the table: %w", indexErr)
	}
	return render(created)
}

// get forwards GetTable; the emulator returns the schema view.
func (t tableAdmin) get(ctx context.Context) (*gateway.RESTResponse, error) {
	table, err := t.admin.GetTable(ctx, &adminpb.GetTableRequest{Name: t.table()})
	if err != nil {
		return nil, backendStatus(err)
	}
	return render(table)
}

// list forwards ListTables. The emulator returns names only, in map order, so
// the provider sorts them by name.
func (t tableAdmin) list(ctx context.Context) (*gateway.RESTResponse, error) {
	listing, err := t.admin.ListTables(ctx, &adminpb.ListTablesRequest{Parent: t.instance()})
	if err != nil {
		return nil, backendStatus(err)
	}
	tables := listing.GetTables()
	sort.Slice(tables, func(i, j int) bool { return tables[i].GetName() < tables[j].GetName() })
	return render(listing)
}

// modifyFamilies forwards ModifyColumnFamilies.
func (t tableAdmin) modifyFamilies(ctx context.Context) (*gateway.RESTResponse, error) {
	request := &adminpb.ModifyColumnFamiliesRequest{}
	if err := decodeBody(t.request.JSON, request); err != nil {
		return nil, err
	}
	request.Name = t.table()
	table, err := t.admin.ModifyColumnFamilies(ctx, request)
	if err != nil {
		return nil, backendStatus(err)
	}
	return render(table)
}

// update forwards UpdateTable (PATCH with an updateMask query parameter). The
// emulator names every operation projects/my-project/operations/1234, so the
// provider owns and assigns the operation name.
func (t tableAdmin) update(ctx context.Context) (*gateway.RESTResponse, error) {
	table := &adminpb.Table{}
	if err := decodeBody(t.request.JSON, table); err != nil {
		return nil, err
	}
	table.Name = t.table()
	request := &adminpb.UpdateTableRequest{Table: table}
	if mask := t.request.Query.Get("updateMask"); mask != "" {
		request.UpdateMask = &fieldmaskpb.FieldMask{Paths: snakePaths(mask)}
	}
	operation, err := t.admin.UpdateTable(ctx, request)
	if err != nil {
		return nil, backendStatus(err)
	}
	operation.Name = fmt.Sprintf("%s/operations/%d", t.table(), t.provider.operations.Add(1))
	return render(operation)
}

// remove forwards DeleteTable, then unpublishes the table and drops its policy.
func (t tableAdmin) remove(ctx context.Context) (*gateway.RESTResponse, error) {
	if _, err := t.admin.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: t.table()}); err != nil {
		return nil, backendStatus(err)
	}
	if err := t.provider.forget(t.scope, tableResource(t.scope.Project, t.request.Path["instance"], t.request.Path["table"])); err != nil {
		return nil, err
	}
	return emptyResponse(), nil
}

// deleteInstance removes the provider-owned instance and, as in GCP, its
// tables. A table with deletion protection blocks the whole delete before any
// table is removed.
func (t tableAdmin) deleteInstance(ctx context.Context) (*gateway.RESTResponse, error) {
	instance := t.request.Path["instance"]
	if t.provider.instanceLocked(t.scope, instance) == nil {
		return nil, provider.NotFound(kindInstance, instance)
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
		if forgetErr := t.provider.forget(t.scope, tableResource(t.scope.Project, instance, id)); forgetErr != nil {
			return nil, forgetErr
		}
	}
	_ = t.provider.scopes.With(t.scope, func(s *scopeState) error {
		delete(s.instances, instance)
		return nil
	})
	if err := t.provider.forget(t.scope, instanceResource(t.scope.Project, instance)); err != nil {
		return nil, err
	}
	return emptyResponse(), nil
}

// decodeBody parses a REST JSON body into its admin request message. Unknown
// fields are rejected, as the REST API rejects them.
func decodeBody(body json.RawMessage, message proto.Message) error {
	if len(body) == 0 {
		return nil
	}
	if err := protojson.Unmarshal(body, message); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid JSON payload received. %v", err)
	}
	return nil
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

// snakePaths converts a REST updateMask (camelCase, comma-separated) to proto
// field paths.
func snakePaths(mask string) []string {
	paths := make([]string, 0, strings.Count(mask, ",")+1)
	for path := range strings.SplitSeq(mask, ",") {
		var converted strings.Builder
		for _, r := range strings.TrimSpace(path) {
			if r >= 'A' && r <= 'Z' {
				converted.WriteByte('_')
				r += 'a' - 'A'
			}
			converted.WriteRune(r)
		}
		paths = append(paths, converted.String())
	}
	return paths
}
