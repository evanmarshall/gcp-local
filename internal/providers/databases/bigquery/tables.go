package bigquery

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// useEngine runs one operation on the scope's engine under the backend gate,
// starting it on first use. Callers have already authorized.
func (p *Provider) useEngine(ctx context.Context, scope state.Scope, operation func(context.Context, *engineBackend) error) error {
	return p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, driver backend.Backend) error { //nolint:wrapcheck // wire statuses pass through
		engine, err := engineOf(driver)
		if err != nil {
			return err
		}
		return operation(ctx, engine)
	})
}

// tableCopy returns a copy of one table taken under the scope lock, or nil;
// callers render the copy, never the stored record.
func (p *Provider) tableCopy(scope state.Scope, dataset, table string) *tableRecord {
	var copied *tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.tables[tableKey(dataset, table)]; record != nil {
			copied = record.clone()
		}
		return nil
	})
	return copied
}

// clone copies a table record; the schema document is replaced, never
// changed in place, so it is shared.
func (t *tableRecord) clone() *tableRecord {
	copied := *t
	copied.Labels = maps.Clone(t.Labels)
	copied.ViewRefs = slices.Clone(t.ViewRefs)
	return &copied
}

// tableBody is the slice of the Table resource tables.insert serves.
type tableBody struct {
	TableReference struct {
		TableID string `json:"tableId"`
	} `json:"tableReference"`
	Schema       map[string]any    `json:"schema"`
	Labels       map[string]string `json:"labels"`
	FriendlyName string            `json:"friendlyName"`
	View         any               `json:"view"`
}

// decodeTable reads and validates a tables.insert body.
func (p *Provider) decodeTable(request *gateway.RESTRequest, scope state.Scope) (tableBody, string, error) {
	var body tableBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return body, "", status.Error(codes.InvalidArgument, "Invalid table body")
	}
	table := cmp.Or(request.Path["tableId"], body.TableReference.TableID)
	switch dataset := request.Path["datasetId"]; {
	case !validTableID(table):
		return body, "", status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
	case body.View != nil:
		return body, "", gap("views through tables.insert; create them with CREATE VIEW")
	case p.datasetCopy(scope, dataset) == nil:
		return body, "", provider.NotFound(kindDataset, dataset)
	case body.Schema == nil || body.Schema[keyFields] == nil:
		return body, "", status.Error(codes.InvalidArgument, "A table requires a schema with fields")
	}
	return body, table, nil
}

// tablesCreate creates one table in the engine and stores its metadata.
func (p *Provider) tablesCreate(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	body, table, err := p.decodeTable(request, scope)
	if err != nil {
		return nil, err
	}
	definitions, err := columnDefinitions(schemaFields(body.Schema))
	if err != nil {
		return nil, err
	}
	var created *tableRecord
	err = p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		if err := p.admitTable(scope, dataset, table); err != nil {
			return err
		}
		if _, err := engine.query(ctx, "CREATE TABLE "+canonicalPath(scope.Project, dataset, table)+" ("+definitions+")", nil); err != nil {
			return err
		}
		var err error
		if created, err = p.storeTable(scope, dataset, table, body); err == nil {
			return nil
		}
		// No metadata names the new engine table; drop it so a later create
		// of the same name does not collide with it.
		if dropErr := dropEngineTable(ctx, engine, dataset, table, false); dropErr != nil {
			return errors.Join(err, dropErr)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, created)}, nil
}

// storeTable indexes a table the engine created and stores its metadata,
// returning a copy.
func (p *Provider) storeTable(scope state.Scope, dataset, table string, body tableBody) (*tableRecord, error) {
	var created *tableRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		owner := s.datasets[dataset]
		if owner == nil {
			return provider.NotFound(kindDataset, dataset)
		}
		if err := p.indexTable(scope, dataset, table); err != nil {
			return err
		}
		record := &tableRecord{Dataset: dataset, ID: table, Type: typeTable, Schema: body.Schema, Labels: maps.Clone(body.Labels),
			FriendlyName: body.FriendlyName, Location: owner.Location, CreatedAt: p.clock()}
		s.tables[tableKey(dataset, table)] = record
		created = record.clone()
		return nil
	})
	return created, err //nolint:wrapcheck // the callback returns wire statuses and wrapped index errors
}

// admitTable refuses a table whose dataset is gone, one that already exists,
// or one the engine could not keep apart from another: the engine folds case
// and joins path parts with underscores. The caller holds the engine gate,
// which a dataset delete also holds, so an admitted dataset stays.
func (p *Provider) admitTable(scope state.Scope, dataset, table string) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback returns wire statuses
		if s.datasets[dataset] == nil {
			return provider.NotFound(kindDataset, dataset)
		}
		if s.tables[tableKey(dataset, table)] != nil {
			return status.Errorf(codes.AlreadyExists, "Already Exists: Table %s:%s.%s", scope.Project, dataset, table)
		}
		for _, record := range s.tables {
			if foldKey(record.Dataset, record.ID) == foldKey(dataset, table) ||
				storageKey(record.Dataset, record.ID) == storageKey(dataset, table) {
				return collisionGap(tableName{record.Dataset, record.ID}, tableName{dataset, table})
			}
		}
		return nil
	})
}

// indexTable publishes one table for IAM ancestry under its dataset.
func (p *Provider) indexTable(scope state.Scope, dataset, table string) error {
	if err := p.index.Put(serviceName, state.Resource{
		Name: tableResource(scope.Project, dataset, table), Service: serviceName,
		Kind: kindTable, Parent: datasetResource(scope.Project, dataset), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the table: %w", err)
	}
	return nil
}

// tableWire renders one table document from its already-fetched record; the
// scope lock is held by the caller, so this must not re-enter the store.
func (p *Provider) tableWire(scope state.Scope, dataset, table string, record *tableRecord) map[string]any {
	document := map[string]any{
		keyKind:     "bigquery#table",
		"id":        scope.Project + ":" + dataset + "." + table,
		keyETag:     etagOf(record.CreatedAt),
		keySelfLink: selfLink + scope.Project + "/datasets/" + dataset + "/tables/" + table,
		"tableReference": map[string]any{
			"datasetId": dataset, keyProjectID: scope.Project, "tableId": table,
		},
		keySchema:          record.Schema,
		keyType:            cmp.Or(record.Type, typeTable),
		keyLocation:        cmp.Or(record.Location, "US"),
		keyCreationTime:    strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
		"lastModifiedTime": strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
	}
	if record.Type == typeView {
		document["view"] = map[string]any{keyQuery: record.ViewQuery, "useLegacySql": false}
	} else {
		// Storage counters: streamed rows stay in the buffer and are not counted.
		for _, counter := range []string{"numBytes", "numLongTermBytes", "numRows", "numActiveLogicalBytes",
			"numTotalLogicalBytes", "numLongTermLogicalBytes"} {
			document[counter] = "0"
		}
	}
	addDescriptors(document, record.Labels, record.FriendlyName)
	return document
}

func (p *Provider) tablesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, provider.NotFound(kindTable, table)
	}
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, record)}, nil
}

// tablesList returns the dataset's tables.
func (p *Provider) tablesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for key, record := range s.tables {
			if strings.HasPrefix(key, dataset+"/") {
				items = append(items, p.tableWire(scope, dataset, record.ID, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"tables": items}}, nil
}

// tablesUpdate replaces labels and the friendly name.
func (p *Provider) tablesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	var body struct {
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	// The whole body decodes before anything changes, so a bad field
	// changes nothing.
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid table update")
	}
	var document map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.tables[tableKey(dataset, table)]
		if record == nil {
			return provider.NotFound(kindTable, table)
		}
		record.Labels, record.FriendlyName = maps.Clone(body.Labels), body.FriendlyName
		document = p.tableWire(scope, dataset, table, record.clone())
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // the callback returns a wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// tablesDelete drops one table or view from the engine, then its metadata.
func (p *Provider) tablesDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, provider.NotFound(kindTable, table)
	}
	if err := p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		return p.dropTables(ctx, engine, scope, []*tableRecord{record})
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// dropEngineTable drops one table or view from the engine, if it exists.
func dropEngineTable(ctx context.Context, engine *engineBackend, dataset, table string, view bool) error {
	kind := "TABLE"
	if view {
		kind = "VIEW"
	}
	_, err := engine.query(ctx, "DROP "+kind+" IF EXISTS "+canonicalPath(engine.project, dataset, table), nil)
	return err
}

// dropTables drops tables from the engine and then removes their metadata.
func (p *Provider) dropTables(ctx context.Context, engine *engineBackend, scope state.Scope, records []*tableRecord) error {
	for _, record := range records {
		if err := dropEngineTable(ctx, engine, record.Dataset, record.ID, record.Type == typeView); err != nil {
			return err
		}
		if err := p.scopes.With(scope, func(s *scopeState) error {
			delete(s.tables, tableKey(record.Dataset, record.ID))
			return p.unindexTables(scope, record.Dataset, []string{record.ID})
		}); err != nil {
			return err //nolint:wrapcheck // index errors are already wrapped
		}
	}
	return nil
}

// rowsBody is the tabledata.insertAll request.
type rowsBody struct {
	Rows []struct {
		JSON     map[string]any `json:"json"`
		InsertID string         `json:"insertId"`
	} `json:"rows"`
	SkipInvalidRows     bool   `json:"skipInvalidRows"`
	IgnoreUnknownValues bool   `json:"ignoreUnknownValues"`
	TemplateSuffix      string `json:"templateSuffix"`
}

// rowsInsert streams rows into the engine. Like the streaming API, a request
// with an invalid row inserts nothing: that row is reported invalid and the
// others stopped. insertId deduplication is not modeled.
func (p *Provider) rowsInsert(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, provider.NotFound(kindTable, table)
	}
	fields := schemaFields(record.Schema)
	rows, refused, err := decodeRows(request.JSON, record, fields)
	if refused != nil || err != nil {
		return refused, err
	}
	var failed int
	err = p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		var err error
		failed, err = engine.insertRows(ctx, dataset, table, fields, rows)
		return err
	})
	var bad badValueError
	switch {
	case err == nil:
		return &gateway.RESTResponse{JSON: map[string]any{keyKind: "bigquery#tableDataInsertAllResponse"}}, nil
	case failed >= 0 && (errors.As(err, &bad) || status.Code(err) == codes.InvalidArgument):
		return insertErrors(failed, len(rows), err.Error()), nil
	default:
		return nil, err
	}
}

// decodeRows reads an insertAll body's rows. A row with a field the schema
// lacks is reported in insertErrors unless ignoreUnknownValues is set.
func decodeRows(raw []byte, record *tableRecord, fields []map[string]any) ([]map[string]any, *gateway.RESTResponse, error) {
	var body rowsBody
	if json.Unmarshal(raw, &body) != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "Invalid insertAll body")
	}
	switch {
	case len(body.Rows) == 0:
		return nil, nil, status.Error(codes.InvalidArgument, "insertAll requires at least one row")
	case body.SkipInvalidRows:
		return nil, nil, gap("insertAll skipInvalidRows")
	case body.TemplateSuffix != "":
		return nil, nil, gap("insertAll templateSuffix")
	case record.Type == typeView:
		return nil, nil, status.Errorf(codes.InvalidArgument, "Cannot insert into view %s.%s", record.Dataset, record.ID)
	}
	rows := make([]map[string]any, 0, len(body.Rows))
	for index, row := range body.Rows {
		if row.JSON == nil {
			return nil, nil, status.Error(codes.InvalidArgument, "Each row requires a json object")
		}
		if unknown := unknownField(fields, row.JSON); unknown != "" && !body.IgnoreUnknownValues {
			return nil, insertErrors(index, len(body.Rows), "no such field: "+unknown+"."), nil
		}
		rows = append(rows, row.JSON)
	}
	return rows, nil, nil
}

// unknownField names a row field the schema does not have.
func unknownField(fields []map[string]any, row map[string]any) string {
	for name := range row {
		known := false
		for _, field := range fields {
			if field["name"] == name {
				known = true
			}
		}
		if !known {
			return name
		}
	}
	return ""
}

// insertErrors reports one invalid row; every other row was stopped.
func insertErrors(invalid, total int, message string) *gateway.RESTResponse {
	errorsList := make([]any, 0, total)
	for index := range total {
		entry := map[string]any{"reason": "stopped", "message": ""}
		if index == invalid {
			entry = map[string]any{"reason": "invalid", "message": message}
		}
		errorsList = append(errorsList, map[string]any{"index": index, "errors": []any{entry}})
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyKind: "bigquery#tableDataInsertAllResponse", "insertErrors": errorsList}}
}

// rowsOptions are the tabledata.list query parameters served; selectedFields
// is a coverage gap.
var rowsOptions = []string{"maxResults", optionPageToken, optionStartIndex, optionInt64Timestamp, optionTimestampFormat}

// rowsList reads a table's rows from the engine, in insertion order.
func (p *Provider) rowsList(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := validateRowsOptions(request.Query); err != nil {
		return nil, err
	}
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, provider.NotFound(kindTable, table)
	}
	if record.Type == typeView {
		return nil, status.Errorf(codes.InvalidArgument, "Cannot list a table of type VIEW.")
	}
	int64Timestamp, err := timestampFormat(request.Query)
	if err != nil {
		return nil, err
	}
	var result *engineResult
	if err := p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		var err error
		result, err = engine.query(ctx, "SELECT "+columnList(record.Schema)+" FROM "+canonicalPath(scope.Project, dataset, table), nil)
		return err
	}); err != nil {
		return nil, err
	}
	start, end, err := resultPage(request.Query, len(result.rows))
	if err != nil {
		return nil, err
	}
	document := map[string]any{
		keyKind:     "bigquery#tableDataList",
		keyETag:     etagOf(record.CreatedAt),
		"totalRows": strconv.Itoa(len(result.rows)),
		"rows":      cellRows(result.columns, result.rows[start:end], int64Timestamp),
	}
	if end < len(result.rows) {
		document[optionPageToken] = strconv.Itoa(end)
	}
	return &gateway.RESTResponse{JSON: partial(request.Query, document)}, nil
}

// validateRowsOptions refuses a tabledata.list query parameter not served.
func validateRowsOptions(query url.Values) error {
	for key := range query {
		if !slices.Contains(systemOptions, key) && !slices.Contains(rowsOptions, key) {
			return gap("query parameter " + key)
		}
	}
	return nil
}

// columnList names a table's columns in schema order.
func columnList(schema map[string]any) string {
	fields := schemaFields(schema)
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, "`"+fieldName(field)+"`")
	}
	return strings.Join(columns, ", ")
}

// tableIam reads or writes the table's own IAM policy, which inherits the
// dataset's through the index ancestry; it never touches the dataset policy.
func (p *Provider) tableIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	if p.tableCopy(scope, dataset, table) == nil {
		return nil, provider.NotFound(kindTable, table)
	}
	resource := tableResource(scope.Project, dataset, table)
	if call.Method == methodTablesGetIam {
		policy, err := p.engine.GetPolicy(resource)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read the table policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy.Wire()}, nil
	}
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(resource, envelope.Policy)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the table policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// schemaFields returns the schema's field list.
func schemaFields(schema map[string]any) []map[string]any {
	fields, _ := schema[keyFields].([]any)
	out := []map[string]any{}
	for _, field := range fields {
		if entry, ok := field.(map[string]any); ok {
			out = append(out, entry)
		}
	}
	return out
}
