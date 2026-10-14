package bigquery

import (
	"bytes"
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
// starting it on first use, after dropping the tables that expired. Callers
// have already authorized.
func (p *Provider) useEngine(ctx context.Context, scope state.Scope, operation func(context.Context, *engineBackend) error) error {
	return p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, driver backend.Backend) error { //nolint:wrapcheck // wire statuses pass through
		engine, err := engineOf(driver)
		if err != nil {
			return err
		}
		if err := p.dropExpired(ctx, engine, scope); err != nil {
			return err
		}
		return operation(ctx, engine)
	})
}

// tableCopy returns a copy of one live table taken under the scope lock, or
// nil; an expired table reads as gone. Callers render the copy, never the
// stored record.
func (p *Provider) tableCopy(scope state.Scope, dataset, table string) *tableRecord {
	var copied *tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.tables[tableKey(dataset, table)]; record != nil && !record.expired(p.clock()) {
			copied = record.clone()
		}
		return nil
	})
	return copied
}

// clone copies a table record. The schema document and the partitioning
// records are replaced, never changed in place, so they are shared.
func (t *tableRecord) clone() *tableRecord {
	copied := *t
	copied.Labels = maps.Clone(t.Labels)
	copied.ViewRefs = slices.Clone(t.ViewRefs)
	copied.Clustering = slices.Clone(t.Clustering)
	return &copied
}

// tableFields lists the Table schema fields the provider doesn't model
// (BigQuery v2 discovery, Table, TableSchema, TableFieldSchema), each refused
// with a gap when set: CMEK, external and materialized tables, and column
// options. Its output-only fields are dropped. Names the schema lacks are
// ignored, as GCP ignores them.
var tableFields = provider.Fields{
	Unmodeled: append([]string{
		"encryptionConfiguration", "managedTableType",
		"biglakeConfiguration", "resourceTags", "materializedView", "externalCatalogTableOptions",
		"defaultRoundingMode", "model", "tableConstraints", "tableReplicationInfo", "maxStaleness",
		"externalDataConfiguration", "defaultCollation", "schema.foreignTypeInfo",
	}, columnOptions()...),
	OutputOnly: []string{
		"numLongTermLogicalBytes", "numLongTermPhysicalBytes", keyLocation, "replicas", "numActivePhysicalBytes",
		"numLongTermBytes", "numActiveLogicalBytes", "restrictions", "snapshotDefinition", "id", "numPartitions",
		"numTimeTravelPhysicalBytes", keyType, "numCurrentPhysicalBytes", "cloneDefinition", "streamingBuffer",
		keyCreationTime, "numTotalLogicalBytes", "numBytes", "numTotalPhysicalBytes", keyETag, keySelfLink,
		"materializedViewStatus", keyLastModified, "partitionDefinition", "numRows", "numPhysicalBytes",
	},
	IgnoreUnknown: true,
}

// columnOptions names the TableFieldSchema options the provider doesn't
// model, at every nesting depth BigQuery allows (15).
func columnOptions() []string {
	options := []string{
		"generatedColumn", "maxLength", "precision", "timestampPrecision", "dataPolicies",
		"dataGovernanceTagsInfo", "defaultValueExpression", "roundingMode", "rangeElementType", "collation",
		"dataPolicyList", "foreignTypeDefinition", "categories", "scale",
	}
	var paths []string
	for depth, prefix := 1, "schema.fields"; depth <= 15; depth, prefix = depth+1, prefix+".fields" {
		for _, option := range options {
			paths = append(paths, prefix+"."+option)
		}
	}
	return paths
}

// columnSchema is one modeled column of a table schema.
// PolicyTags is accepted only empty: the google Terraform provider sends
// {"names": []} on every column.
type columnSchema struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Mode        string         `json:"mode"`
	Description string         `json:"description"`
	Fields      []columnSchema `json:"fields"`
	PolicyTags  *struct {
		Names []string `json:"names"`
	} `json:"policyTags"`
}

// taggedColumn names the first column, at any depth, that carries policy
// tags, or "".
func taggedColumn(columns []columnSchema) string {
	for _, column := range columns {
		if column.PolicyTags != nil && len(column.PolicyTags.Names) > 0 {
			return column.Name
		}
		if name := taggedColumn(column.Fields); name != "" {
			return name
		}
	}
	return ""
}

// tableBody is the slice of the Table resource tables.insert and
// tables.patch serve. Schema is the request's schema as sent, which the
// provider stores and returns.
type tableBody struct {
	Kind           string `json:"kind"`
	TableReference struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
		TableID   string `json:"tableId"`
	} `json:"tableReference"`
	Fields *struct {
		Fields []columnSchema `json:"fields"`
	} `json:"schema"`
	Labels                 map[string]*string     `json:"labels"`
	FriendlyName           patchText              `json:"friendlyName"`
	Description            patchText              `json:"description"`
	View                   json.RawMessage        `json:"view"`
	TimePartitioning       *timePartitioningBody  `json:"timePartitioning"`
	RangePartitioning      *rangePartitioningBody `json:"rangePartitioning"`
	Clustering             clusteringBody         `json:"clustering"`
	RequirePartitionFilter *bool                  `json:"requirePartitionFilter"`
	ExpirationTime         optionalInt            `json:"expirationTime"`
	Schema                 map[string]any         `json:"-"`
}

// decodeTableBody strictly decodes a table body and keeps its schema as sent.
func decodeTableBody(raw []byte) (tableBody, error) {
	var body tableBody
	if err := provider.DecodeJSON(raw, "", &body, gap, tableFields); err != nil {
		return body, err
	}
	if body.Fields != nil {
		if column := taggedColumn(body.Fields.Fields); column != "" {
			return body, gap("column policyTags (column " + column + ")")
		}
	}
	var schema struct {
		Schema map[string]any `json:"schema"`
	}
	_ = json.Unmarshal(raw, &schema) // DecodeJSON accepted the same body.
	body.Schema = schema.Schema
	return body, nil
}

// invalidTableID is GCP's refusal of a table ID (recorded).
func invalidTableID(table string) error {
	return failure(codes.InvalidArgument, reasonInvalid, fmt.Sprintf("Invalid table ID %q.", table))
}

// tableNotFound is GCP's 404 for a missing table (recorded).
func tableNotFound(project, dataset, table string) error {
	return failure(codes.NotFound, reasonNotFound, "Not found: Table "+project+":"+dataset+"."+table)
}

// decodeTable reads and validates a tables.insert body.
func (p *Provider) decodeTable(request *gateway.RESTRequest, scope state.Scope) (tableBody, string, error) {
	body, err := decodeTableBody(request.JSON)
	if err != nil {
		return body, "", err
	}
	table := cmp.Or(request.Path[keyTableID], body.TableReference.TableID)
	switch dataset := request.Path[keyDatasetID]; {
	case !validTableID(table):
		return body, "", invalidTableID(table)
	case len(body.View) > 0 && string(body.View) != jsonNull:
		return body, "", gap("views through tables.insert; create them with CREATE VIEW")
	case p.datasetCopy(scope, dataset) == nil:
		return body, "", datasetNotFound(scope.Project, dataset)
	case body.Schema == nil || body.Schema[keyFields] == nil:
		return body, "", status.Error(codes.InvalidArgument, "A table requires a schema with fields")
	}
	return body, table, nil
}

// tablesCreate creates one table in the engine and stores its metadata. An
// ingestion-time partitioned table gets the hidden partition-time column.
func (p *Provider) tablesCreate(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path[keyDatasetID]
	body, table, err := p.decodeTable(request, scope)
	if err != nil {
		return nil, err
	}
	definitions, shape, err := p.tableDefinition(body)
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
		if created, err = p.storeTable(scope, dataset, table, body, shape); err == nil {
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
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, created, false)}, nil
}

// tableDefinition validates a new table's columns and layout and renders its
// engine columns, the hidden pseudo-column of an ingestion-time table
// included.
func (p *Provider) tableDefinition(body tableBody) (string, layout, error) {
	fields := schemaFields(body.Schema)
	definitions, err := columnDefinitions(fields)
	if err != nil {
		return "", layout{}, err
	}
	shape, err := newLayout(body, fields, p.clock())
	if err != nil {
		return "", layout{}, err
	}
	if shape.time != nil && shape.time.Field == "" {
		definitions += ", " + pseudoDefinition(shape.time)
	}
	return definitions, shape, nil
}

// storeTable indexes a table the engine created and stores its metadata,
// with its dataset's default expirations, returning a copy.
func (p *Provider) storeTable(scope state.Scope, dataset, table string, body tableBody, shape layout) (*tableRecord, error) {
	var created *tableRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		owner := s.datasets[dataset]
		if owner == nil {
			return datasetNotFound(scope.Project, dataset)
		}
		if err := p.indexTable(scope, dataset, table); err != nil {
			return err
		}
		now := p.clock()
		record := &tableRecord{Dataset: dataset, ID: table, Type: typeTable, Schema: body.Schema, Labels: setLabels(nil, body.Labels),
			FriendlyName: body.FriendlyName.Value, Description: body.Description.Value, Location: owner.Location, CreatedAt: now, ModifiedAt: now}
		shape.applyDefaults(owner, now)
		shape.store(record)
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
			return datasetNotFound(scope.Project, dataset)
		}
		if s.tables[tableKey(dataset, table)] != nil {
			return failure(codes.AlreadyExists, reasonDuplicate, "Already Exists: Table "+scope.Project+":"+dataset+"."+table)
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
// echoFilter is layoutWire's.
func (p *Provider) tableWire(scope state.Scope, dataset, table string, record *tableRecord, echoFilter bool) map[string]any {
	document := map[string]any{
		keyKind:     "bigquery#table",
		"id":        scope.Project + ":" + dataset + "." + table,
		keyETag:     etagOf(cmp.Or(record.ModifiedAt, record.CreatedAt)),
		keySelfLink: selfLink + scope.Project + "/datasets/" + dataset + "/tables/" + table,
		"tableReference": map[string]any{
			keyDatasetID: dataset, keyProjectID: scope.Project, keyTableID: table,
		},
		keySchema:       record.Schema,
		keyType:         cmp.Or(record.Type, typeTable),
		keyLocation:     cmp.Or(record.Location, "US"),
		keyCreationTime: strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
		keyLastModified: strconv.FormatInt(cmp.Or(record.ModifiedAt, record.CreatedAt).UnixMilli(), 10),
	}
	layoutWire(document, record, echoFilter)
	partitionDefinition(document, record)
	if record.Type == typeView {
		document["view"] = map[string]any{keyQuery: record.ViewQuery, keyUseLegacySQL: false}
	} else {
		// Storage counters: streamed rows stay in the buffer and are not counted.
		for _, counter := range []string{"numBytes", "numLongTermBytes", "numRows", "numActiveLogicalBytes",
			"numTotalLogicalBytes", "numLongTermLogicalBytes"} {
			document[counter] = "0"
		}
	}
	addDescriptors(document, record.Labels, record.FriendlyName, record.Description)
	return document
}

func (p *Provider) tablesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := refuseOptions(request.Query, "view", "selectedFields"); err != nil {
		return nil, err
	}
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, tableNotFound(scope.Project, dataset, table)
	}
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, record, true)}, nil
}

// tablesList returns one page of the dataset's tables by ID, as GCP orders
// them (recorded), with totalItems. A page token the provider didn't issue
// reads the first page, as GCP does (recorded).
func (p *Provider) tablesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path[keyDatasetID]
	if p.datasetCopy(scope, dataset) == nil {
		return nil, datasetNotFound(scope.Project, dataset)
	}
	var items []map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		var ids []string
		for _, record := range s.tables {
			if record.Dataset == dataset && !record.expired(p.clock()) {
				ids = append(ids, record.ID)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			items = append(items, tableItem(scope.Project, s.tables[tableKey(dataset, id)]))
		}
		return nil
	})
	page, next, err := listPage(items, request.Query)
	if errors.Is(err, errBadPageToken) {
		first := url.Values{optionMaxResults: request.Query[optionMaxResults]}
		page, next, err = listPage(items, first)
	}
	if err != nil {
		return nil, err
	}
	document := provider.ListBody("tables", page, next)
	document[keyKind], document[keyETag], document["totalItems"] = "bigquery#tableList", etagOf(p.clock()), len(items)
	return &gateway.RESTResponse{JSON: document}, nil
}

// tableItem is one tables.list entry (recorded fields, partitioning and
// clustering included); a view carries its legacy-SQL flag, as the reference
// documents.
func tableItem(project string, record *tableRecord) map[string]any {
	item := map[string]any{
		keyKind: "bigquery#table", "id": project + ":" + record.Dataset + "." + record.ID,
		"tableReference": map[string]any{keyDatasetID: record.Dataset, keyProjectID: project, keyTableID: record.ID},
		keyType:          cmp.Or(record.Type, typeTable),
		keyCreationTime:  strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
	}
	if record.Type == typeView {
		item["view"] = map[string]any{keyUseLegacySQL: false}
	}
	layoutWire(item, record, false)
	addDescriptors(item, record.Labels, record.FriendlyName, "")
	return item
}

// tablesUpdate is tables.patch: labels merge, and friendlyName and
// description change only when the body names them (recorded), as do
// clustering, requirePartitionFilter, partition expiration, and
// expirationTime. A schema is accepted only unchanged; the whole body is
// validated first.
func (p *Provider) tablesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := refuseOptions(request.Query, "autodetect_schema"); err != nil {
		return nil, err
	}
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	body, err := decodeTableBody(request.JSON)
	if err != nil {
		return nil, err
	}
	var document map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.tables[tableKey(dataset, table)]
		if record == nil || record.expired(p.clock()) {
			return tableNotFound(scope.Project, dataset, table)
		}
		if err := p.patchTable(record, body); err != nil {
			return err
		}
		record.Labels = setLabels(record.Labels, body.Labels)
		body.FriendlyName.apply(&record.FriendlyName)
		body.Description.apply(&record.Description)
		record.ModifiedAt = p.clock()
		document = p.tableWire(scope, dataset, table, record.clone(), true)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // the callback returns a wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// patchTable checks a patch's schema and view, which may not change, and
// applies its layout changes. The caller holds the scope lock.
func (p *Provider) patchTable(record *tableRecord, body tableBody) error {
	if body.Schema != nil && !sameJSON(body.Schema, record.Schema) {
		return gap("schema changes through tables.patch; use ALTER TABLE")
	}
	if len(body.View) > 0 && string(body.View) != jsonNull {
		return gap("view changes through tables.patch; use CREATE OR REPLACE VIEW")
	}
	shape, err := patchLayout(body, record, p.clock())
	if err != nil {
		return err
	}
	shape.store(record)
	return nil
}

// sameJSON reports two JSON documents with the same value.
func sameJSON(a, b any) bool {
	left, _ := json.Marshal(a)  //nolint:errchkjson // decoded JSON values
	right, _ := json.Marshal(b) //nolint:errchkjson // decoded JSON values
	return bytes.Equal(left, right)
}

// tablesDelete drops one table or view from the engine, then its metadata.
func (p *Provider) tablesDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, tableNotFound(scope.Project, dataset, table)
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

// rowsBody is the tabledata.insertAll request. traceId only labels the
// request in GCP's logs.
type rowsBody struct {
	Kind    string `json:"kind"`
	TraceID string `json:"traceId"`
	Rows    []struct {
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
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, tableNotFound(scope.Project, dataset, table)
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
	if err := provider.DecodeJSON(raw, "", &body, gap, provider.Fields{IgnoreUnknown: true}); err != nil {
		return nil, nil, err
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
		entry := map[string]any{keyReason: "stopped", keyMessage: ""}
		if index == invalid {
			entry = map[string]any{keyReason: "invalid", keyMessage: message}
		}
		errorsList = append(errorsList, map[string]any{"index": index, "errors": []any{entry}})
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyKind: "bigquery#tableDataInsertAllResponse", "insertErrors": errorsList}}
}

// rowsOptions are the tabledata.list query parameters served; selectedFields
// is a coverage gap.
var rowsOptions = []string{optionMaxResults, optionPageToken, optionStartIndex, optionInt64Timestamp, optionTimestampFormat}

// rowsList reads a table's rows from the engine, in insertion order.
func (p *Provider) rowsList(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := validateRowsOptions(request.Query); err != nil {
		return nil, err
	}
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	record := p.tableCopy(scope, dataset, table)
	if record == nil {
		return nil, tableNotFound(scope.Project, dataset, table)
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
		if err := p.expirePartitions(ctx, engine, scope, []tableName{{dataset, table}}); err != nil {
			return err
		}
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
// The caller was authorized before the scope lock; a table deleted and
// recreated since has a fresh policy, so the call is authorized again on the
// current table under the lock that guards its existence, and the policy is
// read or written under that same lock.
func (p *Provider) tableIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	resource := tableResource(scope.Project, dataset, table)
	version, write, err := tablePolicyRequest(call, request)
	if err != nil {
		return nil, err
	}
	var response *gateway.RESTResponse
	err = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.tables[tableKey(dataset, table)]; record == nil || record.expired(p.clock()) {
			return tableNotFound(scope.Project, dataset, table)
		}
		decision, err := p.gate.Check(call, iam.Target{Name: resource}, resource)
		if err := refused(call, call.Method, resource, decision, err); err != nil {
			return err
		}
		response, err = p.applyTablePolicy(resource, version, write)
		return err
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return response, nil
}

// tablePolicyRequest decodes a table getIamPolicy's requested version, or a
// setIamPolicy's policy.
func tablePolicyRequest(call gateway.Call, request *gateway.RESTRequest) (int, *iam.Policy, error) {
	if call.Method == methodTablesGetIam {
		version, err := provider.RequestedPolicyVersion(request, "options.requestedPolicyVersion", gap)
		return version, nil, err //nolint:wrapcheck // wire statuses
	}
	policy, err := decodeTablePolicy(request.JSON)
	if err != nil {
		return 0, nil, err
	}
	return 0, &policy, nil
}

// applyTablePolicy reads the table's policy at version, or writes write.
func (p *Provider) applyTablePolicy(resource string, version int, write *iam.Policy) (*gateway.RESTResponse, error) {
	if write == nil {
		policy, err := p.engine.GetPolicy(resource)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read the table policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy.View(version).Wire()}, nil
	}
	stored, err := p.engine.SetPolicy(resource, *write)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the table policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: stored}, nil
}

// decodeTablePolicy strictly decodes a table setIamPolicy body. A table
// policy takes no conditions: GCP refuses a version above 1 (recorded), with
// no errors[] entry.
func decodeTablePolicy(raw []byte) (iam.Policy, error) {
	var envelope struct {
		Policy     iam.Policy `json:"policy"`
		UpdateMask string     `json:"updateMask"`
	}
	if err := provider.DecodeJSON(raw, "", &envelope, gap, provider.Fields{IgnoreUnknown: true}); err != nil {
		return iam.Policy{}, err
	}
	for path := range strings.SplitSeq(envelope.UpdateMask, ",") {
		if path = strings.TrimSpace(path); path != "" && path != "bindings" && path != keyETag {
			return iam.Policy{}, gap("setIamPolicy updateMask " + path)
		}
	}
	if envelope.Policy.Version > 1 {
		return iam.Policy{}, &apiError{status: status.Newf(codes.InvalidArgument,
			"Unsupported policy version %d; max supported version is 1.", envelope.Policy.Version)}
	}
	for _, binding := range envelope.Policy.Bindings {
		if len(binding.Condition) > 0 {
			return iam.Policy{}, status.Error(codes.InvalidArgument, "Table policies take no conditional bindings")
		}
	}
	return envelope.Policy, nil
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
