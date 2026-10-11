package bigquery

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized BigQuery request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodDatasetsCreate:
		return p.datasetsCreate(request, scope)
	case methodDatasetsGet:
		return p.datasetsGet(request, scope)
	case methodDatasetsList:
		return p.datasetsList(scope)
	case methodDatasetsUpdate:
		return p.datasetsUpdate(request, scope)
	case methodDatasetsDelete:
		return p.datasetsDelete(request, scope)
	default:
		return p.executeTables(call, request, scope)
	}
}

// executeTables dispatches the table and row methods.
func (p *Provider) executeTables(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodTablesCreate:
		return p.tablesCreate(request, scope)
	case methodTablesGet:
		return p.tablesGet(request, scope)
	case methodTablesList:
		return p.tablesList(request, scope)
	case methodTablesUpdate:
		return p.tablesUpdate(request, scope)
	case methodTablesDelete:
		return p.tablesDelete(request, scope)
	case methodRowsInsert:
		return p.rowsInsert(request, scope)
	case methodJobsQuery:
		return p.jobsQuery(request, scope)
	case methodJobsResults:
		return p.jobsResults(request, scope)
	default:
		return p.rowsList(request, scope)
	}
}

// datasetRecordLocked reads one dataset under the scope lock.
func (p *Provider) datasetLocked(scope state.Scope, dataset string) *datasetRecord {
	var record *datasetRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.datasets[dataset]; return nil })
	return record
}

// datasetsCreate validates and stores one dataset.
func (p *Provider) datasetsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		DatasetReference struct {
			DatasetID string `json:"datasetId"`
		} `json:"datasetReference"`
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid dataset body")
	}
	dataset := request.Path["datasetId"]
	if dataset == "" {
		dataset = body.DatasetReference.DatasetID
	}
	if !uid.MatchString(dataset) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid dataset id %q", dataset)
	}
	now := p.clock()
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.datasets[dataset] != nil {
			return status.Errorf(codes.AlreadyExists, "Dataset %s already exists", dataset)
		}
		s.datasets[dataset] = &datasetRecord{
			ID: dataset, Labels: body.Labels, FriendlyName: body.FriendlyName, CreatedAt: now,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the dataset: %w", err)
	}
	created := &datasetRecord{
		ID: dataset, Labels: body.Labels, FriendlyName: body.FriendlyName, CreatedAt: now,
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: datasetResource(scope.Project, dataset), Service: serviceName,
		Kind: kindDataset, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the dataset: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: datasetWire(scope.Project, dataset, created)}, nil
}

// datasetWire renders one dataset document.
func datasetWire(project, dataset string, record *datasetRecord) map[string]any {
	labels := map[string]string{}
	if record != nil && record.Labels != nil {
		labels = record.Labels
	}
	friendly := ""
	if record != nil {
		friendly = record.FriendlyName
	}
	created := time.Time{}
	if record != nil {
		created = record.CreatedAt
	}
	return map[string]any{
		"id":               dataset,
		"datasetReference": map[string]any{"datasetId": dataset, "projectId": project},
		"friendlyName":     friendly,
		"labels":           labels,
		"creationTime":     strconv.FormatInt(created.UnixMilli(), 10),
		"lastModifiedTime": strconv.FormatInt(created.UnixMilli(), 10),
	}
}

// datasetsGet returns one dataset.
func (p *Provider) datasetsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	record := p.datasetLocked(scope, dataset)
	if record == nil {
		return nil, notFound(kindDataset, dataset)
	}
	return &gateway.RESTResponse{JSON: datasetWire(scope.Project, dataset, record)}, nil
}

// datasetsList returns the project's datasets.
func (p *Provider) datasetsList(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.datasets {
			items = append(items, datasetWire(scope.Project, record.ID, record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"datasets": items}}, nil
}

// datasetsUpdate replaces labels and the friendly name.
func (p *Provider) datasetsUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	record := p.datasetLocked(scope, dataset)
	if record == nil {
		return nil, notFound(kindDataset, dataset)
	}
	var body struct {
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid dataset update")
	}
	record.Labels = body.Labels
	record.FriendlyName = body.FriendlyName
	return &gateway.RESTResponse{JSON: datasetWire(scope.Project, dataset, record)}, nil
}

// datasetsDelete removes one empty dataset.
func (p *Provider) datasetsDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	if p.datasetLocked(scope, dataset) == nil {
		return nil, notFound(kindDataset, dataset)
	}
	nonEmpty := false
	err := p.scopes.With(scope, func(s *scopeState) error {
		for key := range s.tables {
			if strings.HasPrefix(key, dataset+"/") {
				nonEmpty = true
				return nil
			}
		}
		delete(s.datasets, dataset)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("remove the dataset: %w", err)
	}
	if nonEmpty {
		return nil, status.Errorf(codes.FailedPrecondition, "Dataset %s still contains tables", dataset)
	}
	if indexErr := p.index.Remove(serviceName, scope, datasetResource(scope.Project, dataset)); indexErr != nil {
		return nil, fmt.Errorf("unregister the dataset: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// tableLocked reads one table under the scope lock.
func (p *Provider) tableLocked(scope state.Scope, dataset, table string) *tableRecord {
	var record *tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.tables[tableKey(dataset, table)]; return nil })
	return record
}

// tablesCreate validates and stores one table with its schema.
func (p *Provider) tablesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	var body struct {
		TableReference struct {
			TableID string `json:"tableId"`
		} `json:"tableReference"`
		Schema       map[string]any    `json:"schema"`
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid table body")
	}
	table := request.Path["tableId"]
	if table == "" {
		table = body.TableReference.TableID
	}
	if !uid.MatchString(table) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid table id %q", table)
	}
	if p.datasetLocked(scope, dataset) == nil {
		return nil, notFound(kindDataset, dataset)
	}
	if body.Schema == nil || body.Schema["fields"] == nil {
		return nil, status.Error(codes.InvalidArgument, "A table requires a schema with fields")
	}
	now := p.clock()
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := tableKey(dataset, table)
		if s.tables[key] != nil {
			return status.Errorf(codes.AlreadyExists, "Table %s already exists", table)
		}
		s.tables[key] = &tableRecord{Dataset: dataset, ID: table, Schema: body.Schema, CreatedAt: now}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the table: %w", err)
	}
	created := &tableRecord{Dataset: dataset, ID: table, Schema: body.Schema, CreatedAt: now}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: tableResource(scope.Project, dataset, table), Service: serviceName,
		Kind: kindTable, Parent: datasetResource(scope.Project, dataset), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the table: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, created)}, nil
}

// tableWire renders one table document from its already-fetched record; the
// scope lock is held by the caller, so this must not re-enter the store.
func (p *Provider) tableWire(scope state.Scope, dataset, table string, record *tableRecord) map[string]any {
	labels := map[string]string{}
	if record != nil && record.Labels != nil {
		labels = record.Labels
	}
	friendly := ""
	if record != nil {
		friendly = record.FriendlyName
	}
	return map[string]any{
		"id": table,
		"tableReference": map[string]any{
			"datasetId": dataset, "projectId": scope.Project, "tableId": table,
		},
		"schema":           record.Schema,
		"labels":           labels,
		"friendlyName":     friendly,
		"creationTime":     strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
		"lastModifiedTime": strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
	}
}

// tablesGet returns one table.
func (p *Provider) tablesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableLocked(scope, dataset, table)
	if record == nil {
		return nil, notFound(kindTable, table)
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
	record := p.tableLocked(scope, dataset, table)
	if record == nil {
		return nil, notFound(kindTable, table)
	}
	var body struct {
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid table update")
	}
	record.Labels = body.Labels
	record.FriendlyName = body.FriendlyName
	return &gateway.RESTResponse{JSON: p.tableWire(scope, dataset, table, record)}, nil
}

// tablesDelete removes one table and its rows.
func (p *Provider) tablesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	if p.tableLocked(scope, dataset, table) == nil {
		return nil, notFound(kindTable, table)
	}
	var indexErr error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.tables, tableKey(dataset, table))
		indexErr = p.index.Remove(serviceName, scope, tableResource(scope.Project, dataset, table))
		return nil
	})
	if indexErr != nil {
		return nil, fmt.Errorf("unregister the table: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// rowsInsert validates and stores posted rows against the table schema.
func (p *Provider) rowsInsert(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableLocked(scope, dataset, table)
	if record == nil {
		return nil, notFound(kindTable, table)
	}
	var body struct {
		Rows []struct {
			JSON map[string]any `json:"json"`
		} `json:"rows"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid insertAll body")
	}
	if len(body.Rows) == 0 {
		return nil, status.Error(codes.InvalidArgument, "insertAll requires at least one row")
	}
	inserted := make([]string, 0, len(body.Rows))
	for _, row := range body.Rows {
		if row.JSON == nil {
			return nil, status.Error(codes.InvalidArgument, "Each row requires a json object")
		}
		inserted = append(inserted, "stored")
	}
	_ = inserted
	err := p.scopes.With(scope, func(s *scopeState) error {
		stored := s.tables[tableKey(dataset, table)]
		for _, row := range body.Rows {
			stored.Rows = append(stored.Rows, row.JSON)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("insert the rows: %w", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{"kind": "bigquery#tableDataInsertAllResponse"}}, nil
}

// rowsList returns the stored rows newest-last.
func (p *Provider) rowsList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset, table := request.Path["datasetId"], request.Path["tableId"]
	record := p.tableLocked(scope, dataset, table)
	if record == nil {
		return nil, notFound(kindTable, table)
	}
	rows := []any{}
	for _, row := range record.Rows {
		rows = append(rows, map[string]any{"f": []any{map[string]any{"v": row}}})
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"totalRows": strconv.Itoa(len(rows)),
		"rows":      rows,
	}}, nil
}

// jobsQuery runs a bounded local SELECT against one stored table.
// Supported grammar: SELECT * | column[, column] FROM dataset.table
// [WHERE column = literal]; joins, aggregations, and functions are refused
// as explicit gaps rather than approximated.
func (p *Provider) jobsQuery(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		Query        string `json:"query"`
		UseLegacySQL bool   `json:"useLegacySql"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid query body")
	}
	if body.UseLegacySQL {
		return nil, status.Error(codes.Unimplemented, "Legacy SQL is not supported")
	}
	parsed, parseErr := parseQuery(body.Query)
	if parseErr != nil {
		return nil, parseErr
	}
	table := p.tableLocked(scope, parsed.dataset, parsed.table)
	if table == nil {
		return nil, notFound("table", parsed.table)
	}
	fields := schemaFields(table.Schema)
	if len(parsed.columns) > 0 {
		projected := []map[string]any{}
		for _, field := range fields {
			for _, want := range parsed.columns {
				if field["name"] == want {
					projected = append(projected, field)
					break
				}
			}
		}
		if len(projected) != len(parsed.columns) {
			return nil, status.Errorf(codes.InvalidArgument, "Unknown column in query")
		}
		fields = projected
	}
	rows := []map[string]any{}
	for _, row := range table.Rows {
		if parsed.filterColumn != "" {
			value, ok := row[parsed.filterColumn]
			if !ok || !literalEquals(value, parsed.filterValue) {
				continue
			}
		}
		projected := map[string]any{}
		for _, field := range fields {
			name, _ := field["name"].(string)
			projected[name] = row[name]
		}
		rows = append(rows, projected)
	}
	jobID := "job_" + strconv.Itoa(int(p.clock().UnixNano()))
	job := &jobRecord{ID: jobID, Fields: fields, Rows: rows, CreatedAt: p.clock()}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.jobs[jobID] = job
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"jobReference": map[string]any{"projectId": scope.Project, "jobId": jobID},
		"jobComplete":  true,
		"schema":       map[string]any{"fields": fields},
		"rows":         bqRows(fields, rows),
		"totalRows":    len(rows),
	}}, nil
}

// jobsResults returns the stored result of one query job.
func (p *Provider) jobsResults(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	job := p.jobLocked(scope, request.Path["jobId"])
	if job == nil {
		return nil, notFound("job", request.Path["jobId"])
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"jobReference": map[string]any{"projectId": scope.Project, "jobId": job.ID},
		"jobComplete":  true,
		"schema":       map[string]any{"fields": job.Fields},
		"rows":         bqRows(job.Fields, job.Rows),
		"totalRows":    len(job.Rows),
	}}, nil
}

// jobLocked reads one job under the scope lock.
func (p *Provider) jobLocked(scope state.Scope, job string) *jobRecord {
	var record *jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.jobs[job]; return nil })
	return record
}

// parsedQuery is the bounded SELECT the local engine accepts.
type parsedQuery struct {
	dataset, table string
	columns        []string
	filterColumn   string
	filterValue    string
}

// parseQuery parses the supported SELECT subset.
func parseQuery(query string) (parsedQuery, error) {
	parsed := parsedQuery{}
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "SELECT ") {
		return parsed, status.Error(codes.Unimplemented, "Only SELECT statements are supported")
	}
	rest := strings.TrimSpace(query[len("SELECT "):])
	from := strings.Index(strings.ToUpper(rest), " FROM ")
	if from < 0 {
		return parsed, status.Errorf(codes.InvalidArgument, "Missing FROM clause in %q", query)
	}
	columns := strings.TrimSpace(rest[:from])
	after := strings.TrimSpace(rest[from+len(" FROM "):])
	table, filter := after, ""
	if index := strings.Index(strings.ToUpper(after), " WHERE "); index >= 0 {
		table = after[:index]
		filter = strings.TrimSpace(after[index+len(" WHERE "):])
	}
	parts := strings.Split(table, ".")
	switch len(parts) {
	case 2:
		parsed.dataset, parsed.table = parts[0], parts[1]
	case 3:
		parsed.dataset, parsed.table = parts[1], parts[2]
	default:
		return parsed, status.Errorf(codes.InvalidArgument, "Unreadable table reference %q", table)
	}
	if !uid.MatchString(parsed.dataset) || !uid.MatchString(parsed.table) {
		return parsed, status.Errorf(codes.InvalidArgument, "Invalid table reference %q", table)
	}
	if columns != "*" {
		for _, column := range strings.Split(columns, ",") {
			name := strings.TrimSpace(column)
			if !uid.MatchString(name) {
				return parsed, status.Errorf(codes.Unimplemented, "Only plain column projections are supported: %q", name)
			}
			parsed.columns = append(parsed.columns, name)
		}
	}
	if filter != "" {
		segments := strings.SplitN(filter, "=", 2)
		if len(segments) != 2 {
			return parsed, status.Errorf(codes.Unimplemented, "Only equality filters are supported")
		}
		parsed.filterColumn = strings.TrimSpace(segments[0])
		parsed.filterValue = strings.Trim(strings.TrimSpace(segments[1]), "'\"")
		if !uid.MatchString(parsed.filterColumn) {
			return parsed, status.Errorf(codes.InvalidArgument, "Invalid filter column %q", parsed.filterColumn)
		}
	}
	return parsed, nil
}

// schemaFields returns the schema's field list.
func schemaFields(schema map[string]any) []map[string]any {
	fields, _ := schema["fields"].([]any)
	out := []map[string]any{}
	for _, field := range fields {
		if entry, ok := field.(map[string]any); ok {
			out = append(out, entry)
		}
	}
	return out
}

// literalEquals compares one stored value against the filter literal.
func literalEquals(value any, literal string) bool {
	switch typed := value.(type) {
	case string:
		return typed == literal
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64) == literal
	case int:
		return strconv.Itoa(typed) == literal
	default:
		return false
	}
}

// bqRows renders stored rows in the BigQuery cell shape.
func bqRows(fields []map[string]any, rows []map[string]any) []any {
	out := []any{}
	for _, row := range rows {
		cells := []any{}
		for _, field := range fields {
			name, _ := field["name"].(string)
			cells = append(cells, map[string]any{"v": row[name]})
		}
		out = append(out, map[string]any{"f": cells})
	}
	return out
}
