package bigquery

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// jobIDPattern is the documented JobReference.jobId alphabet; ids are at most
// 1,024 characters.
var jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// gap refuses a request shape this provider does not model.
var gap = provider.CoverageGap("BigQuery")

// errLegacySQL refuses legacy SQL on jobs.query and jobs.insert.
var errLegacySQL = status.Error(codes.Unimplemented, "Legacy SQL is not supported")

// systemOptions are the Google API system parameters the job methods accept.
// fields selects top-level response fields (partial response).
var systemOptions = []string{"prettyPrint", keyFields, "alt"}

// jobOptions are the query parameters each job method's reference defines.
var jobOptions = map[string][]string{
	methodJobsInsert: nil,
	methodJobsQuery:  nil,
	methodJobsGet:    {keyLocation},
	methodJobsResults: {keyLocation, "maxResults", optionPageToken, optionStartIndex, "timeoutMs",
		optionInt64Timestamp, optionTimestampFormat},
}

// Result format options; they change only how TIMESTAMP cells are encoded.
const (
	optionInt64Timestamp  = "formatOptions.useInt64Timestamp"
	optionTimestampFormat = "formatOptions.timestampOutputFormat"
	// optionPageToken is the decimal index of the next row, which this
	// emulator issues as a page token.
	optionPageToken  = "pageToken"
	optionStartIndex = "startIndex"
)

// validateJobOptions refuses any query parameter the method does not define.
func validateJobOptions(method string, query url.Values) error {
	for key := range query {
		if !slices.Contains(systemOptions, key) && !slices.Contains(jobOptions[method], key) {
			return gap("query parameter " + key)
		}
	}
	if alt := query.Get("alt"); alt != "" && alt != "json" {
		return gap("alt=" + alt)
	}
	if strings.ContainsAny(query.Get(keyFields), "/()*") {
		return gap("nested fields selectors")
	}
	return nil
}

// timestampFormat reads the result format options: TIMESTAMP cells are float
// seconds unless the caller asks for integer microseconds.
func timestampFormat(query url.Values) (bool, error) {
	switch format := query.Get(optionTimestampFormat); format {
	case "", "TIMESTAMP_OUTPUT_FORMAT_UNSPECIFIED", sqlFloat64:
		return query.Get(optionInt64Timestamp) == "true", nil
	case sqlInt64:
		return true, nil
	default:
		return false, gap("formatOptions.timestampOutputFormat=" + format)
	}
}

// partial keeps only the top-level fields a fields parameter names; GCP
// honors it on getQueryResults (recorded: bigquery-jobs-insert-query).
func partial(query url.Values, document map[string]any) map[string]any {
	fields := query.Get(keyFields)
	if fields == "" {
		return document
	}
	selected := map[string]any{}
	for field := range strings.SplitSeq(fields, ",") {
		if value, ok := document[strings.TrimSpace(field)]; ok {
			selected[strings.TrimSpace(field)] = value
		}
	}
	return selected
}

// datasetReference names a job's default dataset.
type datasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

// queryOptions are the query settings jobs.query and query jobs share.
type queryOptions struct {
	Query           string            `json:"query"`
	UseLegacySQL    *bool             `json:"useLegacySql"`
	DefaultDataset  *datasetReference `json:"defaultDataset"`
	ParameterMode   string            `json:"parameterMode"`
	QueryParameters []queryParameter  `json:"queryParameters"`
	// The query cache is an optimization, and no bytes are billed locally;
	// results are the same either way.
	UseQueryCache      *bool  `json:"useQueryCache"`
	MaximumBytesBilled string `json:"maximumBytesBilled"`
}

// insertBody is the slice of the Job resource jobs.insert serves.
type insertBody struct {
	JobReference struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
		Location  string `json:"location"`
	} `json:"jobReference"`
	Configuration struct {
		JobType string            `json:"jobType"`
		DryRun  bool              `json:"dryRun"`
		Labels  map[string]string `json:"labels"`
		Query   *struct {
			queryOptions
			Priority string `json:"priority"`
		} `json:"query"`
	} `json:"configuration"`
}

// decodeStrict reads a request body, refusing unknown fields as coverage
// gaps rather than ignoring what they would change.
func decodeStrict(body []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return gap("job field " + strings.Trim(field, `"`))
		}
		return provider.Invalid("Invalid job body")
	}
	return nil
}

// decodeInsert reads a jobs.insert body. Only Standard SQL query jobs are
// served; load, extract, and copy jobs, destination tables, and every other
// Job field are coverage gaps.
func decodeInsert(body []byte, project string) (insertBody, error) {
	var job insertBody
	if err := decodeStrict(body, &job); err != nil {
		return job, err
	}
	reference := job.JobReference
	if reference.ProjectID != "" && reference.ProjectID != project {
		return job, provider.Invalid("Job project disagrees with the request path")
	}
	if reference.JobID != "" && (len(reference.JobID) > 1024 || !jobIDPattern.MatchString(reference.JobID)) {
		return job, provider.Invalid("Invalid job id")
	}
	return job, validateQueryJob(job)
}

// validateQueryJob refuses every job but an interactive Standard SQL query.
func validateQueryJob(job insertBody) error {
	query := job.Configuration.Query
	switch {
	case query == nil || (job.Configuration.JobType != "" && job.Configuration.JobType != "QUERY"):
		return gap("only query jobs are supported")
	case query.UseLegacySQL == nil || *query.UseLegacySQL:
		// jobs.insert defaults configuration.query.useLegacySql to true.
		return errLegacySQL
	case query.Priority != "" && query.Priority != "INTERACTIVE":
		return gap("query priority " + query.Priority)
	}
	return nil
}

// jobsInsert runs one query job to completion and stores its result. A dry
// run is validated and authorized like a real job but not stored.
func (p *Provider) jobsInsert(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := validateJobOptions(methodJobsInsert, request.Query); err != nil {
		return nil, err
	}
	body, err := decodeInsert(request.JSON, scope.Project)
	if err != nil {
		return nil, err
	}
	id := cmp.Or(body.JobReference.JobID, "job_"+strconv.FormatInt(p.clock().UnixNano(), 10))
	if !body.Configuration.DryRun && p.jobCopy(scope, id) != nil {
		return nil, status.Errorf(codes.AlreadyExists, "Already Exists: Job %s:%s.%s", scope.Project, cmp.Or(body.JobReference.Location, "US"), id)
	}
	job, denied, err := p.runQuery(call, scope, body.Configuration.Query.queryOptions, body.JobReference.Location, body.Configuration.DryRun)
	if err != nil || denied != nil {
		return denied, err
	}
	job.ID = id
	job.Labels = body.Configuration.Labels
	if body.Configuration.DryRun {
		return &gateway.RESTResponse{JSON: partial(request.Query, jobWire(scope.Project, job, true))}, nil
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if s.jobs[job.ID] != nil {
			return status.Errorf(codes.AlreadyExists, "Already Exists: Job %s:%s.%s", scope.Project, job.Location, job.ID)
		}
		s.jobs[job.ID] = job
		return nil
	}); err != nil {
		return nil, fmt.Errorf("store the query job: %w", err)
	}
	return &gateway.RESTResponse{JSON: partial(request.Query, jobWire(scope.Project, job, false))}, nil
}

// queryRequest is the jobs.query body.
type queryRequest struct {
	queryOptions
	Kind          string            `json:"kind"`
	DryRun        bool              `json:"dryRun"`
	Location      string            `json:"location"`
	TimeoutMs     json.Number       `json:"timeoutMs"`
	MaxResults    json.Number       `json:"maxResults"`
	Labels        map[string]string `json:"labels"`
	RequestID     string            `json:"requestId"`
	FormatOptions struct {
		UseInt64Timestamp     bool   `json:"useInt64Timestamp"`
		TimestampOutputFormat string `json:"timestampOutputFormat"`
	} `json:"formatOptions"`
	JobCreationMode string `json:"jobCreationMode"`
}

// decodeQuery reads and validates a jobs.query request.
func decodeQuery(request *gateway.RESTRequest) (queryRequest, error) {
	var body queryRequest
	if err := validateJobOptions(methodJobsQuery, request.Query); err != nil {
		return body, err
	}
	if err := decodeStrict(request.JSON, &body); err != nil {
		return body, err
	}
	if body.UseLegacySQL != nil && *body.UseLegacySQL {
		// jobs.query defaults useLegacySql to true, but the client libraries
		// always send false; an absent value is served as Standard SQL.
		return body, errLegacySQL
	}
	return body, nil
}

// resultOptions reads the body's result paging and format options in their
// getQueryResults query-parameter form.
func (body queryRequest) resultOptions() url.Values {
	format := url.Values{optionInt64Timestamp: {strconv.FormatBool(body.FormatOptions.UseInt64Timestamp)}}
	if body.FormatOptions.TimestampOutputFormat != "" {
		format.Set(optionTimestampFormat, body.FormatOptions.TimestampOutputFormat)
	}
	if body.MaxResults != "" {
		format.Set("maxResults", body.MaxResults.String())
	}
	return format
}

// jobsQuery runs a GoogleSQL script and returns its first result page.
func (p *Provider) jobsQuery(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeQuery(request)
	if err != nil {
		return nil, err
	}
	format := body.resultOptions()
	int64Timestamp, err := timestampFormat(format)
	if err != nil {
		return nil, err
	}
	job, denied, err := p.runQuery(call, scope, body.queryOptions, body.Location, body.DryRun)
	if err != nil || denied != nil {
		return denied, err
	}
	job.ID = "job_" + strconv.FormatInt(job.CreatedAt.UnixNano(), 10)
	job.Labels = body.Labels
	if body.DryRun {
		return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, true, 0, 0, false)}, nil
	}
	start, end, err := resultPage(format, len(job.Rows))
	if err != nil {
		return nil, err
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.jobs[job.ID] = job
		return nil
	}); err != nil {
		return nil, fmt.Errorf("store query job: %w", err)
	}
	return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, false, start, end, int64Timestamp)}, nil
}

// runQuery plans a script, authorizes every table it names, runs it, and
// returns the unstored job. A denied check returns its 403; nothing runs
// before every check passes. location, when set, must be the location of
// every dataset the script names.
func (p *Provider) runQuery(call gateway.Call, scope state.Scope, options queryOptions, location string, dryRun bool) (*jobRecord, *gateway.RESTResponse, error) {
	defaultDataset, err := options.defaultDataset(scope.Project)
	if err != nil {
		return nil, nil, err
	}
	catalog := p.catalogOf(scope)
	plan, err := planQuery(catalog, options.Query, defaultDataset, options.parameterMode(), options.QueryParameters)
	if err != nil {
		return nil, nil, err
	}
	jobLocation, err := p.jobLocation(scope, catalog, plan, location)
	if err != nil {
		return nil, nil, err
	}
	for _, check := range plan.checks {
		if denied, err := p.authorizeCheck(call, scope.Project, check); denied != nil || err != nil {
			return nil, denied, err
		}
	}
	now := p.clock()
	job := &jobRecord{
		StatementType: plan.statementType, Location: jobLocation, CreatedAt: now,
		Principal: call.Principal.Subject, Query: options.Query,
	}
	if dryRun {
		return job, nil, p.dryRunSchema(call.Context, scope, plan, job)
	}
	if err := p.runPlan(call.Context, scope, plan, job); err != nil {
		return nil, nil, err
	}
	return job, nil, nil
}

// defaultDataset reads the default dataset, which must be in the job's
// project.
func (options queryOptions) defaultDataset(project string) (string, error) {
	reference := options.DefaultDataset
	if reference == nil {
		return "", nil
	}
	if reference.ProjectID != "" && !strings.EqualFold(reference.ProjectID, project) {
		return "", gap("cross-project table references (default dataset " + reference.ProjectID + "." + reference.DatasetID + ")")
	}
	return reference.DatasetID, nil
}

// parameterMode is the declared mode, or POSITIONAL when the parameters
// have no names.
func (options queryOptions) parameterMode() string {
	mode := strings.ToUpper(options.ParameterMode)
	if mode == "" && len(options.QueryParameters) > 0 && options.QueryParameters[0].Name == "" {
		return modePositional
	}
	return mode
}

// dryRunSchema reports a dry-run SELECT's result columns by running it with
// LIMIT 0 after authorization; a SELECT changes nothing. Other statements
// report no schema.
func (p *Provider) dryRunSchema(ctx context.Context, scope state.Scope, plan *queryPlan, job *jobRecord) error {
	if plan.statementType != "SELECT" {
		return nil
	}
	query := strings.TrimRight(strings.TrimSpace(plan.script), ";")
	return p.useEngine(ctx, scope, func(ctx context.Context, engine *engineBackend) error {
		result, err := engine.query(ctx, "SELECT * FROM ("+query+"\n) LIMIT 0", plan.args) //nolint:unqueryvet // a dry run reports every result column
		if err != nil {
			return err
		}
		job.Columns = result.columns
		return nil
	})
}

// jobLocation is the job's location: the requested one, which every named
// dataset must be in, or the first named dataset's, or US.
func (p *Provider) jobLocation(scope state.Scope, catalog *catalog, plan *queryPlan, requested string) (string, error) {
	location := requested
	for _, check := range plan.checks {
		dataset := datasetOfResource(check.resource)
		record := p.datasetCopy(scope, catalog.datasets[strings.ToLower(dataset)])
		if record == nil {
			continue
		}
		if location == "" {
			location = record.Location
		}
		if !strings.EqualFold(location, record.Location) {
			return "", status.Errorf(codes.NotFound, "Not found: Dataset %s:%s was not found in location %s", scope.Project, dataset, location)
		}
	}
	return cmp.Or(location, "US"), nil
}

// datasetOfResource reads the dataset id from a dataset or table resource.
func datasetOfResource(resource string) string {
	_, rest, _ := strings.Cut(resource, "/datasets/")
	dataset, _, _ := strings.Cut(rest, "/")
	return dataset
}

// runPlan runs a planned script in the engine. A single DML statement
// reports the rows it changed; anything else returns its last result. Tables
// the script created or dropped are reconciled with the engine even when a
// later statement fails, since earlier statements already ran.
func (p *Provider) runPlan(ctx context.Context, scope state.Scope, plan *queryPlan, job *jobRecord) error {
	return p.useEngine(ctx, scope, func(ctx context.Context, engine *engineBackend) error {
		result, runErr := runStatements(ctx, engine, plan)
		reconcileErr := p.reconcile(ctx, engine, scope, plan.ddl)
		if runErr != nil {
			return runErr
		}
		if reconcileErr != nil {
			return reconcileErr
		}
		job.Columns, job.Rows, job.Affected = result.columns, result.rows, result.affected
		if target := plan.target; target != nil {
			if record := p.tableCopy(scope, target.dataset, target.table); record != nil {
				job.TargetSchema = record.Schema
			}
		}
		return nil
	})
}

// runStatements runs the script: a single DML statement through exec, so
// the engine reports the rows it changed (not for MERGE, whose count the
// engine does not report), and anything else through query.
func runStatements(ctx context.Context, engine *engineBackend, plan *queryPlan) (*engineResult, error) {
	switch plan.statementType {
	case statementInsert, statementUpdate, statementDelete:
		return engine.exec(ctx, plan.script, plan.args)
	case statementMerge:
		result, err := engine.exec(ctx, plan.script, plan.args)
		if result != nil {
			result.affected = nil
		}
		return result, err
	default:
		return engine.query(ctx, plan.script, plan.args)
	}
}

// reconcile reads each CREATE or DROP target back from the engine and makes
// the table metadata agree: a new table gets its engine schema, a replaced
// one its new schema, and a dropped one loses its metadata. The caller holds
// the engine gate, so no dataset changes meanwhile.
func (p *Provider) reconcile(ctx context.Context, engine *engineBackend, scope state.Scope, targets []ddlTarget) error {
	for _, target := range targets {
		columns, exists, err := engine.columnsOf(ctx, target.name.dataset, target.name.table)
		if err != nil {
			return err
		}
		if err := p.scopes.With(scope, func(s *scopeState) error {
			return p.applyTarget(s, scope, target, exists, columns)
		}); err != nil {
			return fmt.Errorf("reconcile %s.%s: %w", target.name.dataset, target.name.table, err)
		}
	}
	return nil
}

// applyTarget updates one table's metadata and index entry from the
// engine's view of it. The caller holds the scope lock.
func (p *Provider) applyTarget(s *scopeState, scope state.Scope, target ddlTarget, exists bool, columns []engineColumn) error {
	key := tableKey(target.name.dataset, target.name.table)
	record, owner := s.tables[key], s.datasets[target.name.dataset]
	switch {
	case !exists && record != nil:
		delete(s.tables, key)
		return p.unindexTables(scope, target.name.dataset, []string{target.name.table})
	case !exists || owner == nil:
		return nil
	case record == nil:
		if err := p.indexTable(scope, target.name.dataset, target.name.table); err != nil {
			return err
		}
		record = &tableRecord{Dataset: target.name.dataset, ID: target.name.table, Location: owner.Location, CreatedAt: p.clock()}
		s.tables[key] = record
	case !target.replaces(record):
		return nil
	}
	fields := make([]any, 0, len(columns))
	for _, column := range columns {
		fields = append(fields, tableField(column.name, column.kind))
	}
	record.Schema = map[string]any{keyFields: fields}
	record.Type, record.ViewQuery, record.ViewRefs = typeTable, "", nil
	if target.view {
		record.Type, record.ViewQuery, record.ViewRefs = typeView, target.viewQuery, slices.Clone(target.viewRefs)
	}
	return nil
}

// replaces reports whether a CREATE target redefines an existing table: an
// OR REPLACE statement, as opposed to IF NOT EXISTS or a DROP.
func (t ddlTarget) replaces(record *tableRecord) bool {
	return t.create && t.replace && record != nil
}

// catalogOf copies the scope's datasets and tables for planning.
func (p *Provider) catalogOf(scope state.Scope) *catalog {
	c := &catalog{project: scope.Project, datasets: map[string]string{}, tables: map[string]*tableRecord{}}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for id := range s.datasets {
			c.datasets[strings.ToLower(id)] = id
		}
		for _, record := range s.tables {
			copied := *record
			copied.ViewRefs = slices.Clone(record.ViewRefs)
			c.tables[foldKey(record.Dataset, record.ID)] = &copied
		}
		return nil
	})
	return c
}

// authorizeCheck runs one of a query's table checks.
func (p *Provider) authorizeCheck(call gateway.Call, project string, check tableCheck) (*gateway.RESTResponse, error) {
	related := call
	related.Method = check.method
	related.Resource = check.resource
	decision, resource, err := p.authorizeAt(related, check.resource, project)
	if err == nil {
		return nil, nil //nolint:nilnil // a nil response with a nil error means the check passed
	}
	if provider.Unverified(err) {
		return nil, fmt.Errorf("authorize the query: %w", err)
	}
	denied := provider.Denied(decision, resource)
	denied.Headers = http.Header{headerDecision: {decision.ID}}
	return denied, nil
}

// ownJobCheck names the permission plan that authorizes a job read: a stored
// job's creator reads it under checkOwnJob instead of bigquery.jobs.get, and
// so does a read of a job that does not exist, which GCP answers 404 to a job
// user without bigquery.jobs.get (recorded: bigquery-jobs-insert-query).
func (p *Provider) ownJobCheck(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) string {
	if call.Method != methodJobsGet && call.Method != methodJobsResults {
		return call.Method
	}
	job := p.jobCopy(scope, request.Path["jobId"])
	if job == nil || (job.Principal != "" && job.Principal == call.Principal.Subject) {
		return checkOwnJob
	}
	return call.Method
}

// jobsGet returns one stored job resource.
func (p *Provider) jobsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	job, err := p.requestedJob(methodJobsGet, request, scope)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: partial(request.Query, jobWire(scope.Project, job, false))}, nil
}

// jobsResults returns one page of a stored query job's result.
func (p *Provider) jobsResults(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	job, err := p.requestedJob(methodJobsResults, request, scope)
	if err != nil {
		return nil, err
	}
	int64Timestamp, err := timestampFormat(request.Query)
	if err != nil {
		return nil, err
	}
	start, end, err := resultPage(request.Query, len(job.Rows))
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: partial(request.Query, resultsWire(scope.Project, job, start, end, int64Timestamp))}, nil
}

// requestedJob validates a job read's options and returns the stored job; a
// job in another location than the one requested is not found.
func (p *Provider) requestedJob(method string, request *gateway.RESTRequest, scope state.Scope) (*jobRecord, error) {
	if err := validateJobOptions(method, request.Query); err != nil {
		return nil, err
	}
	id := request.Path["jobId"]
	job := p.jobCopy(scope, id)
	if location := request.Query.Get(keyLocation); job == nil || (location != "" && !strings.EqualFold(location, job.Location)) {
		return nil, provider.NotFound("job", id)
	}
	return job, nil
}

// jobCopy returns a copy of one job taken under the scope lock, or nil. A
// stored job never changes, so its rows and columns are shared.
func (p *Provider) jobCopy(scope state.Scope, job string) *jobRecord {
	var copied *jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.jobs[job]; record != nil {
			clone := *record
			clone.Labels = maps.Clone(record.Labels)
			copied = &clone
		}
		return nil
	})
	return copied
}

// resultPage reads the requested row window. startIndex and pageToken (the
// decimal index of the next row, which this emulator issues) set the first
// row; maxResults bounds the page, and 0 returns no rows.
func resultPage(query url.Values, total int) (int, int, error) {
	start := 0
	for _, key := range []string{optionStartIndex, optionPageToken} {
		if value := query.Get(key); value != "" {
			index, err := strconv.Atoi(value)
			if err != nil || index < 0 {
				return 0, 0, provider.Invalid("Invalid " + key)
			}
			start = index
		}
	}
	start = min(start, total)
	end := total
	if value := query.Get("maxResults"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 0 {
			return 0, 0, provider.Invalid("Invalid maxResults")
		}
		end = min(start+limit, total)
	}
	return start, end, nil
}

// Wire keys repeated across the dataset, table, and query documents.
const (
	keyKind         = "kind"
	keyETag         = "etag"
	keyLocation     = "location"
	keyCreationTime = "creationTime"
	keyProjectID    = "projectId"
	keySelfLink     = "selfLink"
	keySchema       = "schema"
	keyFields       = "fields"
	keyJobReference = "jobReference"
	keyCacheHit     = "cacheHit"
	keyQuery        = "query"
	// keyBytesProcessed is always "0": no bytes are billed locally.
	keyBytesProcessed = "totalBytesProcessed"
)

// permJobsCreate runs jobs; the creator check reuses it.
const permJobsCreate = "bigquery.jobs.create"

// selfLink prefixes the API links GCP returns on datasets and tables.
const selfLink = "https://bigquery.googleapis.com/bigquery/v2/projects/"

// etagOf renders a stable revision tag for one stored resource.
func etagOf(moment time.Time) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(moment.UnixNano(), 36)))
}

// defaultAccess is the access list GCP attaches to a new dataset. Only the
// field's presence is recorded; the entries are the documented defaults.
func defaultAccess() []any {
	entry := func(role, group string) any { return map[string]any{"role": role, "specialGroup": group} }
	return []any{entry("WRITER", "projectWriters"), entry("OWNER", "projectOwners"), entry("READER", "projectReaders")}
}

// queryResponse renders a completed jobs.query result page; a dry run
// reports the plan without rows or execution timing. A statement without a
// result (DML, DDL) has no schema or rows; DML reports numDmlAffectedRows.
func queryResponse(project string, job *jobRecord, dryRun bool, start, end int, int64Timestamp bool) map[string]any {
	created := strconv.FormatInt(job.CreatedAt.UnixMilli(), 10)
	response := map[string]any{
		keyKind:            "bigquery#queryResponse",
		keyJobReference:    jobReference(project, job),
		"jobComplete":      true,
		keyLocation:        cmp.Or(job.Location, "US"),
		"statementType":    job.StatementType,
		keyCreationTime:    created,
		"endTime":          created,
		keyBytesProcessed:  "0",
		"totalBytesBilled": "0",
	}
	// A script reports no cacheHit (recorded: bigquery-sql-engine).
	if job.StatementType != statementScript {
		response[keyCacheHit] = false
	}
	addSchema(response, job)
	if dryRun {
		return response
	}
	addAffected(response, job)
	// A statement without a result, DML or DDL, has no rows or totalRows.
	if len(job.Columns) > 0 {
		response["rows"] = cellRows(job.Columns, job.Rows[start:end], int64Timestamp)
		response["pageRowCount"] = strconv.Itoa(end - start)
		response["totalRows"] = strconv.Itoa(len(job.Rows))
		if end < len(job.Rows) {
			response[optionPageToken] = strconv.Itoa(end)
		}
	}
	response["queryId"] = job.ID
	response["startTime"] = created
	response["totalSlotMs"] = "0"
	response["jobCreationReason"] = map[string]any{"code": "REQUESTED"}
	return response
}

// addSchema sets a result's schema, or for DML and CREATE statements the
// schema of the table they wrote.
func addSchema(document map[string]any, job *jobRecord) {
	switch {
	case len(job.Columns) > 0:
		document[keySchema] = map[string]any{keyFields: resultFields(job.Columns)}
	case job.TargetSchema != nil:
		document[keySchema] = job.TargetSchema
	}
}

// dmlCounts name each DML statement's dmlStats counter.
var dmlCounts = map[string]string{statementInsert: "insertedRowCount", statementUpdate: "updatedRowCount", statementDelete: "deletedRowCount"}

// addAffected reports a DML statement's changed-row count when known, and
// the matching dmlStats counter.
func addAffected(document map[string]any, job *jobRecord) {
	if job.Affected == nil {
		return
	}
	count := strconv.FormatInt(*job.Affected, 10)
	document["numDmlAffectedRows"] = count
	if counter, ok := dmlCounts[job.StatementType]; ok {
		document["dmlStats"] = map[string]any{counter: count}
	}
}

// jobReference renders a job's reference; a job's location defaults to US.
func jobReference(project string, job *jobRecord) map[string]any {
	return map[string]any{keyProjectID: project, "jobId": job.ID, keyLocation: cmp.Or(job.Location, "US")}
}

// jobWire renders a completed query job resource (jobs.insert, jobs.get).
// No anonymous destination table is modeled, so configuration.query carries
// no destinationTable; a dry run has no start or end time.
func jobWire(project string, job *jobRecord, dryRun bool) map[string]any {
	location := cmp.Or(job.Location, "US")
	created := strconv.FormatInt(job.CreatedAt.UnixMilli(), 10)
	configuration := map[string]any{
		"jobType": "QUERY",
		keyQuery:  map[string]any{keyQuery: job.Query, "useLegacySql": false, "priority": "INTERACTIVE"},
	}
	if dryRun {
		configuration["dryRun"] = true
	}
	if len(job.Labels) > 0 {
		configuration["labels"] = job.Labels
	}
	queryStatistics := map[string]any{keyBytesProcessed: "0", "totalBytesBilled": "0", keyCacheHit: false, "statementType": job.StatementType}
	addAffected(queryStatistics, job)
	if dryRun && len(job.Columns) > 0 {
		queryStatistics[keySchema] = map[string]any{keyFields: resultFields(job.Columns)}
	}
	statistics := map[string]any{keyCreationTime: created, keyBytesProcessed: "0", keyQuery: queryStatistics}
	if !dryRun {
		statistics["startTime"] = created
		statistics["endTime"] = created
	}
	document := map[string]any{
		keyKind:         "bigquery#job",
		keyETag:         etagOf(job.CreatedAt),
		"id":            project + ":" + location + "." + job.ID,
		keySelfLink:     selfLink + project + "/jobs/" + job.ID + "?location=" + location,
		keyJobReference: jobReference(project, job),
		"configuration": configuration,
		"statistics":    statistics,
		"status":        map[string]any{"state": "DONE"},
	}
	if _, email, ok := strings.Cut(job.Principal, ":"); ok {
		document["user_email"] = email
		document["principal_subject"] = job.Principal
	}
	document["jobCreationReason"] = map[string]any{"code": "REQUESTED"}
	return document
}

// resultsWire renders one jobs.getQueryResults page: rows start through end,
// and a pageToken while rows remain, even after maxResults=0 (recorded:
// bigquery-jobs-insert-query).
func resultsWire(project string, job *jobRecord, start, end int, int64Timestamp bool) map[string]any {
	response := map[string]any{
		keyKind:           "bigquery#getQueryResultsResponse",
		keyETag:           etagOf(job.CreatedAt),
		keyJobReference:   jobReference(project, job),
		"totalRows":       strconv.Itoa(len(job.Rows)),
		"jobComplete":     true,
		keyCacheHit:       false,
		keyBytesProcessed: "0",
	}
	addAffected(response, job)
	if len(job.Columns) > 0 {
		response[keySchema] = map[string]any{keyFields: resultFields(job.Columns)}
	}
	if end > start {
		response["rows"] = cellRows(job.Columns, job.Rows[start:end], int64Timestamp)
	}
	if end < len(job.Rows) {
		response[optionPageToken] = strconv.Itoa(end)
	}
	return response
}
