package bigquery

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
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
	methodJobsList: {"allUsers", "maxCreationTime", "minCreationTime", optionMaxResults, optionPageToken, "projection",
		"stateFilter", "parentJobId"},
	methodJobsResults: {keyLocation, optionMaxResults, optionPageToken, optionStartIndex, "timeoutMs",
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
		return flag(query, optionInt64Timestamp), nil
	case sqlInt64:
		return true, nil
	default:
		return false, gap("formatOptions.timestampOutputFormat=" + format)
	}
}

// flag reads a boolean query parameter. The Python client sends Python's
// spelling (formatOptions.useInt64Timestamp=True, deleteContents=True),
// which GCP accepts, so case is ignored.
func flag(query url.Values, name string) bool {
	return strings.EqualFold(query.Get(name), textTrue)
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
	// The query cache is an optimization; results are the same either way.
	UseQueryCache *bool `json:"useQueryCache"`
}

// unmodeledQuery lists the JobConfigurationQuery fields the provider doesn't
// model (BigQuery v2 discovery); jobs.insert refuses each under
// configuration.query. maximumBytesBilled is one: queries bill no bytes
// locally, so a cap GCP would enforce could never be.
var unmodeledQuery = []string{
	"continuous", "createDisposition", "maximumBillingTier", "clustering", "preserveNulls", "writeDisposition",
	"writeIncrementalResults", "scriptOptions", "allowLargeResults", "connectionProperties", "destinationTable",
	"maximumBytesBilled", "flattenResults", "createSession", "timePartitioning", "destinationEncryptionConfiguration",
	"userDefinedFunctionResources", "rangePartitioning", "tableDefinitions", "schemaUpdateOptions", "secureContext",
}

// jobFields are the Job fields jobs.insert doesn't model: load, extract, and
// copy jobs, job limits, and the query fields above. GCP ignores names its
// schema lacks, and so does the provider.
var jobFields = func() provider.Fields {
	fields := provider.Fields{
		Unmodeled: []string{
			"configuration.load", "configuration.extract", "configuration.copy",
			"configuration.maxSlots", "configuration.reservation",
		},
		OutputOnly: []string{
			"principal_subject", "id", keyStatistics, keySelfLink, keyKind, keyETag, keyStatus, "jobCreationReason",
			"user_email", "configuration.query.systemVariables",
		},
		IgnoreUnknown: true,
	}
	for _, name := range unmodeledQuery {
		fields.Unmodeled = append(fields.Unmodeled, "configuration.query."+name)
	}
	return fields
}()

// queryFields are the QueryRequest fields jobs.query doesn't model (BigQuery
// v2 discovery).
var queryFields = provider.Fields{
	Unmodeled: []string{
		"secureContext", "createSession", "connectionProperties", "reservation", "arrowSerializationOptions",
		"writeIncrementalResults", "destinationEncryptionConfiguration", "maxSlots", "preserveNulls",
		"queryResultsFormat", "continuous", "maximumBytesBilled",
	},
	IgnoreUnknown: true,
}

// insertBody is the slice of the Job resource jobs.insert serves.
type insertBody struct {
	JobReference struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
		Location  string `json:"location"`
	} `json:"jobReference"`
	Configuration struct {
		JobType      string            `json:"jobType"`
		DryRun       *bool             `json:"dryRun"`
		Labels       map[string]string `json:"labels"`
		JobTimeoutMs json.Number       `json:"jobTimeoutMs"`
		Query        *struct {
			queryOptions
			Priority string `json:"priority"`
		} `json:"query"`
	} `json:"configuration"`
}

// decodeInsert reads a jobs.insert body. Only Standard SQL query jobs are
// served; load, extract, and copy jobs, destination tables, and every other
// Job field are coverage gaps.
func decodeInsert(body []byte, project string) (insertBody, error) {
	var job insertBody
	if err := provider.DecodeJSON(body, "", &job, gap, jobFields); err != nil {
		return job, err
	}
	reference := job.JobReference
	if reference.ProjectID != "" && reference.ProjectID != project {
		return job, provider.Invalid("Job project disagrees with the request path")
	}
	if reference.JobID != "" && (len(reference.JobID) > 1024 || !jobIDPattern.MatchString(reference.JobID)) {
		return job, provider.Invalid("Invalid job id")
	}
	if err := validateTimeout(job.Configuration.JobTimeoutMs); err != nil {
		return job, err
	}
	return job, validateQueryJob(job)
}

// validateTimeout reads jobTimeoutMs, which dbt-bigquery sets on every job
// (recorded: bigquery-dbt-requests, echoed by jobs.insert and jobs.get). A
// local query finishes inside the call that runs it, so a positive limit is
// never reached; a value that isn't a positive int64 is a gap.
func validateTimeout(limit json.Number) error {
	if limit == "" {
		return nil
	}
	if value, err := strconv.ParseInt(limit.String(), 10, 64); err != nil || value <= 0 {
		return gap("jobTimeoutMs " + limit.String())
	}
	return nil
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
	id := cmp.Or(body.JobReference.JobID, newJobID())
	dryRun := body.Configuration.DryRun != nil && *body.Configuration.DryRun
	if !dryRun && p.jobCopy(scope, id) != nil {
		return nil, status.Errorf(codes.AlreadyExists, "Already Exists: Job %s:%s.%s", scope.Project, cmp.Or(body.JobReference.Location, "US"), id)
	}
	job, err := p.runQuery(call, scope, body.Configuration.Query.queryOptions, body.JobReference.Location, dryRun)
	if err != nil {
		return nil, err
	}
	job.ID = id
	job.Labels = body.Configuration.Labels
	job.JobTimeoutMs = body.Configuration.JobTimeoutMs.String()
	// GCP echoes a dryRun the job sent, false too (recorded: bigquery-dbt-requests).
	job.DryRunSent = body.Configuration.DryRun != nil
	if dryRun {
		return &gateway.RESTResponse{JSON: partial(request.Query, jobWire(scope.Project, job, true))}, nil
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.expireJobs(p.clock())
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

// newJobID draws a random id for a job that names none: "job_", as GCP's
// assigned ids start, and 28 URL-safe base64 characters (not recorded). Its 168
// random bits keep jobs created in the same clock tick from colliding.
func newJobID() string {
	raw := make([]byte, 21)
	_, _ = rand.Read(raw) // crypto/rand.Read never returns an error.
	return "job_" + base64.RawURLEncoding.EncodeToString(raw)
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
	JobCreationMode string      `json:"jobCreationMode"`
	JobTimeoutMs    json.Number `json:"jobTimeoutMs"`
}

// decodeQuery reads and validates a jobs.query request.
func decodeQuery(request *gateway.RESTRequest) (queryRequest, error) {
	var body queryRequest
	if err := validateJobOptions(methodJobsQuery, request.Query); err != nil {
		return body, err
	}
	if err := provider.DecodeJSON(request.JSON, "", &body, gap, queryFields); err != nil {
		return body, err
	}
	if err := validateTimeout(body.JobTimeoutMs); err != nil {
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
		format.Set(optionMaxResults, body.MaxResults.String())
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
	job := p.requestedBefore(scope, call.Principal.Subject, body.RequestID)
	if job != nil && !body.DryRun {
		start, end, err := resultPage(format, len(job.Rows))
		if err != nil {
			return nil, err
		}
		return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, false, start, end, int64Timestamp)}, nil
	}
	job, err = p.runQuery(call, scope, body.queryOptions, body.Location, body.DryRun)
	if err != nil {
		return nil, err
	}
	job.ID = newJobID()
	job.Labels = body.Labels
	job.RequestID = body.RequestID
	if body.DryRun {
		return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, true, 0, 0, false)}, nil
	}
	start, end, err := resultPage(format, len(job.Rows))
	if err != nil {
		return nil, err
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.expireJobs(p.clock())
		stored := *job // The response below reads job outside the lock.
		s.jobs[job.ID] = &stored
		return nil
	}); err != nil {
		return nil, fmt.Errorf("store query job: %w", err)
	}
	return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, false, start, end, int64Timestamp)}, nil
}

// requestWindow is how long a jobs.query requestId deduplicates retries.
// GCP documents only "a few minutes" of best-effort idempotency.
const requestWindow = 15 * time.Minute

// requestedBefore returns a copy of the caller's job that a jobs.query with
// the same requestId ran within requestWindow, or nil: a retry gets that
// job's result instead of running the query again.
func (p *Provider) requestedBefore(scope state.Scope, principal, requestID string) *jobRecord {
	if requestID == "" {
		return nil
	}
	var found *jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, job := range s.jobs {
			if job.RequestID == requestID && job.Principal == principal && p.clock().Sub(job.CreatedAt) < requestWindow {
				clone := *job
				found = &clone
			}
		}
		return nil
	})
	return found
}

// runQuery plans a script, authorizes every table it names, runs it, and
// returns the unstored job. A denied check returns its 403; nothing runs
// before every check passes. location, when set, must be the location of
// every dataset the script names. GoogleSQL's refusals are invalidQuery.
func (p *Provider) runQuery(call gateway.Call, scope state.Scope, options queryOptions, location string, dryRun bool) (*jobRecord, error) {
	job, err := p.planAndRun(call, scope, options, location, dryRun)
	return job, queryFailure(err)
}

func (p *Provider) planAndRun(call gateway.Call, scope state.Scope, options queryOptions, location string, dryRun bool) (*jobRecord, error) {
	defaultDataset, err := options.defaultDataset(scope.Project)
	if err != nil {
		return nil, err
	}
	catalog := p.catalogOf(scope)
	plan, err := planQuery(catalog, options.Query, defaultDataset, options.parameterMode(), options.QueryParameters, p.clock())
	if err != nil {
		return nil, err
	}
	jobLocation, err := p.jobLocation(scope, catalog, plan, location)
	if err != nil {
		return nil, err
	}
	for _, check := range plan.checks {
		if err := p.authorizeCheck(call, scope.Project, check); err != nil {
			return nil, err
		}
	}
	now := p.clock()
	job := &jobRecord{
		StatementType: plan.statementType, Location: jobLocation, CreatedAt: now,
		Principal: call.Principal.Subject, Query: options.Query,
	}
	if dryRun {
		return job, p.dryRunSchema(call.Context, scope, plan, job)
	}
	if plan.schema != nil {
		return job, p.createSchema(call, scope, plan.schema, job)
	}
	if err := p.runPlan(call.Context, scope, plan, job); err != nil {
		return nil, missingTable(err, scope.Project, jobLocation)
	}
	job.describe(plan)
	return job, nil
}

// createSchema runs CREATE SCHEMA: a new dataset in the job's location with
// the creator as owner, or, with IF NOT EXISTS, nothing when it exists. The
// engine keeps no datasets, so the statement never reaches it. A dataset
// created this way reports its time-travel window (recorded:
// bigquery-dbt-requests).
func (p *Provider) createSchema(call gateway.Call, scope state.Scope, schema *schemaTarget, job *jobRecord) error {
	now := p.clock()
	job.DDLOperation, job.DDLDataset = ddlCreate, schema.dataset
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback returns a wire status
		for id := range s.datasets {
			if strings.EqualFold(id, schema.dataset) && schema.ifNotExists {
				job.DDLOperation, job.DDLDataset = ddlSkip, id
				return nil
			}
		}
		created := &datasetRecord{ID: schema.dataset, Location: job.Location, CreatedAt: now, ModifiedAt: now, Patched: true}
		return p.insertDataset(s, scope, call.Principal.Subject, nil, created)
	})
}

// describe records what a finished job's statement did, as jobs.insert and
// jobs.get report it (recorded: bigquery-dbt-requests): a single DDL
// statement's operation and target; the destination table of a CREATE
// TABLE AS SELECT or MERGE, which is its target. A SELECT's destination is
// its anonymous result table (resultTable). Other statements' destinations
// are not recorded and not reported.
func (job *jobRecord) describe(plan *queryPlan) {
	if len(plan.ddl) == 1 && plan.statementType != statementScript {
		target := plan.ddl[0]
		job.DDLOperation, job.DDLTable = target.operation(), &target.name
	}
	if plan.target != nil && (plan.statementType == statementMerge || plan.statementType == statementCTAS) {
		target := *plan.target
		job.Destination = &target
	}
}

// destination is the table a job wrote: its target, or a SELECT's anonymous
// result table; nil for a dry run, which writes nothing.
func (job *jobRecord) destination(dryRun bool) *tableName {
	if job.StatementType == statementSelect && !dryRun {
		return anonymousTable(job.Principal, job.ID)
	}
	return job.Destination
}

// anonymousTable names a SELECT's result table: a hidden dataset per user
// ("_" and a hash) and a table per job ("anon" and a hash), as GCP names
// them (recorded: bigquery-dbt-requests; the hashes are local).
func anonymousTable(principal, job string) *tableName {
	user, table := sha256.Sum256([]byte(principal)), sha256.Sum256([]byte(job))
	return &tableName{dataset: "_" + hex.EncodeToString(user[:20]), table: "anon" + hex.EncodeToString(table[:20])}
}

// missingTable spells the engine's refusal of a table that doesn't exist as
// GCP's query error: "Not found: Table P:D.T was not found in location US"
// (recorded). Other errors pass.
func missingTable(err error, project, location string) error {
	name, ok := strings.CutPrefix(status.Convert(err).Message(), "Not found: Table ")
	if status.Code(err) != codes.NotFound || !ok {
		return err
	}
	parts := strings.Split(strings.ReplaceAll(name, "`", ""), ".")
	if len(parts) == 2 {
		parts = append([]string{project}, parts...)
	}
	if len(parts) != 3 {
		return err
	}
	message := "Not found: Table " + parts[0] + ":" + parts[1] + "." + parts[2] + " was not found in location " + location
	return failure(codes.NotFound, reasonNotFound, message)
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
	if plan.statementType != statementSelect {
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
		if err := p.requireDatasets(scope, plan); err != nil {
			return err
		}
		if err := p.expirePartitions(ctx, engine, scope, plan.tables); err != nil {
			return err
		}
		if err := evaluateExpirations(ctx, engine, plan.ddl); err != nil {
			return err
		}
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

// requireDatasets refuses a plan naming a dataset deleted since it was
// planned; the engine has no datasets and would create a table in one
// anyway. The caller holds the engine gate, which a dataset delete also
// holds, so the datasets stay while the plan runs.
func (p *Provider) requireDatasets(scope state.Scope, plan *queryPlan) error {
	datasets := make([]string, 0, len(plan.checks)+len(plan.ddl))
	for _, check := range plan.checks {
		datasets = append(datasets, datasetOfResource(check.resource))
	}
	for _, target := range plan.ddl {
		datasets = append(datasets, target.name.dataset)
	}
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback returns a wire status
		for _, dataset := range datasets {
			if s.datasets[dataset] == nil {
				return status.Errorf(codes.NotFound, "Not found: Dataset %s:%s", scope.Project, dataset)
			}
		}
		return nil
	})
}

// runStatements runs the script: a single DML statement through exec, so
// the engine reports the rows it changed (not for MERGE, whose count the
// engine does not report), and anything else through query.
func runStatements(ctx context.Context, engine *engineBackend, plan *queryPlan) (*engineResult, error) {
	if plan.merge != nil {
		return runStagedMerge(ctx, engine, plan)
	}
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

// runStagedMerge runs a MERGE whose source is a subquery: the subquery's rows
// go into a table no metadata names, the MERGE reads it, and the table is
// dropped whatever the MERGE did. Like any MERGE, it reports no row count.
func runStagedMerge(ctx context.Context, engine *engineBackend, plan *queryPlan) (*engineResult, error) {
	if _, err := engine.exec(ctx, plan.merge.stage, plan.args); err != nil {
		return nil, err
	}
	result, err := engine.exec(ctx, plan.merge.script, plan.args)
	if result != nil {
		result.affected = nil
	}
	staged := plan.merge.staged
	return result, errors.Join(err, dropEngineTable(ctx, engine, staged.dataset, staged.table, false))
}

// evaluateExpirations evaluates each CREATE target's expiration_timestamp
// option once, before the statement runs.
func evaluateExpirations(ctx context.Context, engine *engineBackend, targets []ddlTarget) error {
	for index, target := range targets {
		if target.expiration == "" {
			continue
		}
		result, err := engine.query(ctx, "SELECT UNIX_MICROS(CAST(("+target.expiration+") AS TIMESTAMP))", nil)
		if err != nil {
			return err
		}
		micros, ok := firstInt(result)
		if !ok {
			return invalidQuery("expiration_timestamp must be a TIMESTAMP")
		}
		targets[index].expires = time.UnixMicro(micros)
	}
	return nil
}

// reconcile reads each CREATE or DROP target back from the engine and makes
// the table metadata agree: a new table gets its engine schema, a replaced
// one its new schema, and a dropped one loses its metadata. An engine table
// left without metadata (its dataset gone, or its index entry refused) is
// dropped. The caller holds the engine gate, so no dataset changes meanwhile.
func (p *Provider) reconcile(ctx context.Context, engine *engineBackend, scope state.Scope, targets []ddlTarget) error {
	for _, target := range targets {
		name := target.name
		columns, exists, err := engine.columnsOf(ctx, name.dataset, name.table)
		if err != nil {
			return err
		}
		tracked := false
		applyErr := p.scopes.With(scope, func(s *scopeState) error {
			err := p.applyTarget(s, scope, target, exists, columns)
			tracked = s.tables[tableKey(name.dataset, name.table)] != nil
			return err
		})
		if exists && !tracked {
			if dropErr := dropEngineTable(ctx, engine, name.dataset, name.table, target.view); dropErr != nil {
				applyErr = errors.Join(applyErr, dropErr)
			}
		}
		if applyErr != nil {
			return fmt.Errorf("reconcile %s.%s: %w", name.dataset, name.table, applyErr)
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
	// A DDL table or view is unpartitioned and unclustered (PARTITION BY and
	// CLUSTER BY are gaps); it expires at its expiration_timestamp option, or
	// takes its dataset's default table expiration.
	shape := layout{expiration: target.expires}
	shape.applyDefaults(owner, p.clock())
	shape.store(record)
	fields := make([]any, 0, len(columns))
	for _, column := range columns {
		fields = append(fields, tableField(column.name, column.kind))
	}
	record.Schema = map[string]any{keyFields: fields}
	record.Type, record.ViewQuery, record.ViewRefs, record.Streamed = typeTable, "", nil, 0
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

// catalogOf copies the scope's datasets, their access lists, and live tables
// for planning; an expired table is gone.
func (p *Provider) catalogOf(scope state.Scope) *catalog {
	c := &catalog{project: scope.Project, datasets: map[string]string{}, tables: map[string]*tableRecord{}, access: map[string]accessList{}}
	now := p.clock()
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for id, record := range s.datasets {
			c.datasets[strings.ToLower(id)] = id
			c.access[strings.ToLower(id)] = record.Access.clone()
		}
		for _, record := range s.tables {
			if !record.expired(now) {
				c.tables[foldKey(record.Dataset, record.ID)] = record.clone()
			}
		}
		return nil
	})
	return c
}

// authorizeCheck runs one of a query's table checks; a denial names the
// checked table or dataset. A denied read of a table is GCP's query wording
// (recorded: bigquery-dataset-access).
func (p *Provider) authorizeCheck(call gateway.Call, project string, check tableCheck) error {
	related := call
	related.Method = check.method
	related.Resource = check.resource
	_, err := p.authorize(related, call.Method, project, check.resource)
	var refused *apiError
	if check.method != checkQueryTable || !strings.Contains(check.resource, "/tables/") || !errors.As(err, &refused) ||
		refused.status.Code() != codes.PermissionDenied {
		return err
	}
	name := project + ":" + datasetOfResource(check.resource) + "." + provider.LastSegment(check.resource)
	denial := refusal(codes.PermissionDenied, reasonAccessDenied,
		"Access Denied: Table "+name+": User does not have permission to query table "+name+", or perhaps it does not exist.")
	denial.decision = refused.decision
	return denial
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
	if p.clock().Sub(job.CreatedAt) > resultLifetime {
		return nil, status.Errorf(codes.NotFound, "Not found: the results of job %s:%s.%s have expired", scope.Project, cmp.Or(job.Location, "US"), job.ID)
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
		if job != nil {
			location = cmp.Or(location, job.Location)
		}
		return nil, failure(codes.NotFound, reasonNotFound, "Not found: Job "+scope.Project+":"+cmp.Or(location, "US")+"."+id)
	}
	return job, nil
}

// Retention of stored jobs. GCP keeps six months of job history ("BigQuery
// saves a six-month job history", Managing jobs) and a query's results in a
// temporary table for about 24 hours ("Using cached query results"). Without
// these bounds every job and its rows stayed in memory until a reset.
const (
	jobHistory     = 180 * 24 * time.Hour
	resultLifetime = 24 * time.Hour
)

// expireJobs forgets jobs older than the job history and frees the rows of
// jobs whose results have expired. It runs under the scope lock.
func (s *scopeState) expireJobs(now time.Time) {
	for id, job := range s.jobs {
		switch age := now.Sub(job.CreatedAt); {
		case age > jobHistory:
			delete(s.jobs, id)
		case age > resultLifetime:
			job.Rows = nil
		}
	}
}

// jobCopy returns a copy of one job taken under the scope lock, or nil. A
// stored job changes only when expireJobs drops its row slice, so a copy
// shares its rows and columns safely.
func (p *Provider) jobCopy(scope state.Scope, job string) *jobRecord {
	var copied *jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		s.expireJobs(p.clock())
		if record := s.jobs[job]; record != nil {
			clone := *record
			clone.Labels = maps.Clone(record.Labels)
			copied = &clone
		}
		return nil
	})
	return copied
}

// firstInt reads a one-row, one-column integer result.
func firstInt(result *engineResult) (int64, bool) {
	if len(result.rows) != 1 || len(result.rows[0]) != 1 {
		return 0, false
	}
	switch value := result.rows[0][0].(type) {
	case int64:
		return value, true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		return parsed, err == nil
	}
	return 0, false
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
	if value := query.Get(optionMaxResults); value != "" {
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
	keyStatistics   = "statistics"
	keyCode         = "code"
	keyUseLegacySQL = "useLegacySql"
	keyLastModified = "lastModifiedTime"
	keyDatasetID    = "datasetId"
	keyTableID      = "tableId"
	// optionMaxResults bounds a list or result page.
	optionMaxResults = "maxResults"
	// textTrue and jsonNull are the literal values options and bodies compare.
	textTrue = "true"
	jsonNull = "null"
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
	response["jobCreationReason"] = map[string]any{keyCode: "REQUESTED"}
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

// jobConfiguration renders a query job's configuration: the options it
// sent, and the table it wrote.
func jobConfiguration(project string, job *jobRecord, dryRun bool) map[string]any {
	query := map[string]any{keyQuery: job.Query, keyUseLegacySQL: false, "priority": "INTERACTIVE"}
	configuration := map[string]any{"jobType": "QUERY", keyQuery: query}
	if dryRun || job.DryRunSent {
		configuration["dryRun"] = dryRun
	}
	if job.JobTimeoutMs != "" {
		configuration["jobTimeoutMs"] = job.JobTimeoutMs
	}
	if destination := job.destination(dryRun); destination != nil {
		query["destinationTable"] = tableRef(project, *destination)
		if job.StatementType == statementSelect {
			query["writeDisposition"] = "WRITE_TRUNCATE"
		}
	}
	if len(job.Labels) > 0 {
		configuration["labels"] = job.Labels
	}
	return configuration
}

// addDDL reports a single DDL statement's operation and target.
func addDDL(statistics map[string]any, project string, job *jobRecord) {
	if job.DDLOperation != "" {
		statistics["ddlOperationPerformed"] = job.DDLOperation
	}
	if job.DDLTable != nil {
		statistics["ddlTargetTable"] = tableRef(project, *job.DDLTable)
	}
	if job.DDLDataset != "" {
		statistics["ddlTargetDataset"] = map[string]any{keyProjectID: project, keyDatasetID: job.DDLDataset}
	}
}

// jobWire renders a completed query job resource (jobs.insert, jobs.get); a
// dry run has no start or end time.
func jobWire(project string, job *jobRecord, dryRun bool) map[string]any {
	location := cmp.Or(job.Location, "US")
	created := strconv.FormatInt(job.CreatedAt.UnixMilli(), 10)
	configuration := jobConfiguration(project, job, dryRun)
	queryStatistics := map[string]any{keyBytesProcessed: "0", "totalBytesBilled": "0", keyCacheHit: false, "statementType": job.StatementType}
	addAffected(queryStatistics, job)
	addDDL(queryStatistics, project, job)
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
		keyStatistics:   statistics,
		keyStatus:       map[string]any{"state": "DONE"},
	}
	if _, email, ok := strings.Cut(job.Principal, ":"); ok {
		document["user_email"] = email
		document["principal_subject"] = job.Principal
	}
	document["jobCreationReason"] = map[string]any{keyCode: "REQUESTED"}
	return document
}

// tableRef renders a table name as a TableReference document.
func tableRef(project string, name tableName) map[string]any {
	return map[string]any{keyProjectID: project, keyDatasetID: name.dataset, keyTableID: name.table}
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

// jobsList lists jobs newest first, as GCP does (recorded): the caller's own,
// or with allUsers every user's, which needs bigquery.jobs.listAll. Every
// local job is done; stateFilter and the creation-time bounds filter them.
// parentJobId is a gap: local scripts run no child jobs.
func (p *Provider) jobsList(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	query := request.Query
	full, err := jobListOptions(query)
	if err != nil {
		return nil, err
	}
	owner := call.Principal.Subject
	if flag(query, "allUsers") {
		related := call
		related.Method = checkListAll
		if _, err := p.authorize(related, call.Method, scope.Project, provider.ProjectResource(scope.Project)); err != nil {
			return nil, err
		}
		owner = ""
	}
	keep, err := jobFilter(query)
	if err != nil {
		return nil, err
	}
	page, next, err := listPage(p.listedJobs(scope, owner, keep), query)
	if errors.Is(err, errBadPageToken) {
		return nil, failure(codes.InvalidArgument, reasonInvalid, "Invalid pagination token "+query.Get(optionPageToken))
	}
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page))
	for _, job := range page {
		items = append(items, jobItem(scope.Project, job, full))
	}
	document := provider.ListBody("jobs", items, next)
	document[keyKind], document[keyETag] = "bigquery#jobList", etagOf(p.clock())
	return &gateway.RESTResponse{JSON: document}, nil
}

// jobListOptions validates jobs.list's options and reports the full
// projection.
func jobListOptions(query url.Values) (bool, error) {
	if err := validateJobOptions(methodJobsList, query); err != nil {
		return false, err
	}
	if err := refuseOptions(query, "parentJobId"); err != nil {
		return false, err
	}
	switch projection := strings.ToLower(query.Get("projection")); projection {
	case "", "minimal", "full":
		return projection == "full", nil
	default:
		return false, failure(codes.InvalidArgument, reasonInvalid, "Invalid projection "+query.Get("projection"))
	}
}

// listedJobs copies the jobs owner created (every job for "") that keep
// accepts, newest first.
func (p *Provider) listedJobs(scope state.Scope, owner string, keep func(*jobRecord) bool) []*jobRecord {
	var jobs []*jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		s.expireJobs(p.clock())
		for _, job := range s.jobs {
			if (owner == "" || job.Principal == owner) && keep(job) {
				clone := *job
				jobs = append(jobs, &clone)
			}
		}
		return nil
	})
	slices.SortFunc(jobs, func(a, b *jobRecord) int {
		return cmp.Or(b.CreatedAt.Compare(a.CreatedAt), strings.Compare(b.ID, a.ID))
	})
	return jobs
}

// jobFilter reads stateFilter and the creation-time bounds (milliseconds).
func jobFilter(query url.Values) (func(*jobRecord) bool, error) {
	done := len(query["stateFilter"]) == 0
	for _, state := range query["stateFilter"] {
		switch strings.ToLower(state) {
		case "done":
			done = true
		case "pending", "running":
		default:
			return nil, failure(codes.InvalidArgument, reasonInvalid, "Invalid stateFilter "+state)
		}
	}
	bounds := [2]int64{0, math.MaxInt64}
	for i, name := range []string{"minCreationTime", "maxCreationTime"} {
		if text := query.Get(name); text != "" {
			value, err := strconv.ParseUint(text, 10, 63)
			if err != nil {
				return nil, failure(codes.InvalidArgument, reasonInvalid, "Invalid "+name+" "+text)
			}
			bounds[i] = int64(value)
		}
	}
	return func(job *jobRecord) bool {
		created := job.CreatedAt.UnixMilli()
		return done && created >= bounds[0] && created <= bounds[1]
	}, nil
}

// jobItem is one jobs.list entry: the recorded minimal fields, and with the
// full projection the configuration and creator too (recorded).
func jobItem(project string, job *jobRecord, full bool) map[string]any {
	document := jobWire(project, job, false)
	item := map[string]any{"state": "DONE"}
	keys := []string{"id", keyJobReference, keyKind, keyStatistics, keyStatus}
	if full {
		keys = append(keys, "configuration", "principal_subject", "user_email")
	}
	for _, key := range keys {
		if value, ok := document[key]; ok {
			item[key] = value
		}
	}
	if !full {
		statistics, _ := document[keyStatistics].(map[string]any)
		item[keyStatistics] = map[string]any{keyCreationTime: statistics[keyCreationTime], "startTime": statistics["startTime"], "endTime": statistics["endTime"]}
	}
	return item
}
