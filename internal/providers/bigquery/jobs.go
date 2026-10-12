package bigquery

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// jobIDPattern is the documented JobReference.jobId alphabet; ids are at most
// 1,024 characters.
var jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func gap(message string) error {
	return status.Error(codes.Unimplemented, "BigQuery coverage gap: "+message)
}

// errLegacySQL refuses legacy SQL on jobs.query and jobs.insert.
var errLegacySQL = status.Error(codes.Unimplemented, "Legacy SQL is not supported")

// systemOptions are the Google API system parameters the job methods accept.
// fields selects top-level response fields (partial response).
var systemOptions = []string{"prettyPrint", keyFields, "alt"}

// jobOptions are the query parameters each job method's reference defines.
var jobOptions = map[string][]string{
	methodJobsInsert: nil,
	methodJobsGet:    {keyLocation},
	methodJobsResults: {keyLocation, "maxResults", "pageToken", "startIndex", "timeoutMs",
		optionInt64Timestamp, optionTimestampFormat},
}

// Result format options; they change only how TIMESTAMP cells are encoded.
const (
	optionInt64Timestamp  = "formatOptions.useInt64Timestamp"
	optionTimestampFormat = "formatOptions.timestampOutputFormat"
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
			Query        string `json:"query"`
			UseLegacySQL *bool  `json:"useLegacySql"`
			Priority     string `json:"priority"`
			// The query cache is an optimization; results are the same.
			UseQueryCache *bool `json:"useQueryCache"`
		} `json:"query"`
	} `json:"configuration"`
}

// decodeInsert reads a jobs.insert body. Only Standard SQL query jobs are
// served; load, extract, and copy jobs, destination tables, parameters, and
// every other Job field are coverage gaps.
func decodeInsert(body []byte, project string) (insertBody, error) {
	var job insertBody
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&job); err != nil {
		if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return job, gap("job field " + strings.Trim(field, `"`))
		}
		return job, provider.Invalid("Invalid job body")
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
	job, denied, err := p.runQuery(call, scope, body.Configuration.Query.Query, body.JobReference.Location)
	if err != nil || denied != nil {
		return denied, err
	}
	job.ID = cmp.Or(body.JobReference.JobID, job.ID)
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

// jobsQuery runs a bounded local SELECT against one stored table.
// Supported grammar: SELECT * | column[, column] FROM dataset.table
// [WHERE column = literal]; joins, aggregations, and functions are refused
// as explicit gaps rather than approximated.
func (p *Provider) jobsQuery(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		Query        string `json:"query"`
		UseLegacySQL bool   `json:"useLegacySql"`
		DryRun       bool   `json:"dryRun"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid query body")
	}
	if body.UseLegacySQL {
		return nil, errLegacySQL
	}
	job, denied, err := p.runQuery(call, scope, body.Query, "")
	if err != nil || denied != nil {
		return denied, err
	}
	if body.DryRun {
		return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, true)}, nil
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.jobs[job.ID] = job
		return nil
	}); err != nil {
		return nil, fmt.Errorf("store query job: %w", err)
	}
	return &gateway.RESTResponse{JSON: queryResponse(scope.Project, job, false)}, nil
}

// runQuery parses one query, resolves and authorizes its table, and returns
// the unstored job with its result. A denied table read returns the 403.
// location, when set, must be the table's location.
func (p *Provider) runQuery(call gateway.Call, scope state.Scope, sql, location string) (*jobRecord, *gateway.RESTResponse, error) {
	parsed, err := parseQuery(sql)
	if err != nil {
		return nil, nil, err
	}
	if parsed.project != "" && parsed.project != scope.Project {
		return nil, nil, gap("queries read only tables in the job's project")
	}
	table := p.tableLocked(scope, parsed.dataset, parsed.table)
	if table == nil {
		return nil, nil, provider.NotFound(kindTable, parsed.table)
	}
	if location != "" && !strings.EqualFold(location, table.Location) {
		return nil, nil, status.Errorf(codes.NotFound, "Not found: Dataset %s:%s was not found in location %s", scope.Project, parsed.dataset, location)
	}
	if denied, err := p.authorizeTableRead(call, scope, parsed); denied != nil || err != nil {
		return nil, denied, err
	}
	fields, err := projectFields(nullableFields(schemaFields(table.Schema)), parsed.columns)
	if err != nil {
		return nil, nil, err
	}
	now := p.clock()
	return &jobRecord{
		ID: "job_" + strconv.FormatInt(now.UnixNano(), 10), Fields: fields, Rows: selectRows(table.Rows, fields, parsed),
		Location: table.Location, CreatedAt: now, Principal: call.Principal.Subject, Query: sql,
	}, nil, nil
}

// authorizeTableRead checks bigquery.tables.getData on the queried table.
func (p *Provider) authorizeTableRead(call gateway.Call, scope state.Scope, parsed parsedQuery) (*gateway.RESTResponse, error) {
	related := call
	related.Method = checkQueryTable
	related.Resource = tableResource(scope.Project, parsed.dataset, parsed.table)
	decision, err := p.engine.Authorize(related, iam.Target{Name: related.Resource}, nil)
	if err == nil {
		return nil, nil //nolint:nilnil // a nil response with a nil error means the read is allowed
	}
	if code := status.Code(err); code != codes.PermissionDenied && code != codes.Unauthenticated {
		return nil, fmt.Errorf("authorize the table read: %w", err)
	}
	denied := provider.Denied(decision, related.Resource)
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
	job := p.jobLocked(scope, request.Path["jobId"])
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
	if err := timestampOptions(request.Query, job.Fields); err != nil {
		return nil, err
	}
	start, end, err := resultPage(request.Query, len(job.Rows))
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: partial(request.Query, resultsWire(scope.Project, job, start, end))}, nil
}

// requestedJob validates a job read's options and returns the stored job; a
// job in another location than the one requested is not found.
func (p *Provider) requestedJob(method string, request *gateway.RESTRequest, scope state.Scope) (*jobRecord, error) {
	if err := validateJobOptions(method, request.Query); err != nil {
		return nil, err
	}
	id := request.Path["jobId"]
	job := p.jobLocked(scope, id)
	if location := request.Query.Get(keyLocation); job == nil || (location != "" && !strings.EqualFold(location, job.Location)) {
		return nil, provider.NotFound("job", id)
	}
	return job, nil
}

// jobLocked reads one job under the scope lock.
func (p *Provider) jobLocked(scope state.Scope, job string) *jobRecord {
	var record *jobRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.jobs[job]; return nil })
	return record
}

// timestampOptions refuses result format options on a TIMESTAMP column: cells
// are returned as stored, so those options would change nothing here.
func timestampOptions(query url.Values, fields []map[string]any) error {
	format := query.Get(optionTimestampFormat)
	if query.Get(optionInt64Timestamp) != "true" && (format == "" || format == "TIMESTAMP_OUTPUT_FORMAT_UNSPECIFIED") {
		return nil
	}
	for _, field := range fields {
		if field["type"] == "TIMESTAMP" {
			return gap("result format options on TIMESTAMP columns")
		}
	}
	return nil
}

// resultPage reads the requested row window. startIndex and pageToken (the
// decimal index of the next row, which this emulator issues) set the first
// row; maxResults bounds the page, and 0 returns no rows.
func resultPage(query url.Values, total int) (int, int, error) {
	start := 0
	for _, key := range []string{"startIndex", "pageToken"} {
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
