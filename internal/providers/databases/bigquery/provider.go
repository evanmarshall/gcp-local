// Package bigquery implements BigQuery v2 datasets, tables, insertAll,
// tabledata reads, and GoogleSQL query jobs. Table columns and rows live in
// an embedded GoogleSQL engine per project; the provider owns datasets and
// table metadata and authorizes every table a query names before it runs.
package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Provider owns datasets, table metadata, and jobs; each project's engine
// owns its tables' columns and rows.
type Provider struct {
	manager *backend.Manager
	index   *state.Index
	engine  *iam.Engine
	gate    provider.Gate
	scopes  *state.Store[scopeState]
	clock   func() time.Time
}

// New registers the BigQuery provider.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("bigquery permissions: %w", err)
	}
	if err := provider.RegisterPermissions(engine, relatedPermissions); err != nil {
		return nil, fmt.Errorf("bigquery related permissions: %w", err)
	}
	p := &Provider{
		manager: manager, index: index, engine: engine,
		// Denials are rendered by authorize; Message is BigQuery's dataset form.
		gate:   provider.Gate{Engine: engine, Index: index, Message: "Permission %s denied on %s", Attributes: func(target, _ string) iam.ResourceAttributes { return attributes(target) }},
		scopes: state.NewStore(newScopeState, state.GlobalScope),
		clock:  time.Now,
	}
	if err := p.registerBackend(manager, engine, index); err != nil {
		return nil, err
	}
	for method := range permissions {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// registerBackend attaches one GoogleSQL engine per project. Reset stops the
// engine, discarding its tables, before clearing datasets, jobs, policies,
// and index entries for that project.
func (p *Provider) registerBackend(manager *backend.Manager, engine *iam.Engine, index *state.Index) error {
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New: func(scope state.Scope) (backend.Backend, error) {
			return &engineBackend{project: scope.Project}, nil
		},
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			index.RemoveScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return fmt.Errorf("register the BigQuery backend: %w", err)
	}
	return nil
}

// Reset clears the scope's datasets, table metadata, and jobs.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		s.reset()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the bigquery scope: %w", err)
	}
	return nil
}

// handle is the gateway entry point for every BigQuery method. Every refusal
// is rendered as BigQuery's v2 error body, with errors[].reason.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid BigQuery request")
	}
	response, err := p.serve(call, request)
	if err != nil {
		return wireError(err), nil
	}
	return response, nil
}

// serve authorizes one call at its nearest indexed resource, then runs it.
func (p *Provider) serve(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	scope, target, err := resolveTarget(call, request)
	if err != nil {
		return nil, err
	}
	authorized := call
	authorized.Method = p.ownJobCheck(call, request, scope)
	decision, err := p.authorize(authorized, call.Method, scope.Project, target.Name)
	if err != nil {
		return nil, err
	}
	call.Resource = target.Name
	call.Location = scope.Location
	response, err := p.execute(call, request, scope)
	if err != nil {
		return nil, err
	}
	stampDecision(response, decision.ID)
	return response, nil
}

// authorize authorizes call against resource, or, when the index lacks it,
// against its nearest indexed ancestor (table, then dataset, then project),
// so absence reads 404 through the policies that govern it. provider.Nearest
// picks and checks the target under the scope lock, which every index change
// for this provider's datasets and tables also holds. A refusal is
// BigQuery's: a denial naming resource, or the 401 for a call without
// credentials naming method.
func (p *Provider) authorize(call gateway.Call, method, project, resource string) (iam.Decision, error) {
	candidates := []string{provider.ProjectResource(project)}
	for name := resource; name != candidates[len(candidates)-1]; name = parentResource(name, project) {
		candidates = append(candidates[:len(candidates)-1], name, candidates[len(candidates)-1])
	}
	decision, _, err := provider.Nearest(p.gate, p.scopes, provider.ProjectScope(project), call, resource, candidates...)
	switch {
	case err == nil:
		return decision, nil
	case status.Code(err) == codes.PermissionDenied:
		return decision, denied(decision, resource)
	case status.Code(err) == codes.Unauthenticated && call.Principal.State == gateway.Anonymous:
		return decision, credentialsMissing(method)
	default:
		return decision, err
	}
}

// parentResource names a table's dataset, or a dataset's project.
func parentResource(resource, project string) string {
	if dataset, _, isTable := strings.Cut(resource, "/tables/"); isTable {
		return dataset
	}
	return provider.ProjectResource(project)
}

// stampDecision names the call's IAM decision on its response.
func stampDecision(response *gateway.RESTResponse, id string) {
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set(headerDecision, id)
}

// headerDecision names the IAM decision that answered the request.
const headerDecision = "X-Gcp-Local-Decision-Id"

// resolveTarget derives the scope and IAM target for one request. Dataset methods
// authorize against the dataset resource. Table, row, and table IAM methods
// authorize against the table, whose policy inherits the dataset's; a table
// that does not exist authorizes against its dataset (authorize), so only
// callers with dataset access learn it is absent.
func resolveTarget(call gateway.Call, request *gateway.RESTRequest) (state.Scope, iam.Target, error) {
	project := request.Path["project"]
	scope := provider.ProjectScope(project)
	target := iam.Target{Name: provider.ProjectResource(project)}
	dataset, table := request.Path[keyDatasetID], request.Path[keyTableID]
	switch call.Method {
	case methodDatasetsCreate, methodDatasetsList, methodJobsQuery, methodJobsResults, methodJobsInsert, methodJobsGet, methodJobsList:
		// The collection-level create takes the dataset id from the body, and
		// query jobs authorize at the project, like the documented rows.
		return scope, target, nil
	}
	if !uid.MatchString(dataset) {
		return scope, target, status.Errorf(codes.InvalidArgument, "Invalid dataset id %q", dataset)
	}
	switch call.Method {
	case methodDatasetsGet, methodDatasetsDelete, methodDatasetsUpdate, methodDatasetsGetIam, methodDatasetsSetIam,
		methodTablesCreate, methodTablesList:
		target.Name = datasetResource(project, dataset)
		return scope, target, nil
	default:
		if !uid.MatchString(table) {
			return scope, target, failure(codes.InvalidArgument, reasonInvalid, fmt.Sprintf("Invalid table ID %q.", table))
		}
		target.Name = tableResource(project, dataset, table)
		return scope, target, nil
	}
}

// BigQuery's errors[].reason values (recorded: bigquery-api-surface).
const (
	reasonInvalid      = "invalid"
	reasonInvalidQuery = "invalidQuery"
	reasonNotFound     = "notFound"
	reasonDuplicate    = "duplicate"
	reasonAccessDenied = "accessDenied"
	reasonInUse        = "resourceInUse"
)

// apiError is a refusal with BigQuery's errors[] entry: entry nil sends an
// empty errors list, as GCP's policy-version refusal does.
type apiError struct {
	status   *status.Status
	entry    map[string]any
	decision string
}

func (e *apiError) Error() string { return e.status.Message() }

// GRPCStatus lets callers and tests read the code.
func (e *apiError) GRPCStatus() *status.Status { return e.status }

// failure is a refusal whose errors[] entry carries reason in the global
// domain.
func failure(code codes.Code, reason, message string) error { return refusal(code, reason, message) }

// refusal is failure's concrete form.
func refusal(code codes.Code, reason, message string) *apiError {
	return &apiError{status: status.New(code, message), entry: entry(reason, message)}
}

// entry is one errors[] entry in the global domain.
func entry(reason, message string) map[string]any {
	return map[string]any{keyMessage: message, keyDomain: "global", keyReason: reason}
}

// Keys of the errors[] entries.
const (
	keyMessage = "message"
	keyDomain  = "domain"
	keyReason  = "reason"
	keyStatus  = "status"
)

// queryFailure marks a query's refusal of its SQL as invalidQuery on the q
// parameter, as GCP does (recorded for a syntax error); other refusals pass.
func queryFailure(err error) error {
	var refused *apiError
	if err == nil || errors.As(err, &refused) || status.Code(err) != codes.InvalidArgument {
		return err
	}
	message := status.Convert(err).Message()
	located := entry(reasonInvalidQuery, message)
	located["location"], located["locationType"] = "q", "parameter"
	return &apiError{status: status.New(codes.InvalidArgument, message), entry: located}
}

// denied is BigQuery's 403 naming the denied permission and the addressed
// dataset, table, or project (recorded for each).
func denied(decision iam.Decision, resource string) error {
	permission := provider.DeniedPermission(decision)
	project, rest, _ := strings.Cut(strings.TrimPrefix(resource, "//bigquery.googleapis.com/projects/"), "/datasets/")
	dataset, table, isTable := strings.Cut(rest, "/tables/")
	var message string
	switch {
	case strings.HasPrefix(resource, "//cloudresourcemanager.googleapis.com/projects/"):
		project = provider.LastSegment(resource)
		message = fmt.Sprintf("Access Denied: Project %s: User does not have %s permission in project %s.", project, permission, project)
	case isTable:
		name := project + ":" + dataset + "." + table
		message = fmt.Sprintf("Access Denied: Table %s: Permission %s denied on table %s (or it may not exist).", name, permission, name)
	default:
		name := project + ":" + dataset
		message = fmt.Sprintf("Access Denied: Dataset %s: Permission %s denied on dataset %s (or it may not exist).", name, permission, name)
	}
	denial := refusal(codes.PermissionDenied, reasonAccessDenied, message)
	denial.decision = decision.ID
	return denial
}

// rpcNames are the google.cloud.bigquery.v2 RPCs GCP names in a
// missing-credential 401 (recorded for datasets.get).
var rpcNames = map[string]string{
	methodDatasetsCreate: "DatasetService.InsertDataset", methodDatasetsDelete: "DatasetService.DeleteDataset",
	methodDatasetsGet: "DatasetService.GetDataset", methodDatasetsList: "DatasetService.ListDatasets",
	methodDatasetsUpdate: "DatasetService.PatchDataset", methodTablesCreate: "TableService.InsertTable",
	methodTablesDelete: "TableService.DeleteTable", methodTablesGet: "TableService.GetTable",
	methodTablesList: "TableService.ListTables", methodTablesUpdate: "TableService.PatchTable",
	methodJobsQuery: "JobService.Query", methodJobsResults: "JobService.GetQueryResults",
	methodJobsInsert: "JobService.InsertJob", methodJobsGet: "JobService.GetJob", methodJobsList: "JobService.ListJobs",
}

// credentialsMissing is GCP's 401 for a call without credentials, with
// BigQuery's "Login Required." entry (recorded).
func credentialsMissing(method string) error {
	rpc := ""
	if name, ok := rpcNames[method]; ok {
		rpc = "google.cloud.bigquery.v2." + name
	}
	login := entry("required", "Login Required.")
	login["location"], login["locationType"] = "Authorization", "header"
	return &apiError{status: status.Convert(provider.CredentialsMissing(apiHost, rpc)), entry: login}
}

// apiHost is the API host conditions and 401s name.
const apiHost = "bigquery.googleapis.com"

// attributes are the IAM Condition attributes of a dataset or table, as the
// BigQuery IAM Conditions page documents them. GCP refuses conditions on
// table policies (recorded), so only project-level grants can use them.
func attributes(target string) iam.ResourceAttributes {
	name, ok := strings.CutPrefix(target, "//"+apiHost+"/")
	switch {
	case !ok:
		return iam.ResourceAttributes{}
	case strings.Contains(name, "/tables/"):
		return iam.ResourceAttributes{Name: name, Type: apiHost + "/Table", Service: apiHost}
	default:
		return iam.ResourceAttributes{Name: name, Type: apiHost + "/Dataset", Service: apiHost}
	}
}

// httpCodes maps the statuses BigQuery refuses with to HTTP codes, as the
// gateway does.
var httpCodes = map[codes.Code]int{
	codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400, codes.Unauthenticated: 401,
	codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.Aborted: 409,
	codes.ResourceExhausted: 429, codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503,
	codes.DeadlineExceeded: 504,
}

// defaultReasons are the errors[] reasons of plain status refusals.
var defaultReasons = map[codes.Code]string{
	codes.InvalidArgument: reasonInvalid, codes.FailedPrecondition: reasonInvalid, codes.NotFound: reasonNotFound,
	codes.AlreadyExists: reasonDuplicate, codes.PermissionDenied: reasonAccessDenied,
}

// wireError renders a refusal as BigQuery's v2 error body: code, message,
// status, errors[], and google.rpc details.
func wireError(err error) *gateway.RESTResponse {
	refused := refusalOf(err)
	code, ok := httpCodes[refused.status.Code()]
	if !ok {
		code = http.StatusInternalServerError
	}
	entries := []any{}
	if refused.entry != nil {
		entries = append(entries, refused.entry)
	}
	body := map[string]any{keyCode: code, keyMessage: refused.status.Message(), "errors": entries, keyStatus: statusName(refused.status.Code())}
	var details []json.RawMessage
	for _, detail := range refused.status.Proto().GetDetails() {
		if encoded, encodeErr := protojson.Marshal(detail); encodeErr == nil {
			details = append(details, encoded)
		}
	}
	if len(details) > 0 {
		body["details"] = details
	}
	response := &gateway.RESTResponse{Status: code, JSON: map[string]any{"error": body}}
	if refused.decision != "" {
		stampDecision(response, refused.decision)
	}
	return response
}

// refusalOf reads an error as a refusal. A plain status gets GCP's reason
// for its code; one carrying a BadRequest names no domain, as recorded. A
// refusal without a recorded reason, such as a coverage gap, has an empty
// errors list. A failure that carries no status is an internal error, never
// its text.
func refusalOf(err error) *apiError {
	var refused *apiError
	var wrapped interface{ GRPCStatus() *status.Status }
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return &apiError{status: status.FromContextError(err)}
	case errors.As(err, &refused):
		return refused
	case errors.As(err, &wrapped):
		current := wrapped.GRPCStatus()
		refused = &apiError{status: current}
		if reason, ok := defaultReasons[current.Code()]; ok {
			refused.entry = entry(reason, current.Message())
			if len(current.Proto().GetDetails()) > 0 {
				delete(refused.entry, keyDomain)
			}
		}
		return refused
	default:
		return &apiError{status: status.New(codes.Internal, "Internal provider error")}
	}
}

// statusNames spell the statuses whose Google name differs from the code's
// upper-cased name.
var statusNames = map[codes.Code]string{
	codes.Canceled: "CANCELLED", codes.InvalidArgument: "INVALID_ARGUMENT", codes.DeadlineExceeded: "DEADLINE_EXCEEDED",
	codes.NotFound: "NOT_FOUND", codes.AlreadyExists: "ALREADY_EXISTS", codes.PermissionDenied: "PERMISSION_DENIED",
	codes.ResourceExhausted: "RESOURCE_EXHAUSTED", codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.OutOfRange: "OUT_OF_RANGE", codes.DataLoss: "DATA_LOSS",
}

// statusName spells a status as Google's REST errors do.
func statusName(code codes.Code) string {
	if name, ok := statusNames[code]; ok {
		return name
	}
	return strings.ToUpper(code.String())
}

// serviceName owns the BigQuery backend registration.
const serviceName = "bigquery"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodDatasetsCreate = "bigquery.datasets.create"
	methodDatasetsDelete = "bigquery.datasets.delete"
	methodDatasetsGet    = "bigquery.datasets.get"
	methodDatasetsList   = "bigquery.datasets.list"
	methodDatasetsUpdate = "bigquery.datasets.update"
	methodTablesCreate   = "bigquery.tables.create"
	methodTablesDelete   = "bigquery.tables.delete"
	methodTablesGet      = "bigquery.tables.get"
	methodTablesList     = "bigquery.tables.list"
	methodTablesUpdate   = "bigquery.tables.update"
	methodRowsInsert     = "bigquery.tabledata.insertAll"
	methodRowsList       = "bigquery.tabledata.list"
	methodJobsQuery      = "bigquery.jobs.query"
	methodJobsResults    = "bigquery.jobs.getQueryResults"
	methodJobsInsert     = "bigquery.jobs.insert"
	methodJobsGet        = "bigquery.jobs.get"
	methodJobsList       = "bigquery.jobs.list"
	methodDatasetsGetIam = "bigquery.datasets.getIamPolicy"
	methodDatasetsSetIam = "bigquery.datasets.setIamPolicy"
	methodTablesGetIam   = "bigquery.tables.getIamPolicy"
	methodTablesSetIam   = "bigquery.tables.setIamPolicy"
)

// permissions follows the documented BigQuery access-control rows.
var permissions = map[string]string{
	methodDatasetsCreate: "bigquery.datasets.create",
	methodDatasetsDelete: "bigquery.datasets.delete",
	methodDatasetsGet:    "bigquery.datasets.get",
	methodDatasetsList:   "bigquery.datasets.get",
	methodDatasetsUpdate: "bigquery.datasets.update",
	methodTablesCreate:   "bigquery.tables.create",
	methodTablesDelete:   "bigquery.tables.delete",
	methodTablesGet:      "bigquery.tables.get",
	methodTablesList:     "bigquery.tables.list",
	methodTablesUpdate:   "bigquery.tables.update",
	methodRowsInsert:     "bigquery.tables.updateData",
	methodRowsList:       "bigquery.tables.getData",
	methodJobsQuery:      permJobsCreate,
	methodJobsResults:    "bigquery.jobs.get",
	methodJobsInsert:     permJobsCreate,
	methodJobsGet:        "bigquery.jobs.get",
	methodJobsList:       "bigquery.jobs.list",
	methodDatasetsGetIam: "bigquery.datasets.getIamPolicy",
	methodDatasetsSetIam: "bigquery.datasets.setIamPolicy",
	methodTablesGetIam:   "bigquery.tables.getIamPolicy",
	methodTablesSetIam:   "bigquery.tables.setIamPolicy",
}

// Checks that are not routes. A query needs bigquery.jobs.create on the
// project and, per table it names (access-control "Run a query" row and the
// DML and DDL references): bigquery.tables.getData to read, updateData to
// change rows, tables.create on the dataset to create, tables.update to
// replace, and tables.delete to drop. jobs.get and getQueryResults need
// bigquery.jobs.get, except for the job's creator ("Requires that you're the
// person who ran the job", jobs.get reference); the creator's check is
// modeled as bigquery.jobs.create on the project, not recorded.
const (
	checkQueryTable  = "bigquery.query.readTable"
	checkQueryWrite  = "bigquery.query.writeTable"
	checkQueryCreate = "bigquery.query.createTable"
	checkQueryUpdate = "bigquery.query.replaceTable"
	checkQueryDrop   = "bigquery.query.dropTable"
	checkOwnJob      = "bigquery.jobs.readOwn"
	// checkListAll lists other users' jobs (jobs.list allUsers=true).
	checkListAll = "bigquery.jobs.listAllUsers"
)

var relatedPermissions = map[string]string{
	checkQueryTable:  "bigquery.tables.getData",
	checkQueryWrite:  "bigquery.tables.updateData",
	checkQueryCreate: "bigquery.tables.create",
	checkQueryUpdate: "bigquery.tables.update",
	checkQueryDrop:   "bigquery.tables.delete",
	checkOwnJob:      permJobsCreate,
	checkListAll:     "bigquery.jobs.listAll",
}

// Resource-name helpers follow the real BigQuery namespace.
const (
	kindDataset = "dataset"
	kindTable   = "table"
	uidPattern  = `^[A-Za-z0-9_.-]{1,1000}$`
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// New dataset and table ids follow BigQuery's alphabets; neither may hold a
// dot, backtick, or backslash, which would change an engine path.
var (
	datasetIDPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	tableIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// validDatasetID and validTableID check a new id; both are at most 1,024
// characters.
func validDatasetID(id string) bool { return len(id) <= 1024 && datasetIDPattern.MatchString(id) }
func validTableID(id string) bool   { return len(id) <= 1024 && tableIDPattern.MatchString(id) }

// Table types.
const (
	typeTable = "TABLE"
	typeView  = "VIEW"
)

// datasetRecord is one stored BigQuery dataset. Patched records that a
// patch ran: GCP then reports maxTimeTravelHours (recorded). Access is the
// dataset's access list; the provider owns it and derives the dataset's IAM
// policy in the engine from its member entries. The default expirations are
// in milliseconds, 0 for none.
type datasetRecord struct {
	ID                           string
	Labels                       map[string]string
	FriendlyName                 string
	Description                  string
	Location                     string
	CreatedAt                    time.Time
	ModifiedAt                   time.Time
	Patched                      bool
	Access                       accessList
	DefaultTableExpirationMs     int64
	DefaultPartitionExpirationMs int64
}

// tableRecord is one table's metadata. Its columns and rows live in the
// engine; Schema is the REST schema as created (modes and descriptions the
// engine does not keep), or as the engine reported it for a DDL table.
// ViewRefs are the tables a view reads, resolved when it was created.
type tableRecord struct {
	Dataset      string
	ID           string
	Type         string
	Schema       map[string]any
	ViewQuery    string
	ViewRefs     []tableName
	Labels       map[string]string
	FriendlyName string
	Description  string
	Location     string
	CreatedAt    time.Time
	ModifiedAt   time.Time
	// The table's layout (partitions.go). Expiration is zero for none.
	TimePartitioning       *timePartitioning
	RangePartitioning      *rangePartitioning
	Clustering             []string
	RequirePartitionFilter *bool
	Expiration             time.Time
}

// jobRecord is one stored query job with its materialized result. Principal
// is the creator, who may read the job without bigquery.jobs.get. Affected
// is the DML row count when the engine reports one.
type jobRecord struct {
	ID            string
	Columns       []engineColumn
	Rows          [][]any
	StatementType string
	Affected      *int64
	// TargetSchema is the schema of the table a DML or CREATE statement
	// wrote, which GCP returns in place of a result schema.
	TargetSchema map[string]any
	Location     string
	CreatedAt    time.Time
	Principal    string
	Query        string
	Labels       map[string]string
	// RequestID is the jobs.query requestId that ran the job, if any.
	RequestID string
}

// scopeState is one project slice of provider state.
type scopeState struct {
	datasets map[string]*datasetRecord
	tables   map[string]*tableRecord
	jobs     map[string]*jobRecord
}

func newScopeState() scopeState {
	return scopeState{
		datasets: map[string]*datasetRecord{},
		tables:   map[string]*tableRecord{},
		jobs:     map[string]*jobRecord{},
	}
}

func (s *scopeState) reset() {
	s.datasets = map[string]*datasetRecord{}
	s.tables = map[string]*tableRecord{}
	s.jobs = map[string]*jobRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("bigquery requires registry, manager, index, and iam")

func tableKey(dataset, table string) string { return dataset + "/" + table }

// datasetResource names one dataset in the IAM index.
func datasetResource(project, dataset string) string {
	return "//bigquery.googleapis.com/projects/" + project + "/datasets/" + dataset
}

// tableResource names one table in the IAM index.
func tableResource(project, dataset, table string) string {
	return datasetResource(project, dataset) + "/tables/" + table
}
