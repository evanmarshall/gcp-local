// Package bigquery implements BigQuery v2 datasets, tables, insertAll,
// tabledata reads, and GoogleSQL query jobs. Table columns and rows live in
// an embedded GoogleSQL engine per project; the provider owns datasets and
// table metadata and authorizes every table a query names before it runs.
package bigquery

import (
	"errors"
	"regexp"
	"time"
)

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
)

var relatedPermissions = map[string]string{
	checkQueryTable:  "bigquery.tables.getData",
	checkQueryWrite:  "bigquery.tables.updateData",
	checkQueryCreate: "bigquery.tables.create",
	checkQueryUpdate: "bigquery.tables.update",
	checkQueryDrop:   "bigquery.tables.delete",
	checkOwnJob:      permJobsCreate,
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

// datasetRecord is one stored BigQuery dataset.
type datasetRecord struct {
	ID           string
	Labels       map[string]string
	FriendlyName string
	Location     string
	CreatedAt    time.Time
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
	Location     string
	CreatedAt    time.Time
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
