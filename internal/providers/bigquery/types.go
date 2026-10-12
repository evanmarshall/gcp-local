// Package bigquery implements the BigQuery v2 control plane: datasets,
// tables with typed schemas, insertAll row storage, tabledata reads, and a
// bounded single-table SELECT through jobs.query and query jobs.insert.
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

// Checks that are not routes. A query needs bigquery.tables.getData on the
// table it reads besides bigquery.jobs.create on the project (access-control
// "Run a query" row). jobs.get and getQueryResults need bigquery.jobs.get,
// except for the job's creator ("Requires that you're the person who ran the
// job", jobs.get reference); the creator's check is modeled as
// bigquery.jobs.create on the project, not recorded.
const (
	checkQueryTable = "bigquery.query.readTable"
	checkOwnJob     = "bigquery.jobs.readOwn"
)

var relatedPermissions = map[string]string{
	checkQueryTable: "bigquery.tables.getData",
	checkOwnJob:     permJobsCreate,
}

// Resource-name helpers follow the real BigQuery namespace.
const (
	kindDataset = "dataset"
	kindTable   = "table"
	uidPattern  = `^[A-Za-z0-9_.-]{1,1000}$`
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// datasetRecord is one stored BigQuery dataset.
type datasetRecord struct {
	ID           string
	Labels       map[string]string
	FriendlyName string
	Location     string
	CreatedAt    time.Time
}

// tableRecord is one stored table with its schema and rows.
type tableRecord struct {
	Dataset      string
	ID           string
	Schema       map[string]any
	Labels       map[string]string
	FriendlyName string
	Rows         []map[string]any
	Location     string
	CreatedAt    time.Time
}

// jobRecord is one stored query job with its materialized result. Principal
// is the creator, who may read the job without bigquery.jobs.get.
type jobRecord struct {
	ID        string
	Fields    []map[string]any
	Rows      []map[string]any
	Location  string
	CreatedAt time.Time
	Principal string
	Query     string
	Labels    map[string]string
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
