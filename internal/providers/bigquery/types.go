// Package bigquery implements the BigQuery v2 control plane: datasets,
// tables with typed schemas, insertAll row storage, and tabledata reads.
// Queries (jobs.query) remain an explicit gap. No cloud recording covers
// these methods yet, so nothing here claims recorded conformance.
package bigquery

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	methodJobsQuery:      "bigquery.jobs.create",
	methodJobsResults:    "bigquery.jobs.get",
	methodDatasetsGetIam: "bigquery.datasets.getIamPolicy",
	methodDatasetsSetIam: "bigquery.datasets.setIamPolicy",
	methodTablesGetIam:   "bigquery.tables.getIamPolicy",
	methodTablesSetIam:   "bigquery.tables.setIamPolicy",
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
	CreatedAt    time.Time
}

// jobRecord is one stored query job with its materialized result.
type jobRecord struct {
	ID        string
	Fields    []map[string]any
	Rows      []map[string]any
	CreatedAt time.Time
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

// wireError builds the BigQuery error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the BigQuery not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// scopeOf names one project scope.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// tableKey is the state key of one table.
func tableKey(dataset, table string) string { return dataset + "/" + table }

// datasetResource names one dataset in the IAM index.
func datasetResource(project, dataset string) string {
	return "//bigquery.googleapis.com/projects/" + project + "/datasets/" + dataset
}

// tableResource names one table in the IAM index.
func tableResource(project, dataset, table string) string {
	return datasetResource(project, dataset) + "/tables/" + table
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}
