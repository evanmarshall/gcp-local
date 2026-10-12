package firestore

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// serviceName owns the Firestore backend registration.
const serviceName = "firestore"

// REST method names; the generated route table uses the same identifiers.
// The GET documents route serves both get (even segment count) and list (odd).
const (
	methodDocumentsGet       = "firestore.documents.get"
	methodFieldsGet          = "firestore.fields.get"
	methodFieldsUpdate       = "firestore.fields.update"
	methodDocumentsCreate    = "firestore.documents.create"
	methodDocumentsUpdate    = "firestore.documents.update"
	methodDocumentsDelete    = "firestore.documents.delete"
	methodBatchGet           = "firestore.documents.batchGet"
	methodBatchWrite         = "firestore.documents.batchWrite"
	methodBeginTransaction   = "firestore.documents.beginTransaction"
	methodCommit             = "firestore.documents.commit"
	methodRollback           = "firestore.documents.rollback"
	methodRunQuery           = "firestore.documents.runQuery"
	methodRunAggregation     = "firestore.documents.runAggregationQuery"
	methodListCollectionIDs  = "firestore.documents.listCollectionIds"
	grpcMethodPrefix         = "/google.firestore.v1.Firestore/"
	planDocumentsList        = "firestore.documents.list"
	planListen               = "firestore.documents.listen"
	planWriteCreate          = "firestore.writes.create"
	planWriteUpdate          = "firestore.writes.update"
	planWriteUpsert          = "firestore.writes.upsert"
	planWriteDelete          = "firestore.writes.delete"
	planWriteNone            = "firestore.writes.none"
	permissionEntitiesGet    = "datastore.entities.get"
	permissionEntitiesList   = "datastore.entities.list"
	permissionEntitiesCreate = "datastore.entities.create"
	permissionEntitiesUpdate = "datastore.entities.update"
	permissionDatabasesGet   = "datastore.databases.get"
)

// plans are the IAM permission plans, keyed by plan name. They follow the
// "Required permissions for Firestore API methods" table in Google's Firestore
// IAM documentation (read 2026-10-11). Firestore permission names are the
// Cloud Datastore ones. Commit and BatchWrite authorize one write plan per
// distinct write kind they carry:
//   - update/transform with exists=false: create
//   - update/transform with exists=true or an updateTime precondition: update
//   - update/transform with no precondition: create and update
//   - delete: delete
//
// Local choices where the table has no row: Listen uses the query plan (get
// and list); a commit or batch write with no writes needs databases.get, the
// transaction permission; BatchWrite deletes use the commit delete row.
var plans = map[string][]string{
	methodDocumentsGet:      {permissionEntitiesGet},
	planDocumentsList:       {permissionEntitiesGet, permissionEntitiesList},
	methodDocumentsCreate:   {permissionEntitiesCreate},
	methodDocumentsUpdate:   {permissionEntitiesUpdate},
	methodDocumentsDelete:   {"datastore.entities.delete"},
	methodBatchGet:          {permissionEntitiesGet},
	methodBeginTransaction:  {permissionDatabasesGet},
	methodRollback:          {permissionDatabasesGet},
	methodRunQuery:          {permissionEntitiesGet, permissionEntitiesList},
	methodRunAggregation:    {permissionEntitiesGet, permissionEntitiesList},
	methodListCollectionIDs: {permissionEntitiesList},
	planListen:              {permissionEntitiesGet, permissionEntitiesList},
	planWriteCreate:         {permissionEntitiesCreate},
	planWriteUpdate:         {permissionEntitiesUpdate},
	planWriteUpsert:         {permissionEntitiesCreate, permissionEntitiesUpdate},
	planWriteDelete:         {"datastore.entities.delete"},
	planWriteNone:           {permissionDatabasesGet},
	methodFieldsGet:         {"datastore.schemas.get"},
	methodFieldsUpdate:      {"datastore.schemas.update"},
}

// restMethods are the REST routes the provider serves.
var restMethods = []string{
	methodDocumentsGet, methodDocumentsCreate, methodDocumentsUpdate, methodDocumentsDelete,
	methodBatchGet, methodBatchWrite, methodBeginTransaction, methodCommit, methodRollback,
	methodRunQuery, methodRunAggregation, methodListCollectionIDs, methodFieldsGet, methodFieldsUpdate,
}

// Resource-name helpers follow the real Firestore namespace.
const (
	uidPattern = `^[^/]{1,1000}$`
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// fieldRecord is one configured collection-group field.
type fieldRecord struct {
	Name        string
	TTLConfig   map[string]any
	IndexConfig map[string]any
	Configured  bool
	CreatedAt   time.Time
}

// scopeState is one project slice of provider-owned state: collection-group
// field configuration only. Documents live in the scope's emulator.
type scopeState struct {
	fields map[string]*fieldRecord
}

func newScopeState() scopeState {
	return scopeState{fields: map[string]*fieldRecord{}}
}

func (s *scopeState) reset() {
	s.fields = map[string]*fieldRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("firestore requires registry, manager, index, and iam")

// streamedREST lists the REST methods whose responses are JSON arrays of
// stream messages.
var streamedREST = map[string]bool{methodRunQuery: true, methodRunAggregation: true, methodBatchGet: true}

// wireError builds the Firestore error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// scopeOf names one project scope. Databases are project-scoped here; the
// real (default) database lives in the project namespace.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// databaseResource names the default database in the IAM index.
func databaseResource(project string) string {
	return "//firestore.googleapis.com/projects/" + project + "/databases/(default)"
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}
