package firestore

import (
	"errors"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the Firestore backend registration.
const serviceName = "firestore"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodDocumentsGet    = "firestore.documents.get"
	methodDocumentsList   = "firestore.documents.list"
	methodFieldsGet       = "firestore.fields.get"
	methodFieldsUpdate    = "firestore.fields.update"
	methodDocumentsCreate = "firestore.documents.create"
	methodDocumentsUpdate = "firestore.documents.update"
	methodDocumentsDelete = "firestore.documents.delete"
)

// permissions follows the documented Cloud Firestore access-control rows.
// Firestore permission names are the Cloud Datastore ones.
var permissions = map[string]string{
	methodDocumentsGet:    "datastore.entities.get",
	methodDocumentsList:   "datastore.entities.list",
	methodDocumentsCreate: "datastore.entities.create",
	methodDocumentsUpdate: "datastore.entities.update",
	methodDocumentsDelete: "datastore.entities.delete",
	methodFieldsGet:       "datastore.schemas.get",
	methodFieldsUpdate:    "datastore.schemas.update",
}

// Resource-name helpers follow the real Firestore namespace.
const (
	kindDocument = "document"
	uidPattern   = `^[^/]{1,1000}$`
)

// uid is the shared resource-name matcher.
var uid = regexp.MustCompile(uidPattern)

// documentRecord is one stored Firestore document.
type documentRecord struct {
	Collection string
	ID         string
	Fields     map[string]any
	CreateTime string
	UpdateTime string
}

// fieldRecord is one configured collection-group field.
type fieldRecord struct {
	Name        string
	TTLConfig   map[string]any
	IndexConfig map[string]any
	Configured  bool
	CreatedAt   time.Time
}

// scopeState is one project slice of provider state.
type scopeState struct {
	documents map[string]*documentRecord
	fields    map[string]*fieldRecord
}

func newScopeState() scopeState {
	return scopeState{documents: map[string]*documentRecord{}, fields: map[string]*fieldRecord{}}
}

func (s *scopeState) reset() {
	s.documents = map[string]*documentRecord{}
	s.fields = map[string]*fieldRecord{}
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("firestore requires registry, manager, index, and iam")

// wireError builds the Firestore error body.
func wireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// notFound reports the Firestore not-found status.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// scopeOf names one project scope. Databases are project-scoped here; the
// real (default) database lives in the project namespace.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// documentResource names one document in the IAM index.
func documentResource(project, collection, id string) string {
	return "//firestore.googleapis.com/" + documentName(project, collection, id)
}

// databaseResource names the default database in the IAM index.
func databaseResource(project string) string {
	return "//firestore.googleapis.com/projects/" + project + "/databases/(default)"
}

// projectResource names the resource-manager project ancestor.
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// documentName renders the canonical document name.
func documentName(project, collection, id string) string {
	return "projects/" + project + "/databases/(default)/documents/" + collection + "/" + id
}

// stamp renders one RFC3339 timestamp.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
