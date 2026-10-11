package spanner

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Wire keys and statements repeated across the tests.
const (
	keyConfig          = "config"
	keyCreateStatement = "createStatement"
	keyCreateTime      = "createTime"
	keyState           = "state"
	createGlDB         = "CREATE DATABASE `gl_db`"
)

// Scenario names mirroring gcp_validation/spanner.py run_scenario.
const (
	recordedInstance = "gl-recorded01"
	recordedPath     = testInstances + "/" + recordedInstance
	recordedDatabase = recordedPath + "/databases/gl_db"
	allowedToken     = "allowed"
)

// decode parses one JSON response body.
func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return body
}

// object asserts one JSON object value.
func object(t *testing.T, value any) map[string]any {
	t.Helper()
	document, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value %v is not a JSON object", value)
	}
	return document
}

// errorStatus returns the error.status of one error response.
func errorStatus(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	text, _ := object(t, decode(t, recorder)["error"])["status"].(string)
	return text
}

// fields returns the sorted top-level field names of a document.
func fields(document map[string]any) []string {
	names := make([]string, 0, len(document))
	for name := range document {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// expectOperation compares an LRO with the recorded operation() observation.
func expectOperation(t *testing.T, recorder *httptest.ResponseRecorder, metadataType, responseType string, responseFields []string) map[string]any {
	t.Helper()
	if recorder.Code != 200 {
		t.Fatalf("operation status = %d body %s", recorder.Code, recorder.Body.String())
	}
	operation := decode(t, recorder)
	if got := fields(operation); !reflect.DeepEqual(got, []string{"done", "metadata", keyName, "response"}) {
		t.Fatalf("operation fields = %v", got)
	}
	if operation["done"] != true || object(t, operation["metadata"])[keyType] != metadataType {
		t.Fatalf("operation = %v", operation)
	}
	response := object(t, operation["response"])
	if response[keyType] != responseType {
		t.Fatalf("response type = %v", response[keyType])
	}
	if got := fields(response); !reflect.DeepEqual(got, responseFields) {
		t.Fatalf("response fields = %v", got)
	}
	return operation
}

// recordedInstanceBody is the scenario's create_instance request.
func recordedInstanceBody() map[string]any {
	return map[string]any{"instanceId": recordedInstance, "instance": map[string]any{
		keyConfig:      "projects/" + testProject + "/instanceConfigs/regional-us-central1",
		keyDisplayName: "gcp-local conformance", "processingUnits": 100,
		"labels": map[string]any{"managed-by": "gcp-local", "gcp-local-run": "run"},
	}}
}

// recordedInstanceFields is the recorded instance document field set.
var recordedInstanceFields = []string{
	keyConfig, keyCreateTime, "defaultBackupScheduleType", keyDisplayName, "edition", "instanceType",
	"labels", keyName, "processingUnits", "replicaComputeCapacity", "resourceLocation", keyState, "updateTime",
}

func TestRecordedInstanceShapes(t *testing.T) {
	h := newHarness(t)
	operation := expectOperation(t, h.invoke(http.MethodPost, testInstances, recordedInstanceBody()),
		typeCreateInstanceMetadata, typeInstance, append([]string{keyType}, recordedInstanceFields...))
	if name, _ := operation[keyName].(string); !strings.HasPrefix(name, "projects/"+testProject+"/instances/"+recordedInstance+"/operations/") {
		t.Fatalf("operation name = %s", name)
	}
	recorder := h.invoke(http.MethodGet, recordedPath, nil)
	if recorder.Code != 200 {
		t.Fatalf("instance get status = %d", recorder.Code)
	}
	instance := decode(t, recorder)
	if got := fields(instance); !reflect.DeepEqual(got, recordedInstanceFields) {
		t.Fatalf("instance fields = %v", got)
	}
	if instance[keyConfig] != "projects/"+testProject+"/instanceConfigs/regional-us-central1" ||
		instance["processingUnits"] != float64(100) || instance[keyState] != "READY" ||
		instance[keyName] != "projects/"+testProject+"/instances/"+recordedInstance {
		t.Fatalf("instance = %v", instance)
	}
	duplicate := h.invoke(http.MethodPost, testInstances, recordedInstanceBody())
	if duplicate.Code != 409 || errorStatus(t, duplicate) != "ALREADY_EXISTS" {
		t.Fatalf("duplicate status = %d body %s", duplicate.Code, duplicate.Body.String())
	}
}

func TestNodeCountRenderedForWholeNodes(t *testing.T) {
	h := newHarness(t)
	h.instance()
	instance := decode(t, h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil))
	if instance["nodeCount"] != float64(1) || instance["processingUnits"] != float64(1000) {
		t.Fatalf("instance = %v", instance)
	}
}

func TestInstanceScopedViewerGrant(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, testInstances, recordedInstanceBody())
	if code := h.invokeAs(allowedToken, http.MethodGet, recordedPath, nil).Code; code != 403 {
		t.Fatalf("ungranted read status = %d", code)
	}
	recorder := h.invoke(http.MethodPost, recordedPath+":getIamPolicy", map[string]any{})
	policy := decode(t, recorder)
	if recorder.Code != 200 || !reflect.DeepEqual(fields(policy), []string{"etag"}) {
		t.Fatalf("empty policy status = %d body %v", recorder.Code, policy)
	}
	member := "serviceAccount:" + allowedToken + "@" + testProject + ".iam.gserviceaccount.com"
	recorder = h.invoke(http.MethodPost, recordedPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"etag": policy["etag"], "bindings": []any{map[string]any{"role": "roles/spanner.viewer", "members": []string{member}}},
	}})
	if recorder.Code != 200 || !slices.Contains(fields(decode(t, recorder)), "bindings") {
		t.Fatalf("set policy status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if code := h.invokeAs(allowedToken, http.MethodGet, recordedPath, nil).Code; code != 200 {
		t.Fatalf("viewer read status = %d", code)
	}
	recorder = h.invokeAs(allowedToken, http.MethodPost, recordedPath+"/databases",
		map[string]any{keyCreateStatement: "CREATE DATABASE `denied`"})
	if recorder.Code != 403 || errorStatus(t, recorder) != "PERMISSION_DENIED" {
		t.Fatalf("viewer create status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

func TestRecordedDatabaseLifecycle(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, testInstances, recordedInstanceBody())
	expectOperation(t, h.invoke(http.MethodPost, recordedPath+"/databases", map[string]any{
		keyCreateStatement: createGlDB, "extraStatements": []string{recordedTable},
	}), typeCreateDatabaseMetadata, typeDatabase, []string{
		keyType, keyCreateTime, "databaseDialect", "earliestVersionTime", keyName, "resourceLocationId", keyState, "versionRetentionPeriod",
	})
	database := decode(t, h.invoke(http.MethodGet, recordedDatabase, nil))
	if got := fields(database); !reflect.DeepEqual(got, []string{
		keyCreateTime, "databaseDialect", "earliestVersionTime", "encryptionInfo", keyName, keyState, "versionRetentionPeriod",
	}) {
		t.Fatalf("database fields = %v", got)
	}
	if database["databaseDialect"] != "GOOGLE_STANDARD_SQL" || database[keyState] != "READY" ||
		database[keyName] != "projects/"+testProject+"/instances/"+recordedInstance+"/databases/gl_db" {
		t.Fatalf("database = %v", database)
	}
	expectDDL(t, h, recordedCanonical)
	expectOperation(t, h.invoke(http.MethodPatch, recordedDatabase+"/ddl", map[string]any{"statements": []string{recordedColumn}}),
		typeUpdateDdlMetadata, typeEmpty, []string{keyType})
	expectDDL(t, h, recordedAfterColumn)
	expectEmptyDelete(t, h.invoke(http.MethodDelete, recordedDatabase, nil))
	expectNotFound(t, h.invoke(http.MethodGet, recordedDatabase, nil))
	expectEmptyDelete(t, h.invoke(http.MethodDelete, recordedPath, nil))
	expectNotFound(t, h.invoke(http.MethodGet, recordedPath, nil))
}

func TestUnmodeledDDLIsUnimplemented(t *testing.T) {
	h := newHarness(t)
	h.invoke(http.MethodPost, testInstances, recordedInstanceBody())
	recorder := h.invoke(http.MethodPost, recordedPath+"/databases", map[string]any{
		keyCreateStatement: createGlDB, "extraStatements": []string{"CREATE INDEX ByName ON Items(Name)"},
	})
	if recorder.Code != 501 || strings.Contains(recorder.Body.String(), "this method is not implemented") {
		t.Fatalf("unmodeled DDL status = %d body %s", recorder.Code, recorder.Body.String())
	}
	if code := h.invoke(http.MethodGet, recordedDatabase, nil).Code; code != 404 {
		t.Fatalf("refused database exists: status %d", code)
	}
}

// expectDDL compares getDdl with one recorded canonical statement.
func expectDDL(t *testing.T, h *harness, want string) {
	t.Helper()
	recorder := h.invoke(http.MethodGet, recordedDatabase+"/ddl", nil)
	body := decode(t, recorder)
	if recorder.Code != 200 || !reflect.DeepEqual(body, map[string]any{"statements": []any{want}}) {
		t.Fatalf("getDdl status = %d body %q", recorder.Code, body)
	}
}

// expectEmptyDelete checks the recorded 200 {} delete response.
func expectEmptyDelete(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != 200 || len(decode(t, recorder)) != 0 {
		t.Fatalf("delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
}

// expectNotFound checks the recorded 404 NOT_FOUND read.
func expectNotFound(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != 404 || errorStatus(t, recorder) != "NOT_FOUND" {
		t.Fatalf("absent status = %d body %s", recorder.Code, recorder.Body.String())
	}
}
