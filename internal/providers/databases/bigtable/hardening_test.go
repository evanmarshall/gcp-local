package bigtable

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// expectMessage asserts an error's HTTP code and message.
func expectMessage(t *testing.T, label string, recorder *httptest.ResponseRecorder, code int, message string) map[string]any {
	t.Helper()
	wire, _ := decode(t, recorder)["error"].(map[string]any)
	if recorder.Code != code || wire["message"] != message {
		t.Fatalf("%s = %d %s, want %d %q", label, recorder.Code, recorder.Body.String(), code, message)
	}
	return wire
}

// TestInstancesListByName lists instances by name, as GCP does, and ignores
// a page token, as GCP's ListInstances does (recorded).
func TestInstancesListByName(t *testing.T) {
	h := newHarness(t)
	second := createBody()
	second["instanceId"] = "aa-bt"
	second["clusters"] = map[string]any{"aa-btc1": map[string]any{"location": "projects/" + testProject + "/locations/" + testZone, "serveNodes": 1}}
	h.instance()
	expectOK(t, "second instance", h.invoke(http.MethodPost, testInstances, second))
	for _, query := range []string{"", "?pageToken=not-a-page-token"} {
		listing := decode(t, h.invoke(http.MethodGet, testInstances+query, nil))
		items, _ := listing["instances"].([]any)
		var names []string
		for _, item := range items {
			names = append(names, provider.LastSegment(dig(item, "name")))
		}
		if fields(listing) != "instances" || !reflect.DeepEqual(names, []string{"aa-bt", testInstance}) {
			t.Fatalf("list%s = %v", query, listing)
		}
	}
}

// TestPartialUpdateChangesOnlyMaskedFields follows the recorded
// PartialUpdateInstance: a done operation, omitted fields kept, and an empty
// mask refused.
func TestPartialUpdateChangesOnlyMaskedFields(t *testing.T) {
	h := newHarness(t)
	h.instance()
	path := testInstances + "/" + testInstance
	operation := decode(t, h.invoke(http.MethodPatch, path+"?updateMask=displayName", map[string]any{"displayName": "renamed", "labels": map[string]string{}}))
	response, _ := operation["response"].(map[string]any)
	metadata, _ := operation["metadata"].(map[string]any)
	if operation["done"] != true || metadata["@type"] != typeUpdateInstanceMetadata ||
		fields(response) != "@type,createTime,displayName,edition,knowledgeCatalogRegion,labels,name,type" {
		t.Fatalf("rename operation = %v", operation)
	}
	expectOK(t, "rename poll", h.invoke(http.MethodGet, "/v2/"+dig(operation, "name"), nil))
	instance := decode(t, h.invoke(http.MethodGet, path, nil))
	if instance["displayName"] != "renamed" || !reflect.DeepEqual(instance["labels"], map[string]any{"env": "dev"}) {
		t.Fatalf("renamed instance = %v", instance)
	}
	h.poll(decode(t, h.invoke(http.MethodPatch, path+"?updateMask=labels", map[string]any{"labels": map[string]string{"stage": "patched"}})))
	instance = decode(t, h.invoke(http.MethodGet, path, nil))
	if instance["displayName"] != "renamed" || !reflect.DeepEqual(instance["labels"], map[string]any{"stage": "patched"}) {
		t.Fatalf("relabeled instance = %v", instance)
	}
	expectMessage(t, "no mask", h.invoke(http.MethodPatch, path, map[string]any{"displayName": "x"}), 501,
		"A non-empty 'update_mask' must be specified. An empty 'update_mask' is currently not supported.")
	expectMessage(t, "edition mask", h.invoke(http.MethodPatch, path+"?updateMask=edition", map[string]any{"edition": "ENTERPRISE_PLUS"}), 501,
		"Bigtable coverage gap: instance update of edition")
	expectMessage(t, "tags", h.invoke(http.MethodPatch, path+"?updateMask=labels", map[string]any{"tags": map[string]string{"k": "v"}}), 501,
		"Bigtable coverage gap: tags is not modeled")
}

// TestCreateRefusesUnmodeledFields refuses autoscaling, CMEK, node scaling,
// tags, and replication by name, and an unknown name as GCP does (recorded).
func TestCreateRefusesUnmodeledFields(t *testing.T) {
	h := newHarness(t)
	cluster := func(extra map[string]any) map[string]any {
		body := createBody()
		spec := map[string]any{"location": "projects/" + testProject + "/locations/" + testZone, "serveNodes": 1}
		maps.Copy(spec, extra)
		body["clusters"] = map[string]any{testCluster: spec}
		return body
	}
	for field, body := range map[string]map[string]any{
		"clusters.clusterConfig": cluster(map[string]any{"clusterConfig": map[string]any{"clusterAutoscalingConfig": map[string]any{
			"autoscalingLimits": map[string]any{"minServeNodes": 1, "maxServeNodes": 3}}}}),
		"clusters.encryptionConfig":  cluster(map[string]any{"encryptionConfig": map[string]any{"kmsKeyName": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}}),
		"clusters.nodeScalingFactor": cluster(map[string]any{"nodeScalingFactor": "NODE_SCALING_FACTOR_2X"}),
	} {
		expectMessage(t, field, h.invoke(http.MethodPost, testInstances, body), 501, "Bigtable coverage gap: "+field+" is not modeled")
	}
	tagged := createBody()
	tagged["instance"] = map[string]any{"displayName": "demo", "tags": map[string]string{"k": "v"}}
	expectMessage(t, "tags", h.invoke(http.MethodPost, testInstances, tagged), 501, "Bigtable coverage gap: instance.tags is not modeled")
	replicated := createBody()
	replicated["clusters"] = map[string]any{
		testCluster: map[string]any{"location": "projects/" + testProject + "/locations/" + testZone, "serveNodes": 1},
		"second-c1": map[string]any{"location": "projects/" + testProject + "/locations/us-east1-b", "serveNodes": 1},
	}
	expectMessage(t, "replication", h.invoke(http.MethodPost, testInstances, replicated), 501,
		"Bigtable coverage gap: instances with other than one cluster; replication is not modeled")
	unknown := createBody()
	unknown["bogus"] = 1
	wire := expectMessage(t, "unknown name", h.invoke(http.MethodPost, testInstances, unknown), 400,
		`Invalid JSON payload received. Unknown name "bogus": Cannot find field.`)
	if details, _ := wire["details"].([]any); len(details) != 1 {
		t.Fatalf("unknown name details = %v", wire)
	}
	expectError(t, "nothing created", h.invoke(http.MethodGet, testInstances+"/"+testInstance, nil), 404, "NOT_FOUND")
}

// TestTableListPagingAndRefusals pages tables by name and refuses bad page
// parameters in GCP's words (recorded).
func TestTableListPagingAndRefusals(t *testing.T) {
	h := newHarness(t)
	h.instance()
	h.createTable(testProject, "t1")
	h.createTable(testProject, "t0")
	tables := testInstances + "/" + testInstance + "/tables"
	listing := decode(t, h.invoke(http.MethodGet, tables, nil))
	items, _ := listing["tables"].([]any)
	if fields(listing) != "tables" || len(items) != 2 || !strings.HasSuffix(dig(items, 0, "name"), "/t0") {
		t.Fatalf("tables = %v", listing)
	}
	first := decode(t, h.invoke(http.MethodGet, tables+"?pageSize=1", nil))
	second := decode(t, h.invoke(http.MethodGet, tables+"?pageSize=1&pageToken="+dig(first, "nextPageToken"), nil))
	if page, _ := second["tables"].([]any); len(page) != 1 || second["nextPageToken"] != nil ||
		!strings.HasSuffix(dig(page, 0, "name"), "/t1") {
		t.Fatalf("second page = %v", second)
	}
	expectMessage(t, "bad token", h.invoke(http.MethodGet, tables+"?pageToken=not-a-page-token", nil), 400, "Invalid token")
	expectMessage(t, "negative size", h.invoke(http.MethodGet, tables+"?pageSize=-1", nil), 400, "Invalid request: Page size cannot be negative")
	expectMessage(t, "list view", h.invoke(http.MethodGet, tables+"?view=FULL", nil), 501, "Bigtable coverage gap: table list view FULL")
	expectMessage(t, "get view", h.invoke(http.MethodGet, tables+"/t1?view=REPLICATION_VIEW", nil), 501, "Bigtable coverage gap: table view REPLICATION_VIEW")
}

// TestTableRefusalsInGCPWords maps the emulator's table errors to GCP's
// (recorded) and refuses unmodeled table fields and update paths.
func TestTableRefusalsInGCPWords(t *testing.T) {
	h := newHarness(t)
	h.instance()
	h.createTable(testProject, testTable)
	tables := testInstances + "/" + testInstance + "/tables"
	name := tableName(testProject, testInstance, testTable)
	modify := tables + "/" + testTable + ":modifyColumnFamilies"
	expectMessage(t, "drop unknown", h.invoke(http.MethodPost, modify, map[string]any{"modifications": []any{map[string]any{"id": "nope", "drop": true}}}),
		404, "Cannot drop non-existent family nope")
	expectMessage(t, "update unknown", h.invoke(http.MethodPost, modify, map[string]any{"modifications": []any{map[string]any{"id": "nope", "update": map[string]any{}}}}),
		404, "Cannot update non-existent family nope")
	expectMessage(t, "duplicate", h.invoke(http.MethodPost, tables, map[string]any{"tableId": testTable, "table": map[string]any{}}),
		409, "Table already exists: "+name+" : Table "+testTable+" already exists.")
	expectMessage(t, "missing", h.invoke(http.MethodGet, tables+"/absent", nil), 404, "Not found: "+tableName(testProject, testInstance, "absent"))
	expectMessage(t, "change streams", h.invoke(http.MethodPost, tables, map[string]any{"tableId": "cs", "table": map[string]any{
		"changeStreamConfig": map[string]any{"retentionPeriod": "86400s"}}}), 501, "Bigtable coverage gap: table.changeStreamConfig is not modeled")
	expectMessage(t, "update path", h.invoke(http.MethodPatch, tables+"/"+testTable+"?updateMask=changeStreamConfig", map[string]any{}),
		501, `Bigtable coverage gap: table update of "changeStreamConfig"`)
	h.poll(decode(t, h.invoke(http.MethodPatch, tables+"/"+testTable+"?updateMask=deletionProtection", map[string]any{"deletionProtection": true})))
	expectMessage(t, "protected delete", h.invoke(http.MethodDelete, tables+"/"+testTable, nil), 400,
		"Unable to delete table "+name+" because the deletion protection field is set to true.")
}

// TestOperationsArePolledAndManaged polls a create operation that starts
// running, and reads, lists, cancels, and deletes operations by name.
func TestOperationsArePolledAndManaged(t *testing.T) {
	h := newHarness(t)
	started := h.instance()
	if _, done := started["done"]; done {
		t.Fatalf("a create starts running: %v", started)
	}
	name := dig(h.poll(started), "name")
	wrong := strings.Replace(name, "/locations/"+testZone+"/", "/locations/us-east1-b/", 1)
	expectMessage(t, "other zone", h.invoke(http.MethodGet, "/v2/"+wrong, nil), 404, "Operation "+wrong+" not found.")
	listing := decode(t, h.invoke(http.MethodGet, "/v2/operations/projects/"+testProject+"/operations", nil))
	if items, _ := listing["operations"].([]any); len(items) != 1 || dig(items, 0, "name") != name {
		t.Fatalf("operations = %v", listing)
	}
	expectEmpty(t, "cancel finished", h.invoke(http.MethodPost, "/v2/"+name+":cancel", nil))
	expectOK(t, "cancelled read", h.invoke(http.MethodGet, "/v2/"+name, nil))
	expectEmpty(t, "delete", h.invoke(http.MethodDelete, "/v2/"+name, nil))
	expectError(t, "deleted", h.invoke(http.MethodGet, "/v2/"+name, nil), 404, "NOT_FOUND")
	expectError(t, "denied poll", h.invokeAs("denied", http.MethodGet, "/v2/"+name, nil), 403, "PERMISSION_DENIED")
}

// TestAdminRefusalsInGCPWords follows the recorded denial, which names the
// permission only, and the 401 for a call without credentials.
func TestAdminRefusalsInGCPWords(t *testing.T) {
	h := newHarness(t)
	h.instance()
	path := testInstances + "/" + testInstance
	wire := expectMessage(t, "denied", h.invokeAs("denied", http.MethodGet, path, nil), 403, "Access denied. Missing IAM permission: bigtable.instances.get.")
	if wire["details"] != nil {
		t.Fatalf("a denial carries no details: %v", wire)
	}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	wire = expectMessage(t, "anonymous", recorder, 401, "Request is missing required authentication credential. Expected OAuth 2 access token,"+
		" login cookie or other valid authentication credential. See https://developers.google.com/identity/sign-in/web/devconsole-project.")
	details, _ := wire["details"].([]any)
	if len(details) != 1 || !reflect.DeepEqual(dig(details, 0, "metadata"), fmt.Sprint(map[string]any{
		"method": "google.bigtable.admin.v2.BigtableInstanceAdmin.GetInstance", "service": "bigtableadmin.googleapis.com"})) {
		t.Fatalf("anonymous details = %v", wire)
	}
	expectMessage(t, "missing instance", h.invoke(http.MethodGet, testInstances+"/absent-bt", nil), 404, "Instance projects/"+testProject+"/instances/absent-bt not found.")
	expectMessage(t, "duplicate", h.invoke(http.MethodPost, testInstances, createBody()), 409, "Instance "+testInstance+" already exists.")
}

// TestInstanceConditionNamesTheAddressedResource grants viewer on the
// instance under a condition on resource.type, service, and name, as
// recorded: the instance reads, its table does not.
func TestInstanceConditionNamesTheAddressedResource(t *testing.T) {
	h := newHarness(t)
	h.instance()
	h.createTable(testProject, testTable)
	path := testInstances + "/" + testInstance
	expression := `resource.type == "bigtableadmin.googleapis.com/Instance" && resource.service == "bigtableadmin.googleapis.com"` +
		` && resource.name == "projects/` + testProject + `/instances/` + testInstance + `"`
	granted := decode(t, h.invoke(http.MethodPost, path+":setIamPolicy", map[string]any{"policy": map[string]any{"version": 3, "bindings": []any{map[string]any{
		"role": roleViewer, "members": []string{"serviceAccount:allowed@" + testProject + ".iam.gserviceaccount.com"},
		"condition": map[string]any{"title": "instance-only", "expression": expression}}}}}))
	if granted["version"] != float64(3) {
		t.Fatalf("granted = %v", granted)
	}
	expectOK(t, "instance read", h.invokeAs("allowed", http.MethodGet, path, nil))
	expectMessage(t, "table read", h.invokeAs("allowed", http.MethodGet, path+"/tables/"+testTable, nil), 403, "Access denied. Missing IAM permission: bigtable.tables.get.")
	// A missing table authorizes at its instance, whose attributes the
	// condition then sees (Gate.Attributes describes the chosen target); not
	// recorded.
	expectMessage(t, "missing table", h.invokeAs("allowed", http.MethodGet, path+"/tables/absent", nil), 404,
		"Not found: "+tableName(testProject, testInstance, "absent"))
}

// TestDataPlaneServesOnlyTheDefaultAppProfile refuses a named app profile
// before any backend call instead of ignoring it.
func TestDataPlaneServesOnlyTheDefaultAppProfile(t *testing.T) {
	h := newHarness(t)
	d := serveData(t, h)
	expectOK(t, "instance", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances, createBody()))
	expectOK(t, "table", h.invokeAs(d.tokens[testActor], http.MethodPost, testInstances+"/"+testInstance+"/tables", map[string]any{"tableId": testTable}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := bigtablepb.NewBigtableClient(d.conn(t, testActor))
	name := tableName(testProject, testInstance, testTable)
	before := h.backendCalls.Load()
	err := drain(client.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: name, AppProfileId: "batch"}))
	if status.Code(err) != codes.Unimplemented || !strings.HasSuffix(err.Error(), "desc = Bigtable coverage gap: app profile batch") {
		t.Fatalf("named profile = %v", err)
	}
	if h.backendCalls.Load() != before {
		t.Fatal("a refused profile reached the emulator")
	}
	if err := drain(client.ReadRows(ctx, &bigtablepb.ReadRowsRequest{TableName: name, AppProfileId: "default"})); err != nil {
		t.Fatalf("default profile = %v", err)
	}
}

// dig follows map keys and list indexes into a decoded JSON value and
// renders what it finds, "" when the path is missing.
func dig(value any, path ...any) string {
	for _, step := range path {
		switch key := step.(type) {
		case string:
			object, _ := value.(map[string]any)
			value = object[key]
		case int:
			list, _ := value.([]any)
			if key >= len(list) {
				return ""
			}
			value = list[key]
		}
	}
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
