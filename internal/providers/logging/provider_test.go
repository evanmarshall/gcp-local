package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Test names and wire keys repeated across the tests.
const (
	projectA      = "logs-a"
	projectB      = "logs-b"
	typeGlobal    = "global"
	labelEnv      = "env"
	textDisk      = "disk almost full"
	textFailed    = "request failed: timeout"
	textStarted   = "service started"
	orderDesc     = "timestamp desc"
	appLog        = "projects/" + projectA + "/logs/app"
	auditLog      = "projects/" + projectA + "/logs/cloudaudit.googleapis.com%2Factivity"
	grpcLog       = "projects/" + projectA + "/logs/grpc"
	auditPayload  = "json:" + auditLog
	actorWriter   = "writer"
	actorViewer   = "viewer"
	actorAdmin    = "admin"
	projectAName  = "projects/" + projectA
	projectBName  = "projects/" + projectB
	severityInfo  = "INFO"
	projectBOwner = "other"
)

type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	provider *Provider
	engine   *iam.Engine
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	member := func(actor string) []string {
		return []string{"serviceAccount:" + actor + "@" + projectA + ".iam.gserviceaccount.com"}
	}
	grants := map[string][]iam.Binding{
		projectA: {
			{Role: "roles/logging.logWriter", Members: member(actorWriter)},
			{Role: "roles/logging.viewer", Members: member(actorViewer)},
			{Role: "roles/logging.admin", Members: member(actorAdmin)},
		},
		projectB: {{Role: "roles/logging.admin", Members: member(projectBOwner)}},
	}
	for i, project := range []string{projectA, projectB} {
		name := provider.ProjectResource(project)
		if err := index.Put("resourcemanager", state.Resource{Name: name, Service: "resourcemanager", Kind: "project", Number: []string{"111", "222"}[i], Scope: provider.ProjectScope(project)}); err != nil {
			t.Fatalf("register the project: %v", err)
		}
		if _, err := engine.SetPolicy(name, iam.Policy{Bindings: grants[project]}); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	provider, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	provider.clock = func() time.Time { return time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC) }
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + projectA + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, provider: provider, engine: engine}
}

func (h *harness) call(verb, path string, body any, actor string) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatal(err)
		}
	}
	request := httptest.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if actor != "" {
		request.Header.Set("Authorization", "Bearer "+actor)
	}
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

func (h *harness) write(actor string, body map[string]any) int {
	return h.call(http.MethodPost, "/v2/entries:write", body, actor).Code
}

func (h *harness) list(actor string, body map[string]any) (map[string]any, int) {
	h.t.Helper()
	recorder := h.call(http.MethodPost, "/v2/entries:list", body, actor)
	var value map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		h.t.Fatalf("decode %q: %v", recorder.Body, err)
	}
	return value, recorder.Code
}

// listA lists project A with extra request fields.
func (h *harness) listA(actor string, extra map[string]any) (map[string]any, int) {
	body := map[string]any{"resourceNames": []any{projectAName}}
	maps.Copy(body, extra)
	return h.list(actor, body)
}

// global is the global monitored resource.
func global() map[string]any { return map[string]any{"type": typeGlobal} }

func appEntry(severity, text, timestamp string) map[string]any {
	entry := map[string]any{"logName": appLog, "severity": severity, "textPayload": text}
	if timestamp != "" {
		entry["timestamp"] = timestamp
	}
	return entry
}

// seed writes a fixed set of entries as the writer.
func (h *harness) seed() {
	h.t.Helper()
	code := h.write(actorWriter, map[string]any{
		"resource": global(),
		"labels":   map[string]any{labelEnv: "test"},
		"entries": []any{
			appEntry(severityInfo, textStarted, "2026-10-11T10:00:00Z"),
			appEntry("WARNING", textDisk, "2026-10-11T10:05:00Z"),
			appEntry("ERROR", textFailed, "2026-10-11T10:10:00Z"),
			map[string]any{"logName": auditLog, "severity": "NOTICE", "timestamp": "2026-10-11T10:15:00Z",
				"resource":    map[string]any{"type": "gce_instance", "labels": map[string]any{"zone": "us-central1-a"}},
				"jsonPayload": map[string]any{"user": "alice", "status": 503, "nested": map[string]any{"kind": "audit"}}, "labels": map[string]any{labelEnv: "prod"}},
		},
	})
	if code != http.StatusOK {
		h.t.Fatalf("seed: %d", code)
	}
}

// entriesOf reads a listing's entries as objects.
func entriesOf(t *testing.T, value map[string]any) []map[string]any {
	t.Helper()
	raw, _ := value["entries"].([]any)
	entries := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("entry %v is not an object", item)
		}
		entries = append(entries, entry)
	}
	return entries
}

// payloads names each listed entry by its text, or its log for JSON entries.
func payloads(t *testing.T, value map[string]any) []string {
	t.Helper()
	var texts []string
	for _, entry := range entriesOf(t, value) {
		if text, ok := entry["textPayload"].(string); ok {
			texts = append(texts, text)
			continue
		}
		name, _ := entry["logName"].(string)
		texts = append(texts, "json:"+name)
	}
	return texts
}

func TestPermissionPlans(t *testing.T) {
	h := newHarness(t)
	want := map[string]string{
		restWrite: permissionWrite, rpcWrite: permissionWrite,
		restList: permissionList, rpcList: permissionList,
		restLogsList: permissionLogsList, rpcLogsList: permissionLogsList,
		restLogsDelete: permissionLogsDelete, rpcLogsDelete: permissionLogsDelete,
	}
	plans := h.engine.Plans("logging.")
	maps.Copy(plans, h.engine.Plans("/google.logging.v2."))
	if len(plans) != len(want) {
		t.Fatalf("plans = %v", plans)
	}
	for method, permission := range want {
		if !reflect.DeepEqual(plans[method].Baseline, []string{permission}) {
			t.Fatalf("%s = %v", method, plans[method])
		}
	}
}

func TestDeniedWritesStoreNothing(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{"resource": global(), "entries": []any{appEntry(severityInfo, "x", "")}}
	if code := h.write(actorViewer, body); code != http.StatusForbidden {
		t.Fatalf("viewer write: %d", code)
	}
	if code := h.write("", body); code != http.StatusUnauthorized {
		t.Fatalf("anonymous write: %d", code)
	}
	// One entry in a project the caller cannot write rejects the whole request.
	mixed := map[string]any{"resource": global(), "entries": []any{
		appEntry(severityInfo, "a", ""), map[string]any{"logName": projectBName + "/logs/app", "textPayload": "b"},
	}}
	if code := h.write(actorWriter, mixed); code != http.StatusForbidden {
		t.Fatalf("cross-project write: %d", code)
	}
	if value, code := h.listA(actorAdmin, nil); code != http.StatusOK || len(payloads(t, value)) != 0 {
		t.Fatalf("denied writes stored entries: %d %v", code, value)
	}
}

func TestReadAndDeleteNeedTheirPermissions(t *testing.T) {
	h := newHarness(t)
	if _, code := h.listA(actorWriter, nil); code != http.StatusForbidden {
		t.Fatalf("writer list: %d", code)
	}
	if denied := h.call(http.MethodGet, "/v2/"+projectAName+"/logs", nil, actorWriter); denied.Code != http.StatusForbidden {
		t.Fatalf("writer logs.list: %d", denied.Code)
	}
	if denied := h.call(http.MethodDelete, "/v2/"+appLog, nil, actorViewer); denied.Code != http.StatusForbidden {
		t.Fatalf("viewer delete: %d", denied.Code)
	}
}

func TestWriteDefaults(t *testing.T) {
	h := newHarness(t)
	h.seed()
	value, _ := h.listA(actorViewer, map[string]any{"filter": `logName="` + appLog + `"`})
	entries := entriesOf(t, value)
	if len(entries) != 3 || entries[0]["receiveTimestamp"] != "2026-10-11T12:00:00Z" || entries[0]["insertId"] == "" {
		t.Fatalf("entries = %v", entries)
	}
	if !reflect.DeepEqual(entries[0]["labels"], map[string]any{labelEnv: "test"}) || !reflect.DeepEqual(entries[0]["resource"], map[string]any{"type": "global", "labels": map[string]any{"project_id": projectA}}) {
		t.Fatalf("defaults = %v", entries[0])
	}
	if code := h.write(actorWriter, map[string]any{"entries": []any{appEntry(severityInfo, "x", "")}}); code != http.StatusBadRequest {
		t.Fatalf("missing resource: %d", code)
	}
}

func TestProjectsAreIsolated(t *testing.T) {
	h := newHarness(t)
	h.seed()
	if code := h.write(projectBOwner, map[string]any{"logName": projectBName + "/logs/app", "resource": global(), "entries": []any{map[string]any{"textPayload": "from b"}}}); code != http.StatusOK {
		t.Fatalf("project B write: %d", code)
	}
	if _, code := h.list(actorViewer, map[string]any{"resourceNames": []any{projectBName}}); code != http.StatusForbidden {
		t.Fatalf("project A viewer read project B: %d", code)
	}
	if value, _ := h.list(projectBOwner, map[string]any{"resourceNames": []any{projectBName}}); !reflect.DeepEqual(payloads(t, value), []string{"from b"}) {
		t.Fatalf("project B entries = %v", payloads(t, value))
	}
}

func TestFilters(t *testing.T) {
	h := newHarness(t)
	h.seed()
	cases := []struct {
		filter string
		want   []string
	}{
		{"", []string{textStarted, textDisk, textFailed, auditPayload}},
		{"severity>=WARNING", []string{textDisk, textFailed}},
		{"severity>=notice", []string{textDisk, textFailed, auditPayload}},
		{"severity=ERROR", []string{textFailed}},
		{`logName="` + auditLog + `"`, []string{auditPayload}},
		{`logName="projects/logs-a/logs/cloudaudit.googleapis.com/activity"`, []string{auditPayload}},
		{"resource.type=gce_instance", []string{auditPayload}},
		{`resource.labels.zone="us-central1-a"`, []string{auditPayload}},
		{`timestamp>="2026-10-11T10:05:00Z" timestamp<"2026-10-11T10:15:00Z"`, []string{textDisk, textFailed}},
		{`timestamp>="2026-10-11T10:05:00.000000+0000"`, []string{textDisk, textFailed, auditPayload}},
		{"timeout", []string{textFailed}},
		{`"DISK almost"`, []string{textDisk}},
		{"alice", []string{auditPayload}},
		{"labels.env=prod", []string{auditPayload}},
		{"jsonPayload.status>=500 AND jsonPayload.nested.kind=audit", []string{auditPayload}},
		{`textPayload:"FAILED"`, []string{textFailed}},
		{`textPayload=~"^disk"`, []string{textDisk}},
		{`severity=~"^WARN"`, []string{textDisk}},
		{"NOT severity=INFO AND -severity=NOTICE", []string{textDisk, textFailed}},
		{"severity=INFO OR (severity=ERROR AND timeout)", []string{textStarted, textFailed}},
		{"labels.missing:*", nil},
	}
	for _, item := range cases {
		value, code := h.listA(actorViewer, map[string]any{"filter": item.filter})
		if code != http.StatusOK || !reflect.DeepEqual(payloads(t, value), item.want) {
			t.Fatalf("filter %q: %d %v, want %v", item.filter, code, payloads(t, value), item.want)
		}
	}
}

// Cloud Logging binds NOT, then OR, then AND: a AND b OR c is a AND (b OR c)
// (https://cloud.google.com/logging/docs/view/logging-query-language#boolean_operators).
func TestFilterPrecedence(t *testing.T) {
	h := newHarness(t)
	h.seed()
	cases := []struct {
		filter string
		want   []string
	}{
		{"severity=ERROR AND severity=INFO OR severity=WARNING", nil},
		{"severity=INFO OR severity=ERROR AND timeout", []string{textFailed}},
		{"severity=INFO OR severity=ERROR timeout", []string{textFailed}},
		{"(severity=ERROR AND severity=INFO) OR severity=WARNING", []string{textDisk}},
		{"NOT severity=INFO OR severity=ERROR AND -severity=NOTICE", []string{textDisk, textFailed}},
	}
	for _, item := range cases {
		value, code := h.listA(actorViewer, map[string]any{"filter": item.filter})
		if code != http.StatusOK || !reflect.DeepEqual(payloads(t, value), item.want) {
			t.Fatalf("filter %q: %d %v, want %v", item.filter, code, payloads(t, value), item.want)
		}
	}
}

func TestInvalidRequests(t *testing.T) {
	h := newHarness(t)
	for _, bad := range []map[string]any{
		{"filter": "severity>=LOUD"}, {"filter": "unknownField=1"}, {"filter": `textPayload="open`},
		{"filter": "(severity=INFO"}, {"filter": "timestamp>yesterday"}, {"filter": "severity="},
		{"orderBy": "severity"}, {"pageToken": encodeToken(1, "another request")},
	} {
		if _, code := h.listA(actorViewer, bad); code != http.StatusBadRequest {
			t.Fatalf("request %v accepted: %d", bad, code)
		}
	}
	if _, code := h.list(actorViewer, map[string]any{"resourceNames": []any{"organizations/1"}}); code != http.StatusNotImplemented {
		t.Fatalf("organization listing: %d", code)
	}
}

func TestOrderAndPaging(t *testing.T) {
	h := newHarness(t)
	h.seed()
	value, _ := h.listA(actorViewer, map[string]any{"orderBy": orderDesc, "pageSize": 3})
	if got := payloads(t, value); !reflect.DeepEqual(got, []string{auditPayload, textFailed, textDisk}) || value["nextPageToken"] == nil {
		t.Fatalf("desc page = %v", value)
	}
	next, _ := h.listA(actorViewer, map[string]any{"orderBy": orderDesc, "pageSize": 3, "pageToken": value["nextPageToken"]})
	if got := payloads(t, next); !reflect.DeepEqual(got, []string{textStarted}) || next["nextPageToken"] != nil {
		t.Fatalf("second page = %v", next)
	}
	if _, code := h.listA(actorViewer, map[string]any{"pageToken": value["nextPageToken"]}); code != http.StatusBadRequest {
		t.Fatalf("token reused with another order: %d", code)
	}
}

// logPage reads one logs.list page.
func (h *harness) logPage(query string) ([]any, string) {
	h.t.Helper()
	recorder := h.call(http.MethodGet, "/v2/"+projectAName+"/logs?"+query, nil, actorViewer)
	var page map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil || recorder.Code != http.StatusOK {
		h.t.Fatalf("logs page: %d %v", recorder.Code, err)
	}
	names, _ := page["logNames"].([]any)
	token, _ := page["nextPageToken"].(string)
	return names, token
}

func TestLogsListAndDelete(t *testing.T) {
	h := newHarness(t)
	h.seed()
	names, token := h.logPage("pageSize=1")
	if !reflect.DeepEqual(names, []any{appLog}) || token == "" {
		t.Fatalf("first logs page = %v %q", names, token)
	}
	if names, _ = h.logPage("pageSize=1&pageToken=" + token); !reflect.DeepEqual(names, []any{auditLog}) {
		t.Fatalf("second logs page = %v", names)
	}
	if deleted := h.call(http.MethodDelete, "/v2/"+auditLog, nil, actorAdmin); deleted.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", deleted.Code, deleted.Body)
	}
	if value, _ := h.listA(actorViewer, nil); len(payloads(t, value)) != 3 {
		t.Fatalf("after delete = %v", payloads(t, value))
	}
	if missing := h.call(http.MethodDelete, "/v2/"+auditLog, nil, actorAdmin); missing.Code != http.StatusNotFound {
		t.Fatalf("second delete: %d", missing.Code)
	}
	if err := h.provider.Reset(provider.ProjectScope(projectA)); err != nil {
		t.Fatal(err)
	}
	if value, _ := h.listA(actorViewer, nil); len(payloads(t, value)) != 0 {
		t.Fatal("reset kept entries")
	}
}

// grpcConnection serves the gateway's gRPC surface on loopback.
func grpcConnection(t *testing.T, h *harness) *grpc.ClientConn {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func as(actor string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+actor)
}

func TestGRPCWriteAuthorization(t *testing.T) {
	client := loggingpb.NewLoggingServiceV2Client(grpcConnection(t, newHarness(t)))
	write := &loggingpb.WriteLogEntriesRequest{LogName: grpcLog, Resource: &monitoredres.MonitoredResource{Type: typeGlobal},
		Entries: []*loggingpb.LogEntry{{Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "over grpc"}}}}
	if _, err := client.WriteLogEntries(as(actorViewer), write); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("viewer write: %v", err)
	}
	if _, err := client.WriteLogEntries(as(actorWriter), write); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := client.ListMonitoredResourceDescriptors(as(actorAdmin), &loggingpb.ListMonitoredResourceDescriptorsRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("descriptor listing: %v", err)
	}
}

func TestGRPCReadAndDelete(t *testing.T) {
	client := loggingpb.NewLoggingServiceV2Client(grpcConnection(t, newHarness(t)))
	write := &loggingpb.WriteLogEntriesRequest{LogName: grpcLog, Resource: &monitoredres.MonitoredResource{Type: typeGlobal},
		Entries: []*loggingpb.LogEntry{{Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "over grpc"}}}}
	if _, err := client.WriteLogEntries(as(actorWriter), write); err != nil {
		t.Fatalf("write: %v", err)
	}
	listed, err := client.ListLogEntries(as(actorViewer), &loggingpb.ListLogEntriesRequest{ResourceNames: []string{projectAName}, Filter: "over"})
	if err != nil || len(listed.GetEntries()) != 1 || listed.GetEntries()[0].GetLogName() != grpcLog {
		t.Fatalf("list: %v %v", listed, err)
	}
	logs, err := client.ListLogs(as(actorViewer), &loggingpb.ListLogsRequest{Parent: projectAName})
	if err != nil || !reflect.DeepEqual(logs.GetLogNames(), []string{grpcLog}) {
		t.Fatalf("logs: %v %v", logs, err)
	}
	if _, err := client.DeleteLog(as(actorAdmin), &loggingpb.DeleteLogRequest{LogName: grpcLog}); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// errorOf reads a REST error's status and message.
func errorOf(t *testing.T, recorder *httptest.ResponseRecorder) (int, string) {
	t.Helper()
	var wire struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &wire); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body, err)
	}
	return recorder.Code, wire.Error.Message
}

// Query-language functions are gaps, never text searches; any other function
// and any field LogEntry lacks are GCP's 400 (recorded).
func TestUnevaluatedFiltersAreRefused(t *testing.T) {
	h := newHarness(t)
	h.seed()
	for _, item := range []struct {
		filter string
		code   int
		want   string
	}{
		{`log_id("app")`, 501, "Logging coverage gap: filter function log_id() is not implemented"},
		{`-log_id("app")`, 501, "filter function log_id() is not implemented"},
		{`sample(insertId, 0.5)`, 501, "filter function sample() is not implemented"},
		{`ip_in_net(jsonPayload.ip, "10.0.0.0/8")`, 501, "filter function ip_in_net() is not implemented"},
		{`SEARCH("disk")`, 501, "filter function SEARCH() is not implemented"},
		{`severity=ERROR OR search(textPayload, "disk")`, 501, "filter function search() is not implemented"},
		{`cast(severity, STRING)="ERROR"`, 501, "filter function cast() is not implemented"},
		{`textPayload=log_id("x")`, 501, "filter function log_id() is not implemented"},
		{`httpRequest.status>=500`, 501, `filter field "httpRequest.status" is not implemented`},
		{`operation.id="x"`, 501, `filter field "operation.id" is not implemented`},
		{`source_location.file="x"`, 501, `filter field "source_location.file" is not implemented`},
		{`protoPayload.methodName="x"`, 501, `filter field "protoPayload.methodName" is not implemented`},
		{`nosuchfn("disk")`, 400, "Unsupported global function: 'nosuchfn'."},
		{`notAField="x"`, 400, "Field not found: 'notAField'."},
		{`resource.notAField="x"`, 400, "Field not found: 'resource.notAField'."},
		{`severity.level="x"`, 400, "Field not found: 'severity.level'."},
	} {
		code, message := errorOf(t, h.call(http.MethodPost, "/v2/entries:list", map[string]any{
			"resourceNames": []any{projectAName}, "filter": item.filter}, actorViewer))
		if code != item.code || !strings.Contains(message, item.want) {
			t.Fatalf("filter %q: %d %q, want %d %q", item.filter, code, message, item.code, item.want)
		}
	}
}

// Request fields the provider doesn't model are gaps; unknown names are
// GCP's transcoder 400 (recorded); refusals store nothing.
func TestUnmodeledWritesAreRefused(t *testing.T) {
	h := newHarness(t)
	for _, item := range []struct {
		body map[string]any
		code int
		want string
	}{
		{map[string]any{"partialSuccess": true, "entries": []any{appEntry(severityInfo, "a", "")}}, 501, "Logging coverage gap: partialSuccess with an invalid entry is not modeled"},
		{map[string]any{"resource": global(), "entries": []any{map[string]any{"logName": appLog, "protoPayload": map[string]any{"@type": "type.googleapis.com/google.cloud.audit.AuditLog"}}}}, 501, "entries.protoPayload is not modeled"},
		{map[string]any{"resource": global(), "entries": []any{map[string]any{"logName": appLog, "textPayload": "x", "notAField": 1}}}, 400, `Invalid JSON payload received. Unknown name "notAField" at 'entries[0]': Cannot find field.`},
		{map[string]any{"notAField": 1}, 400, `Invalid JSON payload received. Unknown name "notAField": Cannot find field.`},
	} {
		code, message := errorOf(t, h.call(http.MethodPost, "/v2/entries:write", item.body, actorWriter))
		if code != item.code || !strings.Contains(message, item.want) {
			t.Fatalf("write %v: %d %q", item.body, code, message)
		}
	}
	client := loggingpb.NewLoggingServiceV2Client(grpcConnection(t, h))
	if _, err := client.WriteLogEntries(as(actorWriter), &loggingpb.WriteLogEntriesRequest{PartialSuccess: true,
		Resource: &monitoredres.MonitoredResource{Type: typeGlobal}, Entries: []*loggingpb.LogEntry{{LogName: grpcLog}, {}}}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("gRPC partialSuccess with an invalid entry: %v", err)
	}
	if value, _ := h.listA(actorAdmin, nil); len(payloads(t, value)) != 0 {
		t.Fatalf("a refused write stored entries: %v", value)
	}
	// The clients' default: partialSuccess with valid entries writes them all.
	if code := h.write(actorWriter, map[string]any{"partialSuccess": true, "resource": global(),
		"entries": []any{appEntry(severityInfo, "a", "")}}); code != http.StatusOK {
		t.Fatalf("partialSuccess write: %d", code)
	}
}

// Denials, page refusals, and order refusals carry GCP's wording (recorded).
func TestRecordedRefusals(t *testing.T) {
	h := newHarness(t)
	h.seed()
	recorder := h.call(http.MethodPost, "/v2/entries:write", map[string]any{"logName": appLog, "resource": global(),
		"entries": []any{map[string]any{"textPayload": "x"}}}, actorViewer)
	if code, message := errorOf(t, recorder); code != 403 || message != "Permission 'logging.logEntries.create' denied on resource '//logging.googleapis.com/"+appLog+"' (or it may not exist)." ||
		!strings.Contains(recorder.Body.String(), `"domain":"iam.googleapis.com"`) || !strings.Contains(recorder.Body.String(), `"resource":"`+appLog+`"`) {
		t.Fatalf("write denial: %d %s", code, recorder.Body)
	}
	if code, message := errorOf(t, h.call(http.MethodDelete, "/v2/"+appLog, nil, actorViewer)); code != 403 || !strings.Contains(message, "'logging.logs.delete' denied on resource '//logging.googleapis.com/"+appLog+"'") {
		t.Fatalf("delete denial: %d %q", code, message)
	}
	for _, item := range []struct {
		code     int
		recorder *httptest.ResponseRecorder
		want     string
	}{
		{403, h.call(http.MethodPost, "/v2/entries:list", map[string]any{"resourceNames": []any{projectAName}}, actorWriter), "Permission denied for all log views"},
		{403, h.call(http.MethodGet, "/v2/"+projectAName+"/logs", nil, actorWriter), "Permission denied for all log views"},
		{400, h.call(http.MethodPost, "/v2/entries:list", map[string]any{"resourceNames": []any{projectAName}, "pageToken": "bogus"}, actorViewer), "Invalid page_token"},
		{400, h.call(http.MethodPost, "/v2/entries:list", map[string]any{"resourceNames": []any{projectAName}, "pageSize": -1}, actorViewer), "page_size must not be negative"},
		{400, h.call(http.MethodPost, "/v2/entries:list", map[string]any{"resourceNames": []any{projectAName}, "orderBy": "insertId desc"}, actorViewer), "order_by must be on timestamp"},
		{400, h.call(http.MethodGet, "/v2/"+projectAName+"/logs?pageToken=bogus", nil, actorViewer), "Invalid page_token."},
	} {
		if code, message := errorOf(t, item.recorder); code != item.code || message != item.want {
			t.Fatalf("got %d %q, want %d %q", code, message, item.code, item.want)
		}
	}
}

// Sinks, exclusions, buckets, views, and metrics aren't served: their routes
// are explicit gaps, so nothing a write sends is silently unrouted.
func TestConfigurationMethodsAreGaps(t *testing.T) {
	h := newHarness(t)
	for _, path := range []string{"/v2/" + projectAName + "/sinks", "/v2/" + projectAName + "/exclusions",
		"/v2/" + projectAName + "/locations/global/buckets", "/v2/" + projectAName + "/metrics"} {
		if code, _ := errorOf(t, h.call(http.MethodGet, path, nil, actorAdmin)); code != http.StatusNotImplemented {
			t.Fatalf("%s: %d", path, code)
		}
	}
}

// A project-level condition on resource.service grants Logging methods, as
// in GCP; resource.type and resource.name aren't supplied, so a condition
// on them grants nothing (recorded: logging-conditional-access).
func TestConditionsSeeTheServiceOnly(t *testing.T) {
	h := newHarness(t)
	h.seed()
	reader := "serviceAccount:conditional@" + projectA + ".iam.gserviceaccount.com"
	grant := func(expression string) {
		name := provider.ProjectResource(projectA)
		policy, err := h.engine.GetPolicy(name)
		if err != nil {
			t.Fatal(err)
		}
		condition, err := json.Marshal(map[string]string{"title": "c", "expression": expression})
		if err != nil {
			t.Fatal(err)
		}
		policy.Version = 3
		policy.Bindings = append(policy.Bindings[:3:3], iam.Binding{Role: "roles/logging.viewer", Members: []string{reader}, Condition: condition})
		if _, err := h.engine.SetPolicy(name, policy); err != nil {
			t.Fatal(err)
		}
	}
	grant(`resource.service == "logging.googleapis.com"`)
	if _, code := h.listA("conditional", nil); code != http.StatusOK {
		t.Fatalf("service condition: %d", code)
	}
	grant(`resource.type == "logging.googleapis.com/LogEntry"`)
	if _, code := h.listA("conditional", nil); code != http.StatusForbidden {
		t.Fatalf("type condition: %d", code)
	}
}
