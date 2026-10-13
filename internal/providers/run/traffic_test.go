package run

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/run/container"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// echoRuntime runs each revision as a loopback server that answers with its
// K_REVISION, so a test sees which revision served a request. hold, when set,
// keeps each request open until it is closed.
type echoRuntime struct {
	mu      sync.Mutex
	servers map[string]*httptest.Server
	hold    chan struct{}
}

func (e *echoRuntime) Start(_ context.Context, spec container.StartSpec) (container.Started, error) {
	revision := ""
	for _, entry := range spec.Env {
		if value, ok := strings.CutPrefix(entry, "K_REVISION="); ok {
			revision = value
		}
	}
	e.mu.Lock()
	hold := e.hold
	e.mu.Unlock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case hold != nil && r.URL.Path == "/hold":
			<-hold
		case r.URL.Path == "/echo/uri":
			_, _ = io.WriteString(w, r.URL.RequestURI()) //nolint:gosec // a test revision echoes its own request
			return
		}
		_, _ = io.WriteString(w, revision)
	}))
	e.mu.Lock()
	e.servers[server.URL] = server
	e.mu.Unlock()
	address := strings.TrimPrefix(server.URL, "http://")
	return container.Started{Address: address, Container: server.URL, Peer: "127.0.0.1"}, nil
}

func (e *echoRuntime) Ready(context.Context, string) error { return nil }

func (e *echoRuntime) Stop(_ context.Context, id string) error {
	e.mu.Lock()
	server := e.servers[id]
	delete(e.servers, id)
	e.mu.Unlock()
	if server != nil {
		server.Close()
	}
	return nil
}

// launches counts the revision servers started and still running.
func (e *echoRuntime) launches() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.servers)
}

// trafficHarness is a harness whose revisions answer with their names, whose
// routing uses seed, and whose service is public.
type trafficHarness struct {
	*harness
	echo *echoRuntime
}

const (
	revisionType = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"
	serviceHost  = "svc-demo-project.us-central1.run.app"
)

func newTrafficHarness(t *testing.T, seed uint64) *trafficHarness {
	t.Helper()
	h := &trafficHarness{harness: newHarness(t), echo: &echoRuntime{servers: map[string]*httptest.Server{}}}
	h.provider.runtime, h.provider.router = h.echo, newRouter(seed)
	t.Cleanup(func() {
		for _, server := range h.echo.servers {
			server.Close()
		}
	})
	return h
}

// deploy sends a create or update, waits for its operation, and returns the
// service and the operation's error code.
func (h *trafficHarness) deploy(t *testing.T, verb, path string, body map[string]any) (map[string]any, float64) {
	t.Helper()
	recorder := h.invoke(verb, path, body)
	var operation struct {
		Name string `json:"name"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &operation) != nil {
		t.Fatalf("%s %s = %d %s", verb, path, recorder.Code, recorder.Body.String())
	}
	var code float64
	waitFor(t, func() bool {
		done, failed := h.operationState(t, operation.Name)
		code = failed
		return done
	})
	return h.service(t), code
}

// service reads the test service.
func (h *trafficHarness) service(t *testing.T) map[string]any {
	t.Helper()
	recorder := h.invoke(http.MethodGet, "/v2/"+serviceReference, nil)
	var service map[string]any
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &service) != nil {
		t.Fatalf("get = %d %s", recorder.Code, recorder.Body.String())
	}
	return service
}

// body is a full service body: the test image with MODE, and traffic.
func body(mode string, traffic ...map[string]any) map[string]any {
	document := map[string]any{"template": map[string]any{"containers": []any{map[string]any{
		"image": imageReference, "env": []any{map[string]any{"name": "MODE", "value": mode}},
	}}}}
	if len(traffic) > 0 {
		document["traffic"] = traffic
	}
	return document
}

// target is one traffic target; an empty revision is the latest.
func target(revision string, percent int, tag string) map[string]any {
	entry := map[string]any{keyType: latestTraffic}
	if revision != "" {
		entry[keyType], entry["revision"] = revisionType, revision
	}
	if percent != 0 {
		entry["percent"] = percent
	}
	if tag != "" {
		entry["tag"] = tag
	}
	return entry
}

// split creates the service, makes it public, and rolls out a second
// revision that takes half the traffic from the first, which is tagged blue.
func (h *trafficHarness) split(t *testing.T) map[string]any {
	t.Helper()
	h.deploy(t, http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services?serviceId=svc", body("first"))
	if recorder := h.invoke(http.MethodPost, "/v2/"+serviceReference+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}}},
	}}); recorder.Code != http.StatusOK {
		t.Fatalf("setIamPolicy = %d %s", recorder.Code, recorder.Body.String())
	}
	service, code := h.deploy(t, http.MethodPatch, "/v2/"+serviceReference,
		body("second", target(testFirstRevision, 50, "blue"), target("", 50, "")))
	if code != 0 {
		t.Fatalf("split rollout failed with %v", code)
	}
	return service
}

// call invokes one host and returns the status and the revision that answered.
func (h *trafficHarness) call(host, path string, prepare func(*http.Request)) (int, string) {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, invocationPath+host+path, nil)
	if prepare != nil {
		prepare(request)
	}
	recorder := httptest.NewRecorder()
	h.provider.Invocation().ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.String()
}

// field reads one string field of a service document.
func field(document map[string]any, key string) string {
	value, _ := document[key].(string)
	return value
}

// encoded renders a value as JSON for comparisons.
func encoded(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// A split rolls out as GCP reports it (recording run-traffic): statuses carry
// the resolved latest revision and the tag URL; requests to the service URL
// follow the percents by a weighted choice that one seed repeats.
func TestTrafficSplitRoutesByPercent(t *testing.T) {
	h := newTrafficHarness(t, 7)
	service := h.split(t)
	want := `[{"percent":50,"revision":"svc-00001","tag":"blue","type":"TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION",` +
		`"uri":"http://127.0.0.1/_run/blue---svc-demo-project.us-central1.run.app"},` +
		`{"percent":50,"revision":"svc-00002","type":"TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"}]`
	if got := encoded(t, service["trafficStatuses"]); got != want {
		t.Fatalf("trafficStatuses = %s", got)
	}
	sequence := make([]string, 0, 200)
	counts := map[string]int{}
	for range 200 {
		code, revision := h.call(serviceHost, "/", nil)
		if code != http.StatusOK {
			t.Fatalf("invoke = %d %s", code, revision)
		}
		sequence = append(sequence, revision)
		counts[revision]++
	}
	if counts["svc-00001"] < 70 || counts["svc-00002"] < 70 {
		t.Fatalf("a 50/50 split served %v", counts)
	}
	again := newTrafficHarness(t, 7)
	again.split(t)
	for i, want := range sequence {
		if _, got := again.call(serviceHost, "/", nil); got != want {
			t.Fatalf("request %d with the same seed went to %s, not %s", i, got, want)
		}
	}
}

// A tag URL reaches only its revision, with the path and query as sent; an
// unknown tag is 404.
func TestTagURLRoutesToTheTaggedRevision(t *testing.T) {
	h := newTrafficHarness(t, 7)
	h.split(t)
	for range 20 {
		if code, revision := h.call("blue---"+serviceHost, "/", nil); code != http.StatusOK || revision != "svc-00001" {
			t.Fatalf("tag URL = %d %s", code, revision)
		}
	}
	if code, uri := h.call("blue---"+serviceHost, "/echo/uri?x=1", nil); code != http.StatusOK || uri != "/echo/uri?x=1" {
		t.Fatalf("forwarded request = %d %s", code, uri)
	}
	if code, _ := h.call("green---"+serviceHost, "/", nil); code != http.StatusNotFound {
		t.Fatalf("unknown tag = %d", code)
	}
}

// A traffic-only update creates no revision; targets of one revision merge
// into one status and statuses sort by revision (recording run-traffic).
func TestTrafficOnlyUpdateMergesStatusesWithoutARevision(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.split(t)
	launched := h.echo.launches()
	service, code := h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, body("second",
		target("svc-00002", 100, ""), target(testFirstRevision, 0, "blue"), target("svc-00002", 0, "green")))
	if code != 0 || h.echo.launches() != launched || service["generation"] != "3" {
		t.Fatalf("traffic-only update: code %v, %d launches, generation %v", code, h.echo.launches(), service["generation"])
	}
	if !strings.HasSuffix(field(service, "latestCreatedRevision"), "/svc-00002") {
		t.Fatalf("latestCreatedRevision = %v", service["latestCreatedRevision"])
	}
	want := `[{"revision":"svc-00001","tag":"blue","type":"TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION",` +
		`"uri":"http://127.0.0.1/_run/blue---svc-demo-project.us-central1.run.app"},` +
		`{"percent":100,"revision":"svc-00002","tag":"green","type":"TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION",` +
		`"uri":"http://127.0.0.1/_run/green---svc-demo-project.us-central1.run.app"}]`
	if got := encoded(t, service["trafficStatuses"]); got != want {
		t.Fatalf("trafficStatuses = %s", got)
	}
	for range 20 {
		if _, revision := h.call(serviceHost, "/", nil); revision != "svc-00002" {
			t.Fatalf("all traffic to svc-00002 reached %s", revision)
		}
	}
	// The next revision is numbered by the revisions created, not the generation.
	service, _ = h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, body("third"))
	if !strings.HasSuffix(field(service, "latestCreatedRevision"), "/svc-00003") {
		t.Fatalf("latestCreatedRevision = %v", service["latestCreatedRevision"])
	}
}

// GCP refuses a block whose percents don't sum to 100 and duplicate tags
// with these messages, and fails the rollout of a block naming a missing
// revision, leaving the routing in effect (recording run-traffic).
func TestTrafficValidationErrors(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.split(t)
	cases := map[string]struct {
		traffic []map[string]any
		want    string
	}{
		"sum": {[]map[string]any{target(testFirstRevision, 40, ""), target("", 50, "")},
			`{"error":{"code":400,"details":[{"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"description":"traffic percentage adds to 90, should be 100","field":"traffic[].percent"}]}],"message":"traffic[].percent: traffic percentage adds to 90, should be 100","status":"INVALID_ARGUMENT"}}`},
		"duplicate tag": {[]map[string]any{target(testFirstRevision, 50, "same"), target("svc-00002", 50, "same")},
			`{"error":{"code":400,"details":[{"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"description":"may not contain a duplicate tag.","field":"spec.traffic"}]}],"message":"spec.traffic: may not contain a duplicate tag.","status":"INVALID_ARGUMENT"}}`},
	}
	for name, c := range cases {
		recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, body("second", c.traffic...))
		var got, want any
		_ = json.Unmarshal(recorder.Body.Bytes(), &got)
		_ = json.Unmarshal([]byte(c.want), &want)
		if recorder.Code != http.StatusBadRequest || encoded(t, got) != encoded(t, want) {
			t.Errorf("%s = %d %s", name, recorder.Code, recorder.Body.String())
		}
	}
	before := encoded(t, h.service(t)["trafficStatuses"])
	recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, body("second", target("svc-99999", 100, "")))
	var operation struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &operation)
	waitFor(t, func() bool { done, _ := h.operationState(t, operation.Name); return done })
	done := h.invoke(http.MethodGet, "/v2/"+operation.Name, nil).Body.String()
	if !strings.Contains(done, `"code":9,"message":"Revision 'svc-99999' does not exist or is deleted. Update all traffic to route to currently available revisions."`) {
		t.Fatalf("missing revision operation = %s", done)
	}
	service := h.service(t)
	terminal, _ := service["terminalCondition"].(map[string]any)
	if after := encoded(t, service["trafficStatuses"]); after != before || terminal[keyState] != conditionFailed || service["generation"] != "3" {
		t.Fatalf("after a failed rollout: statuses %s, terminal %v, generation %v", after, terminal, service["generation"])
	}
}

// Scaling is validated with GCP's messages and echoed on the template and
// the revision (recording run-traffic).
func TestScalingValidationAndEcho(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.deploy(t, http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services?serviceId=svc", body("first"))
	template := func(extra map[string]any) map[string]any {
		document := body("second")
		inner, _ := document["template"].(map[string]any)
		maps.Copy(inner, extra)
		return document
	}
	cases := map[string]struct {
		body map[string]any
		want string
	}{
		"min above max": {template(map[string]any{"scaling": map[string]any{"minInstanceCount": 3, "maxInstanceCount": 2}}),
			"Violation in UpdateServiceRequest.service.template.scaling.max_instance_count: must be greater or equal than min_instance_count."},
		"concurrency": {template(map[string]any{"maxInstanceRequestConcurrency": 1001}),
			"Violation in UpdateServiceRequest.service.template.max_instance_request_concurrency: Must be a number between 0 and 1000."},
	}
	for name, c := range cases {
		recorder := h.invoke(http.MethodPatch, "/v2/"+serviceReference, c.body)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"message":"`+c.want+`"`) {
			t.Errorf("%s = %d %s", name, recorder.Code, recorder.Body.String())
		}
	}
	if got := encoded(t, h.service(t)["template"]); !strings.Contains(got, `"maxInstanceRequestConcurrency":80`) {
		t.Fatalf("default concurrency = %s", got)
	}
	service, _ := h.deploy(t, http.MethodPatch, "/v2/"+serviceReference,
		template(map[string]any{"scaling": map[string]any{"maxInstanceCount": 2}, "maxInstanceRequestConcurrency": 10}))
	revision := h.invoke(http.MethodGet, "/v2/"+serviceReference+"/revisions/svc-00002", nil)
	for _, document := range []any{service["template"], json.RawMessage(revision.Body.Bytes())} {
		if got := encoded(t, document); !strings.Contains(got, `"maxInstanceRequestConcurrency":10`) || !strings.Contains(got, `"scaling":{"maxInstanceCount":2}`) {
			t.Fatalf("scaling echo = %s", got)
		}
	}
}

// A revision accepts its maximum instances times their concurrency at once;
// one more request is GCP's documented 429.
func TestConcurrencyCapsRequestsPerRevision(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.echo.hold = make(chan struct{})
	h.split(t)
	h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, map[string]any{"template": map[string]any{
		"containers":                    []any{map[string]any{"image": imageReference}},
		"scaling":                       map[string]any{"maxInstanceCount": 1},
		"maxInstanceRequestConcurrency": 2,
	}})
	var held sync.WaitGroup
	for range 2 {
		held.Go(func() { h.call(serviceHost, "/hold", nil) })
	}
	waitFor(t, func() bool {
		inflight := 0
		_ = h.provider.withService(trafficScope, "svc", func(_ *scopeState, record *serviceRecord) error {
			inflight = record.Revisions["svc-00003"].inflight
			return nil
		})
		return inflight == 2
	})
	code, text := h.call(serviceHost, "/", nil)
	close(h.echo.hold)
	held.Wait()
	if code != http.StatusTooManyRequests || !strings.Contains(text, "The request was aborted because there was no available instance.") {
		t.Fatalf("third request = %d %s", code, text)
	}
	if code, _ := h.call(serviceHost, "/", nil); code != http.StatusOK {
		t.Fatalf("after the held requests = %d", code)
	}
}

// trafficScope is the test service's scope.
var trafficScope = state.Scope{Project: "demo-project", Location: "us-central1"}

// GCP refuses to delete the latest created revision and one serving a
// percent, with these messages; a revision only tagged deletes, and its
// targets go with it (recording run-traffic).
func TestRevisionDeleteRules(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.split(t)
	for revision, want := range map[string]string{
		"svc-00002":       `The latest created Revision \"svc-00002\" cannot be directly deleted.`,
		testFirstRevision: `Revision \"svc-00001\" cannot be directly deleted because it is actively serving.`,
	} {
		recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference+"/revisions/"+revision, nil)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"message":"`+want+`","status":"FAILED_PRECONDITION"`) {
			t.Errorf("delete %s = %d %s", revision, recorder.Code, recorder.Body.String())
		}
	}
	h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, body("second",
		target("svc-00002", 100, ""), target(testFirstRevision, 0, "blue"), target("svc-00002", 0, "green")))
	if recorder := h.invoke(http.MethodDelete, "/v2/"+serviceReference+"/revisions/"+testFirstRevision, nil); recorder.Code != http.StatusOK {
		t.Fatalf("delete the tagged revision = %d %s", recorder.Code, recorder.Body.String())
	}
	service := h.service(t)
	if service["generation"] != "4" || strings.Contains(encoded(t, service["traffic"]), testFirstRevision) ||
		strings.Contains(encoded(t, service["trafficStatuses"]), testFirstRevision) {
		t.Fatalf("after deleting the tagged revision: %v", service)
	}
	if code, _ := h.call("blue---"+serviceHost, "/", nil); code != http.StatusNotFound {
		t.Fatalf("the deleted tag = %d", code)
	}
}

// Internal ingress answers external callers 404 before anything else (GCP,
// recording run-traffic); deliveries marked internal for the service's own
// project pass, and a mark for another project doesn't.
func TestIngressInternalRefusesExternalCallers(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.split(t)
	for _, ingress := range []string{"INGRESS_TRAFFIC_INTERNAL_ONLY", "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"} {
		update := body("second", target("", 100, ""))
		update["ingress"] = ingress
		service, _ := h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, update)
		if service["ingress"] != ingress {
			t.Fatalf("ingress = %v", service["ingress"])
		}
		if code, _ := h.call(serviceHost, "/", nil); code != http.StatusNotFound {
			t.Fatalf("%s external = %d", ingress, code)
		}
		mark := func(project string) func(*http.Request) {
			return func(request *http.Request) { provider.MarkInternal(request, project) }
		}
		if code, _ := h.call(serviceHost, "/", mark("demo-project")); code != http.StatusOK {
			t.Fatalf("%s internal = %d", ingress, code)
		}
		if code, _ := h.call(serviceHost, "/", mark("other-project")); code != http.StatusNotFound {
			t.Fatalf("%s internal from another project = %d", ingress, code)
		}
	}
	// A mark is bound to its path; a forged header is external.
	forged := func(request *http.Request) {
		other := httptest.NewRequestWithContext(context.Background(), http.MethodGet, invocationPath+serviceHost+"/other", nil)
		provider.MarkInternal(other, "demo-project")
		request.Header = other.Header
	}
	if code, _ := h.call(serviceHost, "/", forged); code != http.StatusNotFound {
		t.Fatalf("a replayed mark = %d", code)
	}
}

// A tag URL takes ID tokens for the service URL only; GCP answers 401 to a
// token whose audience is the tag URL (recording run-traffic).
func TestTagURLTakesTheServiceAudience(t *testing.T) {
	h := newTrafficHarness(t, 1)
	tokens, err := identity.NewIDTokens(h.engine, h.index)
	if err != nil {
		t.Fatal(err)
	}
	h.provider.tokens = tokens
	h.split(t)
	bearer := func(audience string) func(*http.Request) {
		token, issueErr := tokens.Issue("1", "invoker@demo-project.iam.gserviceaccount.com", audience, true)
		if issueErr != nil {
			t.Fatal(issueErr)
		}
		return func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+token) }
	}
	if code, _ := h.call("blue---"+serviceHost, "/", bearer("https://blue---"+serviceHost)); code != http.StatusUnauthorized {
		t.Fatalf("tag audience = %d", code)
	}
	code, text := h.call("blue---"+serviceHost, "/", bearer("https://"+serviceHost))
	// The token is valid; the invoker grant is allUsers, which a token holder also has.
	if code != http.StatusOK || text != testFirstRevision {
		t.Fatalf("service audience on the tag URL = %d %s", code, text)
	}
}

// Unrecorded shape refusals of single targets name the field.
func TestTrafficTargetShapeRefusals(t *testing.T) {
	for name, traffic := range map[string]map[string]any{
		"no type":            {"percent": 100},
		"latest names":       {keyType: latestTraffic, "revision": "r", "percent": 100},
		"revision unnamed":   {keyType: revisionType, "percent": 100},
		"percent over 100":   {keyType: latestTraffic, "percent": 101},
		"tag with uppercase": {keyType: latestTraffic, "percent": 100, "tag": "Blue"},
	} {
		h := newTrafficHarness(t, 1)
		document := body("first", traffic)
		recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services?serviceId=svc", document)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "traffic[]") {
			t.Errorf("%s = %d %s", name, recorder.Code, recorder.Body.String())
		}
	}
}

// minInstanceCount keeps a revision that receives traffic from the idle
// reaper; without it the revision stops.
func TestMinInstancesKeepARevisionWarm(t *testing.T) {
	h := newTrafficHarness(t, 1)
	h.split(t)
	h.deploy(t, http.MethodPatch, "/v2/"+serviceReference, map[string]any{"template": map[string]any{
		"containers": []any{map[string]any{"image": imageReference}},
		"scaling":    map[string]any{"minInstanceCount": 1},
	}, "traffic": []any{target("", 100, "")}})
	h.provider.clock = func() time.Time { return time.Now().Add(2 * idleTimeout) }
	h.provider.reap(context.Background(), trafficScope, "svc")
	var running []string
	_ = h.provider.withService(trafficScope, "svc", func(_ *scopeState, record *serviceRecord) error {
		for id, revision := range record.Revisions {
			if revision.Container != "" {
				running = append(running, id)
			}
		}
		return nil
	})
	if !slices.Equal(running, []string{"svc-00003"}) {
		t.Fatalf("running after the reaper: %v", running)
	}
}
