package secretmanager

import (
	"encoding/json"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// wireError is one decoded error body.
type wireError struct {
	Code    int              `json:"code"`
	Status  string           `json:"status"`
	Message string           `json:"message"`
	Details []map[string]any `json:"details"`
}

// refused requires one error's HTTP status, status name, and message.
func refused(t *testing.T, step string, recorder *httptest.ResponseRecorder, code int, statusText, message string) wireError {
	t.Helper()
	var body struct {
		Error wireError `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != code ||
		body.Error.Status != statusText || body.Error.Message != message {
		t.Fatalf("%s = %d %s, want %d %s %q", step, recorder.Code, recorder.Body.String(), code, statusText, message)
	}
	return body.Error
}

// document requires HTTP 200 and decodes the body.
func document(t *testing.T, step string, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &body) != nil {
		t.Fatalf("%s = %d %s", step, recorder.Code, recorder.Body.String())
	}
	return body
}

// TestUnmodeledSecretFieldsAreGaps covers each field that changes a secret's
// behavior and is not modeled: refused by name, never stored and ignored.
func TestUnmodeledSecretFieldsAreGaps(t *testing.T) {
	h := newHarness(t)
	auto := replicationAuto()
	cmek := map[string]any{"kmsKeyName": "k"}
	cases := []struct {
		field string
		body  map[string]any
	}{
		{"customerManagedEncryption", map[string]any{"replication": auto, "customerManagedEncryption": cmek}},
		{"replication.automatic.customerManagedEncryption", map[string]any{"replication": map[string]any{
			"automatic": map[string]any{"customerManagedEncryption": cmek}}}},
		{"replication.userManaged.replicas.customerManagedEncryption", map[string]any{"replication": map[string]any{
			"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1", "customerManagedEncryption": cmek}}}}}},
		{"secretType", map[string]any{"replication": auto, "secretType": "PASSWORD"}},
	}
	for _, c := range cases {
		recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, c.body, "actor")
		refused(t, c.field, recorder, 501, "UNIMPLEMENTED", "Secret Manager coverage gap: "+c.field+" is not modeled")
	}
	if code := h.invoke(http.MethodGet, secretPath, nil, "actor").Code; code != 404 {
		t.Fatalf("a refused create stored the secret: %d", code)
	}
	h.create(auto)
	// Clearing a field that is never set changes nothing.
	document(t, "clear encryption", h.invoke(http.MethodPatch, secretPath+"?updateMask=customer_managed_encryption", map[string]any{}, "actor"))
}

// TestRegionalSecretsAreGaps: the locations/*/secrets routes are not served.
func TestRegionalSecretsAreGaps(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/locations/us-central1/secrets?secretId=s",
		map[string]any{}, "actor")
	refused(t, "regional create", recorder, 501, "UNIMPLEMENTED", "No provider implements this REST API method")
}

// TestAnnotationsAndMaskedUpdates pins the recorded update behavior:
// annotations stored and returned, a required mask, etags, the replication
// refusal, and annotation key validation.
func TestAnnotationsAndMaskedUpdates(t *testing.T) {
	h := newHarness(t)
	created := document(t, "create", h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, map[string]any{
		"replication": replicationAuto(), "labels": map[string]any{"a": "b"}, "annotations": map[string]any{"gcp-local.purpose": "x"},
	}, "actor"))
	if !reflect.DeepEqual(created["annotations"], map[string]any{"gcp-local.purpose": "x"}) {
		t.Fatalf("created = %v", created)
	}
	labels := map[string]any{"a": "c"}
	refused(t, "no mask", h.invoke(http.MethodPatch, secretPath, map[string]any{"labels": labels}, "actor"),
		400, "INVALID_ARGUMENT", "Field [update_mask] is required.")
	refused(t, "stale etag", h.invoke(http.MethodPatch, secretPath+"?updateMask=labels",
		map[string]any{"labels": labels, "etag": `"0123456789abc"`}, "actor"),
		400, "FAILED_PRECONDITION", "The etag provided in the request does not match the resource's current etag. "+
			"Please retry the whole read-modify-write with exponential backoff.")
	refused(t, "replication", h.invoke(http.MethodPatch, secretPath+"?updateMask=replication", map[string]any{
		"replication": map[string]any{"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1"}}}},
	}, "actor"), 400, "INVALID_ARGUMENT",
		"Existing secret has automatic replication, but requested secret does not. Updating secret replication is not supported.")
	refused(t, "annotation key", h.invoke(http.MethodPatch, secretPath+"?updateMask=annotations",
		map[string]any{"annotations": map[string]any{"gcp-local/purpose": "x"}}, "actor"), 400, "INVALID_ARGUMENT",
		`[gcp-local/purpose] must follow pattern [a-z0-9A-Z]+([_\.\-]*[a-z0-9A-Z]+)*), be less than 64 characters, and must have a UTF encoding of less than 128 bytes`)
	updated := document(t, "update", h.invoke(http.MethodPatch, secretPath+"?updateMask=labels,annotations", map[string]any{
		"labels": labels, "annotations": map[string]any{"b": "y"}, "etag": created["etag"]}, "actor"))
	if updated["etag"] == created["etag"] || !reflect.DeepEqual(updated["labels"], labels) ||
		!reflect.DeepEqual(updated["annotations"], map[string]any{"b": "y"}) {
		t.Fatalf("updated = %v", updated)
	}
	refused(t, "stale delete", h.invoke(http.MethodDelete, secretPath+"?etag=%22stale%22", nil, "actor"),
		400, "FAILED_PRECONDITION", "The etag provided in the request does not match the resource's current etag. "+
			"Please retry the whole read-modify-write with exponential backoff.")
	etag, _ := updated["etag"].(string)
	document(t, "delete", h.invoke(http.MethodDelete, secretPath+"?etag="+url.QueryEscape(etag), nil, "actor"))
}

// TestPayloadChecksumAndVersionETags pins "add-checksum-mismatch",
// "add-checksum-match", and "disable-stale-etag".
func TestPayloadChecksumAndVersionETags(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	payload := []byte("gcp-local synthetic secret payload")
	sum := crc32.Checksum(payload, castagnoli)
	add := func(checksum uint32) *httptest.ResponseRecorder {
		return h.invoke(http.MethodPost, secretPath+":addVersion", map[string]any{"payload": map[string]any{
			"data": base64Of(string(payload)), "dataCrc32c": strconv.FormatUint(uint64(checksum), 10)}}, "actor")
	}
	refused(t, "mismatch", add(sum+1), 400, "INVALID_ARGUMENT", "Provided SecretPayload.data crc32c does not match calculated crc32c.")
	added := document(t, "match", add(sum))
	if added["clientSpecifiedPayloadChecksum"] != true || added["name"] != "projects/123456/secrets/"+testSecret+"/versions/1" {
		t.Fatalf("added = %v", added)
	}
	refused(t, "stale disable", h.invoke(http.MethodPost, secretPath+"/versions/1:disable", map[string]any{"etag": `"0123456789abc"`}, "actor"),
		400, "FAILED_PRECONDITION", "The etag provided in the request does not match the resource's current etag. "+
			"Please retry the whole read-modify-write with exponential backoff.")
	disabled := document(t, "disable", h.invoke(http.MethodPost, secretPath+"/versions/1:disable", map[string]any{"etag": added["etag"]}, "actor"))
	if disabled["state"] != stateDisabled {
		t.Fatalf("disabled = %v", disabled)
	}
	refused(t, "unknown field", h.invoke(http.MethodPost, secretPath+"/versions/1:enable", map[string]any{"unknownField": 1}, "actor"),
		400, "INVALID_ARGUMENT", `Invalid JSON payload received. Unknown name "unknownField": Cannot find field.`)
}

// TestStrictSecretBodies pins the recorded refusals of unknown and mistyped fields.
func TestStrictSecretBodies(t *testing.T) {
	h := newHarness(t)
	create := func(body map[string]any) *httptest.ResponseRecorder {
		return h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, body, "actor")
	}
	refused(t, "unknown", create(map[string]any{"replication": replicationAuto(), "unknownField": "x"}), 400, "INVALID_ARGUMENT",
		`Invalid JSON payload received. Unknown name "unknownField" at 'secret': Cannot find field.`)
	refused(t, "nested", create(map[string]any{"replication": map[string]any{"automatic": map[string]any{"unknownField": "x"}}}),
		400, "INVALID_ARGUMENT", `Invalid JSON payload received. Unknown name "unknownField" at 'secret.replication.automatic': Cannot find field.`)
	refused(t, "mistyped", create(map[string]any{"replication": replicationAuto(), "labels": "x"}), 400, "INVALID_ARGUMENT",
		`Invalid value at 'secret.labels' (type.googleapis.com/google.cloud.secretmanager.v1.Secret.LabelsEntry), "x"`)
}

// listPage is one decoded secrets or versions list page.
type listPage struct {
	Secrets []struct {
		Name string `json:"name"`
	} `json:"secrets"`
	Versions []struct {
		Name string `json:"name"`
	} `json:"versions"`
	NextPageToken string `json:"nextPageToken"`
	TotalSize     int    `json:"totalSize"`
}

// listed requires a 200 list page.
func listed(t *testing.T, step string, recorder *httptest.ResponseRecorder) listPage {
	t.Helper()
	var page listPage
	if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &page) != nil {
		t.Fatalf("%s = %d %s", step, recorder.Code, recorder.Body.String())
	}
	return page
}

// TestListsPageInRecordedOrder pins "secrets-paged", "versions-paged", and
// the page refusals: names order secrets, versions list newest first, a
// negative size reads as unset, and a foreign token names itself.
func TestListsPageInRecordedOrder(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"b-first", "a-second"} {
		document(t, id, h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+id,
			map[string]any{"replication": replicationAuto()}, "actor"))
	}
	list := "/v1/projects/" + testProject + "/secrets"
	first := listed(t, "first", h.invoke(http.MethodGet, list+"?pageSize=1", nil, "actor"))
	if len(first.Secrets) != 1 || first.Secrets[0].Name != "projects/123456/secrets/a-second" || first.NextPageToken == "" || first.TotalSize != 2 {
		t.Fatalf("first page = %+v", first)
	}
	if second := listed(t, "second", h.invoke(http.MethodGet, list+"?pageSize=1&pageToken="+first.NextPageToken, nil, "actor")); len(second.Secrets) != 1 || second.NextPageToken != "" {
		t.Fatalf("second page = %+v", second)
	}
	if all := listed(t, "negative", h.invoke(http.MethodGet, list+"?pageSize=-1", nil, "actor")); len(all.Secrets) != 2 {
		t.Fatalf("negative size = %+v", all)
	}
	refused(t, "token", h.invoke(http.MethodGet, list+"?pageToken=not-a-page-token", nil, "actor"), 400, "INVALID_ARGUMENT",
		"page_token [not-a-page-token] is invalid. Please retry with a valid token or retry the initial List request.")
	refused(t, "filter", h.invoke(http.MethodGet, list+"?filter=labels.a%3Db", nil, "actor"), 501, "UNIMPLEMENTED",
		"Secret Manager coverage gap: list filter is not implemented")
}

// TestVersionListPagesNewestFirst pins "versions-paged".
func TestVersionListPagesNewestFirst(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	h.addVersion(t, "one")
	h.addVersion(t, "two")
	versions := listed(t, "versions", h.invoke(http.MethodGet, secretPath+"/versions?pageSize=1", nil, "actor"))
	if len(versions.Versions) != 1 || versions.Versions[0].Name != "projects/123456/secrets/"+testSecret+"/versions/2" ||
		versions.NextPageToken == "" || versions.TotalSize != 2 {
		t.Fatalf("versions page = %+v", versions)
	}
}

// TestRecordedDenialAndConditions pins the recorded denial (message without
// the resource, ErrorInfo in the iam.googleapis.com domain with only the
// permission) and "conditional-access".
func TestRecordedDenialAndConditions(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	h.addVersion(t, "one")
	h.addVersion(t, "two")
	denied := refused(t, "denied", h.invoke(http.MethodGet, secretPath+"/versions/1:access", nil, testAccessor), 403,
		"PERMISSION_DENIED", "Permission 'secretmanager.versions.access' denied on resource (or it may not exist).")
	want := []map[string]any{{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "IAM_PERMISSION_DENIED",
		"domain": "iam.googleapis.com", "metadata": map[string]any{"permission": "secretmanager.versions.access"}}}
	if !reflect.DeepEqual(denied.Details, want) {
		t.Fatalf("details = %v", denied.Details)
	}
	name := "projects/123456/secrets/" + testSecret
	member := "serviceAccount:" + testAccessor + "@" + testProject + ".iam.gserviceaccount.com"
	document(t, "conditional grant", h.invoke(http.MethodPost, secretPath+":setIamPolicy", map[string]any{"policy": map[string]any{
		"version": 3, "bindings": []any{
			map[string]any{"role": "roles/secretmanager.secretAccessor", "members": []any{member}, "condition": map[string]any{
				"title": "version-two", "expression": `resource.type == "secretmanager.googleapis.com/SecretVersion" && ` +
					`resource.service == "secretmanager.googleapis.com" && resource.name == "` + name + `/versions/2"`}},
			map[string]any{"role": "roles/secretmanager.viewer", "members": []any{member}, "condition": map[string]any{
				"title": "secret-only", "expression": `resource.type == "secretmanager.googleapis.com/Secret" && resource.name == "` + name + `"`}},
		}}}, "actor"))
	for path, code := range map[string]int{
		"/versions/2:access": 200, "/versions/1:access": 403, "/versions/latest:access": 403,
		"": 200, "/versions/2": 403, "/versions": 200,
	} {
		if got := h.invoke(http.MethodGet, secretPath+path, nil, testAccessor).Code; got != code {
			t.Fatalf("%q = %d, want %d", path, got, code)
		}
	}
}

// testClock is a manual time source for the lifecycle workers.
type testClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []testWaiter
}

type testWaiter struct {
	at time.Time
	ch chan time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, testWaiter{c.now.Add(d), ch})
	return ch
}

// Advance moves the clock and fires the timers that are due.
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	pending := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			pending = append(pending, w)
			continue
		}
		w.ch <- c.now
	}
	c.waiters = pending
}

// published is one captured Pub/Sub publish.
type published struct {
	subject, topic string
	attributes     map[string]string
	data           map[string]any
}

// lifecycleHarness adds a manual clock, a topic the service agent may
// publish to, and a capturing Pub/Sub publish handler.
type lifecycleHarness struct {
	*harness
	clock *testClock
	mu    sync.Mutex
	sent  []published
}

const (
	testTopic = "projects/" + testProject + "/topics/rotations"
	testAgent = "serviceAccount:service-123456@gcp-sa-secretmanager.iam.gserviceaccount.com"
)

func newLifecycleHarness(t *testing.T, grant bool) *lifecycleHarness {
	t.Helper()
	h := &lifecycleHarness{harness: newHarness(t), clock: &testClock{now: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)}}
	h.provider.clock, h.provider.after = h.clock.Now, h.clock.After
	topic := "//pubsub.googleapis.com/" + testTopic
	if err := h.provider.index.Put("pubsub", state.Resource{Name: topic, Service: "pubsub", Kind: "topic",
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Scope: state.Scope{Project: testProject, Location: state.Global}}); err != nil {
		t.Fatalf("index the topic: %v", err)
	}
	if grant {
		if _, err := h.provider.engine.SetPolicy(topic, iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{testAgent}}}}); err != nil {
			t.Fatalf("grant the service agent: %v", err)
		}
	}
	if err := h.provider.registry.Register(publishMethod, func(call gateway.Call, raw any) (any, error) {
		request, _ := raw.(*pubsubpb.PublishRequest)
		message := request.GetMessages()[0]
		var data map[string]any
		_ = json.Unmarshal(message.GetData(), &data)
		h.mu.Lock()
		h.sent = append(h.sent, published{call.Principal.Subject, request.GetTopic(), message.GetAttributes(), data})
		h.mu.Unlock()
		return &pubsubpb.PublishResponse{MessageIds: []string{"1"}}, nil
	}); err != nil {
		t.Fatalf("register the publish handler: %v", err)
	}
	return h
}

// events waits until n notifications were published and returns their types.
func (h *lifecycleHarness) events(n int) []published {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.Lock()
		sent := slices.Clone(h.sent)
		h.mu.Unlock()
		if len(sent) >= n || time.Now().After(deadline) {
			if len(sent) != n {
				h.t.Fatalf("published %d notifications, want %d: %+v", len(sent), n, sent)
			}
			return sent
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// eventually polls a request until check passes.
func (h *lifecycleHarness) eventually(step string, check func() bool) {
	h.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !check(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			h.t.Fatalf("%s did not happen", step)
		}
	}
}

func (h *lifecycleHarness) createSecret(body map[string]any) *httptest.ResponseRecorder {
	return h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, body, "actor")
}

// TestRotationRefusals: rotation needs a topic, a next time in the
// documented window, and a period of whole seconds from 1 hour.
func TestRotationRefusals(t *testing.T) {
	h := newLifecycleHarness(t, true)
	topics := []any{map[string]any{"name": testTopic}}
	next := h.clock.Now().Add(10 * time.Minute)
	rotation := map[string]any{"nextRotationTime": next.Format(time.RFC3339), "rotationPeriod": "3600s"}
	auto := replicationAuto()
	refused(t, "no topic", h.createSecret(map[string]any{"replication": auto, "rotation": rotation}), 400, "INVALID_ARGUMENT",
		"There must be at least one topic configured when a Rotation policy is set.")
	refused(t, "too soon", h.createSecret(map[string]any{"replication": auto, "topics": topics,
		"rotation": map[string]any{"nextRotationTime": h.clock.Now().Add(time.Minute).Format(time.RFC3339)}}), 400, "INVALID_ARGUMENT",
		"Next rotation time must be at least [5m] in the future.")
	refused(t, "short period", h.createSecret(map[string]any{"replication": auto, "topics": topics,
		"rotation": map[string]any{"nextRotationTime": next.Format(time.RFC3339), "rotationPeriod": "60s"}}), 400, "INVALID_ARGUMENT",
		"Rotation period cannot be shorter than [1h].")
	refused(t, "period alone", h.createSecret(map[string]any{"replication": auto, "topics": topics,
		"rotation": map[string]any{"rotationPeriod": "3600s"}}), 400, "FAILED_PRECONDITION", "Next rotation time must be set while the rotation period is set.")
	document(t, "create", h.createSecret(map[string]any{"replication": auto, "topics": topics, "rotation": rotation}))
	refused(t, "drop topics", h.invoke(http.MethodPatch, secretPath+"?updateMask=topics", map[string]any{}, "actor"),
		400, "INVALID_ARGUMENT", "There must be at least one topic configured when a Rotation policy is set.")
}

// TestRotationNotifiesTopics: at nextRotationTime the service agent
// publishes SECRET_ROTATE and the time advances by rotationPeriod. Nothing
// rotates the value.
func TestRotationNotifiesTopics(t *testing.T) {
	h := newLifecycleHarness(t, true)
	topics := []any{map[string]any{"name": testTopic}}
	rotation := map[string]any{"nextRotationTime": h.clock.Now().Add(10 * time.Minute).Format(time.RFC3339), "rotationPeriod": "3600s"}
	created := document(t, "create", h.createSecret(map[string]any{"replication": replicationAuto(), "topics": topics, "rotation": rotation}))
	if !reflect.DeepEqual(created["rotation"], map[string]any{"nextRotationTime": "2026-10-13T12:10:00Z", "rotationPeriod": "3600s"}) ||
		!reflect.DeepEqual(created["topics"], topics) {
		t.Fatalf("created = %v", created)
	}
	h.addVersion(t, "one")
	h.clock.Advance(10 * time.Minute)
	sent := h.events(4)
	if !reflect.DeepEqual(sent[0].attributes, map[string]string{"eventType": eventConfigured}) || sent[0].data != nil {
		t.Fatalf("topic configured = %+v", sent[0])
	}
	for i, kind := range []string{eventCreate, eventVersionAdd, eventRotate} {
		requireNotification(t, sent[i+1], kind)
	}
	rotated, _ := sent[3].data["rotation"].(map[string]any)
	if sent[2].attributes["versionId"] != "projects/123456/secrets/"+testSecret+"/versions/1" || rotated["nextRotationTime"] != "2026-10-13T13:10:00Z" {
		t.Fatalf("notifications = %+v", sent)
	}
	h.eventually("next rotation advanced", func() bool {
		rotated := document(t, "get", h.invoke(http.MethodGet, secretPath, nil, "actor"))
		value, _ := rotated["rotation"].(map[string]any)
		return value["nextRotationTime"] == "2026-10-13T13:10:00Z"
	})
	payload := h.invoke(http.MethodGet, secretPath+"/versions/latest:access", nil, "actor")
	if !strings.Contains(payload.Body.String(), `"name":"projects/123456/secrets/`+testSecret+`/versions/1"`) {
		t.Fatalf("rotation changed the value: %s", payload.Body.String())
	}
	if cleared := document(t, "clear", h.invoke(http.MethodPatch, secretPath+"?updateMask=rotation", map[string]any{}, "actor")); cleared["rotation"] != nil {
		t.Fatalf("cleared = %v", cleared)
	}
}

// requireNotification checks one notification's type, publisher, topic,
// and the attributes every event carries.
func requireNotification(t *testing.T, sent published, kind string) {
	t.Helper()
	want := map[string]string{"eventType": kind, "dataFormat": "JSON_API_V1", "secretId": "projects/123456/secrets/" + testSecret}
	for key, value := range want {
		if sent.attributes[key] != value {
			t.Fatalf("notification %s: %s = %q, want %q", kind, key, sent.attributes[key], value)
		}
	}
	if sent.subject != testAgent || sent.topic != testTopic {
		t.Fatalf("notification %s = %+v", kind, sent)
	}
}

// TestTopicsNeedThePublisherGrant: without pubsub.topics.publish for the
// service agent, or for a topic that doesn't exist, topics are refused.
func TestTopicsNeedThePublisherGrant(t *testing.T) {
	h := newLifecycleHarness(t, false)
	topics := func(topic string) map[string]any {
		return map[string]any{"replication": replicationAuto(), "topics": []any{map[string]any{"name": topic}}}
	}
	agent := strings.TrimPrefix(testAgent, "serviceAccount:")
	refused(t, "no grant", h.createSecret(topics(testTopic)), 400, "FAILED_PRECONDITION", "Permission 'pubsub.topics.publish' denied for "+
		agent+" for pubsub topic: "+testTopic+" or the topic doesn't exist. Grant the 'pubsub.topic.publish' permission "+
		"(or 'roles/pubsub.publisher') on this topic to "+agent+".")
	missing := "projects/" + testProject + "/topics/missing"
	refused(t, "missing", h.createSecret(topics(missing)), 404, "NOT_FOUND", "Topic ["+missing+"] not found.")
	refused(t, "bad name", h.createSecret(map[string]any{"replication": replicationAuto(), "topics": []any{map[string]any{"name": "rotations"}}}),
		400, "INVALID_ARGUMENT", `Invalid Pub/Sub topic name "rotations".`)
}

// TestExpirationDeletesTheSecret: ttl reads back as expireTime, and at that
// time the secret, its versions, and its policy are deleted, with a
// SECRET_DELETE notification of deleteType EXPIRATION.
func TestExpirationDeletesTheSecret(t *testing.T) {
	h := newLifecycleHarness(t, true)
	refused(t, "past", h.createSecret(map[string]any{"replication": replicationAuto(), "expireTime": "2020-01-01T00:00:00Z"}),
		400, "INVALID_ARGUMENT", "Expiration time must be at least [1m] in the future.")
	created := document(t, "create", h.createSecret(map[string]any{"replication": replicationAuto(), "ttl": "120s",
		"topics": []any{map[string]any{"name": testTopic}}}))
	if created["expireTime"] != "2026-10-13T12:02:00Z" || created["ttl"] != nil {
		t.Fatalf("created = %v", created)
	}
	extended := document(t, "extend", h.invoke(http.MethodPatch, secretPath+"?updateMask=expire_time",
		map[string]any{"expireTime": "2026-10-13T12:05:00.5Z"}, "actor"))
	if extended["expireTime"] != "2026-10-13T12:05:00.500Z" {
		t.Fatalf("extended = %v", extended)
	}
	h.clock.Advance(3 * time.Minute)
	if code := h.invoke(http.MethodGet, secretPath, nil, "actor").Code; code != 200 {
		t.Fatalf("the secret expired early: %d", code)
	}
	h.clock.Advance(3 * time.Minute)
	h.eventually("expiry", func() bool { return h.invoke(http.MethodGet, secretPath, nil, "actor").Code == 404 })
	sent := h.events(4)
	if sent[3].attributes["eventType"] != eventDelete || sent[3].attributes["deleteType"] != "EXPIRATION" {
		t.Fatalf("delete notification = %+v", sent[3])
	}
	if _, ok := h.provider.index.Get(secretResource(testProject, testSecret)); ok {
		t.Fatal("the expired secret stayed indexed")
	}
}

// TestVersionAliases: aliases name existing versions, read back as strings,
// and resolve only for get and access.
func TestVersionAliases(t *testing.T) {
	h := newLifecycleHarness(t, true)
	refused(t, "no version", h.createSecret(map[string]any{"replication": replicationAuto(), "versionAliases": map[string]any{"current": "1"}}),
		400, "INVALID_ARGUMENT", "Aliases cannot be assigned to versions that don't exist")
	h.create(replicationAuto())
	h.addVersion(t, "one")
	h.addVersion(t, "two")
	refused(t, "reserved", h.invoke(http.MethodPatch, secretPath+"?updateMask=version_aliases",
		map[string]any{"versionAliases": map[string]any{"latest": "1"}}, "actor"), 400, "INVALID_ARGUMENT", `[latest] must follow pattern [a-zA-Z][a-Aa-Z0-9_-]+, be less than 64 characters and cannot be "latest" or "NEW"`)
	updated := document(t, "alias", h.invoke(http.MethodPatch, secretPath+"?updateMask=versionAliases",
		map[string]any{"versionAliases": map[string]any{"current": "1"}}, "actor"))
	if !reflect.DeepEqual(updated["versionAliases"], map[string]any{"current": "1"}) {
		t.Fatalf("updated = %v", updated)
	}
	accessed := document(t, "access", h.invoke(http.MethodGet, secretPath+"/versions/current:access", nil, "actor"))
	if accessed["name"] != "projects/123456/secrets/"+testSecret+"/versions/1" {
		t.Fatalf("accessed = %v", accessed)
	}
	document(t, "get", h.invoke(http.MethodGet, secretPath+"/versions/current", nil, "actor"))
	refused(t, "disable by alias", h.invoke(http.MethodPost, secretPath+"/versions/current:disable", map[string]any{}, "actor"),
		400, "INVALID_ARGUMENT", "The provided Secret Version ID [projects/123456/secrets/"+testSecret+"/versions/current] "+
			"does not match the expected format [projects/*/secrets/*/versions*]")
}

// TestUserManagedReplication: replicas are stored and reported in request
// order on the secret and each version, and replication stays immutable.
func TestUserManagedReplication(t *testing.T) {
	h := newLifecycleHarness(t, true)
	managed := map[string]any{"userManaged": map[string]any{"replicas": []any{
		map[string]any{"location": "us-east1"}, map[string]any{"location": "us-central1"}}}}
	refused(t, "empty", h.createSecret(map[string]any{"replication": map[string]any{"userManaged": map[string]any{}}}),
		400, "INVALID_ARGUMENT", "At least one location must be provided for a user_managed policy.")
	created := document(t, "create", h.createSecret(map[string]any{"replication": managed}))
	if !reflect.DeepEqual(created["replication"], managed) {
		t.Fatalf("created = %v", created)
	}
	added := document(t, "add", h.invoke(http.MethodPost, secretPath+":addVersion",
		map[string]any{"payload": map[string]any{"data": base64Of("one")}}, "actor"))
	if !reflect.DeepEqual(added["replicationStatus"], managed) {
		t.Fatalf("added = %v", added)
	}
	document(t, "same", h.invoke(http.MethodPatch, secretPath+"?updateMask=replication", map[string]any{"replication": managed}, "actor"))
	refused(t, "automatic", h.invoke(http.MethodPatch, secretPath+"?updateMask=replication", map[string]any{"replication": replicationAuto()}, "actor"),
		400, "INVALID_ARGUMENT", "Existing secret has user-managed replication, but requested secret does not. Updating secret replication is not supported.")
	refused(t, "replicas", h.invoke(http.MethodPatch, secretPath+"?updateMask=replication", map[string]any{"replication": map[string]any{
		"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1"}}}}}, "actor"),
		400, "INVALID_ARGUMENT", "Adding or removing replicas from Secret user-managed replication is not supported. "+
			"Number of replicas requested: [1], number of existing replicas: [2]")
}

// TestDelayedDestruction: with versionDestroyTtl, destroy disables the
// version and schedules destruction; enabling cancels it; otherwise the
// worker destroys it at scheduledDestroyTime.
func TestDelayedDestruction(t *testing.T) {
	h := newLifecycleHarness(t, true)
	refused(t, "short", h.createSecret(map[string]any{"replication": replicationAuto(), "versionDestroyTtl": "60s"}),
		400, "INVALID_ARGUMENT", "Version destroy TTL must be at least [24h].")
	created := document(t, "create", h.createSecret(map[string]any{"replication": replicationAuto(), "versionDestroyTtl": "86400s",
		"topics": []any{map[string]any{"name": testTopic}}}))
	if created["versionDestroyTtl"] != "86400s" {
		t.Fatalf("created = %v", created)
	}
	h.addVersion(t, "one")
	scheduled := document(t, "destroy", h.invoke(http.MethodPost, secretPath+"/versions/1:destroy", map[string]any{}, "actor"))
	if scheduled["state"] != stateDisabled || scheduled["scheduledDestroyTime"] != "2026-10-14T12:00:00.000000Z" {
		t.Fatalf("scheduled = %v", scheduled)
	}
	refused(t, "again", h.invoke(http.MethodPost, secretPath+"/versions/1:destroy", map[string]any{}, "actor"),
		400, "FAILED_PRECONDITION", "SecretVersion is already scheduled for DESTRUCTION.")
	if enabled := document(t, "enable", h.invoke(http.MethodPost, secretPath+"/versions/1:enable", map[string]any{}, "actor")); enabled["scheduledDestroyTime"] != nil {
		t.Fatalf("enabled = %v", enabled)
	}
	document(t, "destroy again", h.invoke(http.MethodPost, secretPath+"/versions/1:destroy", map[string]any{}, "actor"))
	h.clock.Advance(24 * time.Hour)
	h.eventually("destruction", func() bool {
		version := document(t, "get", h.invoke(http.MethodGet, secretPath+"/versions/1", nil, "actor"))
		return version["state"] == stateDestroyed && version["destroyTime"] != nil
	})
	kinds := make([]string, 0, 7)
	for _, sent := range h.events(7) {
		kinds = append(kinds, sent.attributes["eventType"])
	}
	want := []string{eventConfigured, eventCreate, eventVersionAdd, eventVersionScheduled, eventVersionEnable, eventVersionScheduled, eventVersionDestroy}
	if !slices.Equal(kinds, want) {
		t.Fatalf("notifications = %v, want %v", kinds, want)
	}
}
