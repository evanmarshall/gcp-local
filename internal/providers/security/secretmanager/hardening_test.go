package secretmanager

import (
	"encoding/json"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
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
// behavior later: refused by name, never stored and ignored.
func TestUnmodeledSecretFieldsAreGaps(t *testing.T) {
	h := newHarness(t)
	auto := replicationAuto()
	cases := []struct {
		field string
		body  map[string]any
	}{
		{"rotation", map[string]any{"replication": auto, "rotation": map[string]any{"rotationPeriod": "3600s"}}},
		{"expireTime", map[string]any{"replication": auto, "expireTime": "2030-01-01T00:00:00Z"}},
		{"ttl", map[string]any{"replication": auto, "ttl": "3600s"}},
		{"versionAliases", map[string]any{"replication": auto, "versionAliases": map[string]any{"current": "1"}}},
		{"topics", map[string]any{"replication": auto, "topics": []any{map[string]any{"name": "projects/p/topics/t"}}}},
		{"versionDestroyTtl", map[string]any{"replication": auto, "versionDestroyTtl": "86400s"}},
		{"replication.automatic.customerManagedEncryption", map[string]any{"replication": map[string]any{
			"automatic": map[string]any{"customerManagedEncryption": map[string]any{"kmsKeyName": "k"}}}}},
		{"replication.userManaged", map[string]any{"replication": map[string]any{
			"userManaged": map[string]any{"replicas": []any{map[string]any{"location": "us-east1"}}}}}},
	}
	for _, c := range cases {
		recorder := h.invoke(http.MethodPost, "/v1/projects/"+testProject+"/secrets?secretId="+testSecret, c.body, "actor")
		refused(t, c.field, recorder, 501, "UNIMPLEMENTED", "Secret Manager coverage gap: "+c.field+" is not modeled")
	}
	if code := h.invoke(http.MethodGet, secretPath, nil, "actor").Code; code != 404 {
		t.Fatalf("a refused create stored the secret: %d", code)
	}
	h.create(auto)
	refused(t, "update rotation", h.invoke(http.MethodPatch, secretPath+"?updateMask=rotation",
		map[string]any{"rotation": map[string]any{"rotationPeriod": "3600s"}}, "actor"),
		501, "UNIMPLEMENTED", "Secret Manager coverage gap: rotation is not modeled")
	// Clearing a field that is never set changes nothing.
	document(t, "clear rotation", h.invoke(http.MethodPatch, secretPath+"?updateMask=rotation,ttl", map[string]any{}, "actor"))
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
