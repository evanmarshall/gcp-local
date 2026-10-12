package storage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/state"
)

const admin = "admin@project-a.test"

// decodeBody reads one JSON response document.
func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	value := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return value
}

// TestBucketMetadataRead covers the provider-owned fields of a bucket read.
func TestBucketMetadataRead(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "meta-bucket", 200)
	got := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/meta-bucket", nil, 200).Body.Bytes())
	if got["projectNumber"] != "123456" || got["metageneration"] != "1" || got["kind"] != "storage#bucket" || got["id"] != "meta-bucket" {
		t.Fatalf("bucket = %v", got)
	}
	if rpo, found := got["rpo"]; !found || rpo != nil {
		t.Fatalf("single-region bucket must report rpo null: %v", got)
	}
}

// TestBucketLabelPatchAndPreconditions covers label updates, label removal,
// metageneration preconditions, and refused patch shapes.
func TestBucketLabelPatchAndPreconditions(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "meta-bucket", 200)

	patched := decodeBody(t, f.request(admin, "PATCH", "/storage/v1/b/meta-bucket?ifMetagenerationMatch=1", []byte(`{"labels":{"env":"test","team":"a"}}`), 200).Body.Bytes())
	labels, _ := patched["labels"].(map[string]any)
	if patched["metageneration"] != "2" || labels["env"] != "test" || labels["team"] != "a" {
		t.Fatalf("patched bucket = %v", patched)
	}
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket?ifMetagenerationMatch=1", []byte(`{"labels":{"env":"stale"}}`), 412)
	patched = decodeBody(t, f.request(admin, "PATCH", "/storage/v1/b/meta-bucket?ifMetagenerationNotMatch=1", []byte(`{"labels":{"env":null}}`), 200).Body.Bytes())
	labels, _ = patched["labels"].(map[string]any)
	if _, found := labels["env"]; found || labels["team"] != "a" || patched["metageneration"] != "3" {
		t.Fatalf("null label was not removed: %v", patched)
	}
	got := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/meta-bucket", nil, 200).Body.Bytes())
	if labels, _ := got["labels"].(map[string]any); labels["team"] != "a" || got["metageneration"] != "3" {
		t.Fatalf("patch did not persist: %v", got)
	}
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket", []byte(`{"versioning":{"enabled":true}}`), 501)
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket", []byte(`[]`), 400)
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket", []byte(`{"labels":{"a":1}}`), 400)
	f.request("stranger@project-a.test", "PATCH", "/storage/v1/b/meta-bucket", []byte(`{"labels":{}}`), 403)
}

// TestBucketListAndDeleteReleaseState covers bucket listing isolation and the
// state a delete must release before the name is reused.
func TestBucketListAndDeleteReleaseState(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "meta-bucket", 200)
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket", []byte(`{"labels":{"team":"a"}}`), 200)

	listed := decodeBody(t, f.request(admin, "GET", "/storage/v1/b?project=project-a", nil, 200).Body.Bytes())
	items, _ := listed["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("bucket list = %v", listed)
	}
	if item, _ := items[0].(map[string]any); item["projectNumber"] != "123456" {
		t.Fatalf("listed bucket lacks provider metadata: %v", item)
	}
	peer := decodeBody(t, f.request("admin@project-b.test", "GET", "/storage/v1/b?project=project-b", nil, 200).Body.Bytes())
	if items, _ := peer["items"].([]any); len(items) != 0 {
		t.Fatalf("another project listed this bucket: %v", peer)
	}

	f.policy(t, "meta-bucket", "reader@project-b.test", "roles/storage.objectViewer")
	f.request(admin, "DELETE", "/storage/v1/b/meta-bucket", nil, 204)
	f.request(admin, "GET", "/storage/v1/b/meta-bucket", nil, 404)
	if _, found := f.p.index.Get(BucketResource("meta-bucket")); found {
		t.Fatal("deleted bucket stayed indexed")
	}
	f.bucket(testProject, "meta-bucket", 200)
	policy, err := f.p.iam.GetPolicy(BucketResource("meta-bucket"))
	if err != nil || len(policy.Bindings) != 0 {
		t.Fatalf("recreated bucket inherited grants: %+v %v", policy, err)
	}
	got := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/meta-bucket", nil, 200).Body.Bytes())
	if _, found := got["labels"]; found {
		t.Fatalf("recreated bucket inherited labels: %v", got)
	}
}

// TestBucketCreateRejectsUnsupportedConfiguration covers every create-time
// configuration the provider refuses, and the project preconditions.
func TestBucketCreateRejectsUnsupportedConfiguration(t *testing.T) {
	f := setup(t)
	base := `"location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}`
	tests := []struct {
		name, body string
		want       int
	}{
		{"unknown field", `{"name":"b-unknown",` + base + `,"website":{}}`, 501},
		{"malformed", `{"name":`, 400},
		{"no uniform access", `{"name":"b-acl","location":"US","softDeletePolicy":{"retentionDurationSeconds":"0"}}`, 501},
		{"versioning", `{"name":"b-ver",` + base + `,"versioning":{"enabled":true}}`, 501},
		{"default hold", `{"name":"b-hold",` + base + `,"defaultEventBasedHold":true}`, 501},
		{"soft delete effective time", `{"name":"b-soft","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0","effectiveTime":"2020-01-01T00:00:00Z"}}`, 501},
		{"lifecycle rules", `{"name":"b-life",` + base + `,"lifecycle":{"rule":[{}]}}`, 501},
		{"storage class", `{"name":"b-class",` + base + `,"storageClass":"NEARLINE"}`, 501},
		{"missing location", `{"name":"b-loc","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`, 400},
	}
	for _, test := range tests {
		f.request(admin, "POST", "/storage/v1/b?project=project-a", []byte(test.body), test.want)
	}
	f.request(admin, "POST", "/storage/v1/b?project=project-c", []byte(`{"name":"b-c",`+base+`}`), 501)
	f.request(admin, "GET", "/storage/v1/b?project=project-a&project=project-b", nil, 400)
	scope := state.Scope{Project: testProject, Location: state.Global}
	for _, name := range []string{"b-unknown", "b-acl", "b-ver", "b-hold", "b-soft", "b-life", "b-class", "b-loc"} {
		if _, found, _ := f.p.catalog.Bucket(scope, name); found {
			t.Fatalf("rejected bucket %s was published", name)
		}
	}
}

// TestRequestOptionValidation covers unsupported and malformed query options
// and headers, which fail before any data access.
func TestRequestOptionValidation(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "opt-bucket", 200)
	tests := []struct {
		path string
		want int
	}{
		{"/storage/v1/b/opt-bucket?userProject=x", 501},
		{"/storage/v1/b/opt-bucket?alt=media", 501},
		{"/storage/v1/b/opt-bucket/iam?optionsRequestedPolicyVersion=2", 501},
		{"/storage/v1/b/opt-bucket?projection=full", 501},
		{"/storage/v1/b/opt-bucket/o?versions=maybe", 400},
		{"/storage/v1/b/opt-bucket?prettyPrint=true&prettyPrint=false", 400},
	}
	for _, test := range tests {
		f.request(admin, "GET", test.path, nil, test.want)
	}
	f.request(admin, "GET", "/storage/v1/b/opt-bucket/iam?optionsRequestedPolicyVersion=3", nil, 200)
	f.request(admin, "GET", "/storage/v1/b/opt-bucket/o?versions=false&projection=noAcl&alt=json", nil, 200)
	f.request(admin, "GET", "/storage/v1/b/absent-bucket", nil, 404)
	f.request(admin, "GET", "/storage/v1/b/absent-bucket/o/x", nil, 404)
}

// TestBucketPolicyWritesAreValidated covers the bucket IAM write shapes the
// provider refuses and the recorded Storage error format.
func TestBucketPolicyWritesAreValidated(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "iam-bucket", 200)
	for _, body := range []string{
		`{"bindings":[],"extra":true}`,
		`{"bindings":[],"resourceId":"projects/_/buckets/other"}`,
		`{"bindings":[],"kind":"storage#bucket"}`,
	} {
		f.request(admin, "PUT", "/storage/v1/b/iam-bucket/iam", []byte(body), 400)
	}
	conditional := `{"version":1,"bindings":[{"role":"roles/storage.objectViewer","members":["serviceAccount:x@project-a.test"],"condition":{"expression":"true","title":"t"}}]}`
	denied := decodeBody(t, f.request(admin, "PUT", "/storage/v1/b/iam-bucket/iam", []byte(conditional), 400).Body.Bytes())
	if errorBody, _ := denied["error"].(map[string]any); errorBody["code"] != float64(400) {
		t.Fatalf("condition error = %v", denied)
	}
	f.request(admin, "PUT", "/storage/v1/b/iam-bucket/iam", []byte(`{"kind":"storage#policy","resourceId":"projects/_/buckets/iam-bucket","bindings":[{"role":"roles/storage.objectViewer","members":["serviceAccount:x@project-a.test"]}]}`), 200)
	read := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/iam-bucket/iam", nil, 200).Body.Bytes())
	if bindings, _ := read["bindings"].([]any); len(bindings) != 1 {
		t.Fatalf("policy = %v", read)
	}
	f.request("stranger@project-a.test", "GET", "/storage/v1/b/iam-bucket/iam", nil, 403)
	f.request("stranger@project-a.test", "PUT", "/storage/v1/b/iam-bucket/iam", []byte(`{"bindings":[]}`), 403)
}

// TestAnonymousCallersAreRefusedInStorageFormat proves anonymous requests are
// denied with Storage's 401 shape before any backend starts.
func TestAnonymousCallersAreRefusedInStorageFormat(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "anon-bucket", 200)
	body := decodeBody(t, f.request("", "GET", "/storage/v1/b/anon-bucket/o", nil, 401).Body.Bytes())
	errorBody, _ := body["error"].(map[string]any)
	message, _ := errorBody["message"].(string)
	if !strings.HasPrefix(message, "Anonymous caller") {
		t.Fatalf("anonymous denial = %v", body)
	}
}
