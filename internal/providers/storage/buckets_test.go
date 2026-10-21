package storage

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/protobuf/encoding/protojson"
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
	f.request(admin, "PATCH", "/storage/v1/b/meta-bucket", []byte(`{"billing":{"requesterPays":true}}`), 400)
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
		{"unknown field", `{"name":"b-unknown",` + base + `,"website":{}}`, 400},
		{"malformed", `{"name":`, 400},
		{"no uniform access", `{"name":"b-acl","location":"US","softDeletePolicy":{"retentionDurationSeconds":"0"}}`, 400},
		{"soft delete effective time", `{"name":"b-soft","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0","effectiveTime":"2020-01-01T00:00:00Z"}}`, 400},
		{"lifecycle rules", `{"name":"b-life",` + base + `,"lifecycle":{"rule":[{}]}}`, 400},
		{"storage class", `{"name":"b-class",` + base + `,"storageClass":"NEARLINE"}`, 400},
		{"missing location", `{"name":"b-loc","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`, 400},
	}
	for _, test := range tests {
		f.request(admin, "POST", "/storage/v1/b?project=project-a", []byte(test.body), test.want)
	}
	f.request(admin, "POST", "/storage/v1/b?project=project-c", []byte(`{"name":"b-c",`+base+`}`), 400)
	f.request(admin, "GET", "/storage/v1/b?project=project-a&project=project-b", nil, 400)
	scope := state.Scope{Project: testProject, Location: state.Global}
	for _, name := range []string{"b-unknown", "b-acl", "b-ver", "b-hold", "b-soft", "b-life", "b-class", "b-loc"} {
		if _, found, _ := f.p.catalog.Bucket(scope, name); found {
			t.Fatalf("rejected bucket %s was published", name)
		}
	}
}

// TestBucketPolicyOnlyAlias covers the legacy name rclone sends for uniform
// bucket-level access: accepted on create, reported under both names, and
// refused when it contradicts uniformBucketLevelAccess or is off.
func TestBucketPolicyOnlyAlias(t *testing.T) {
	f := setup(t)
	rest := `"location":"US-CENTRAL1","softDeletePolicy":{"retentionDurationSeconds":"0"}`
	created := decodeBody(t, f.request(admin, "POST", "/storage/v1/b?project=project-a",
		[]byte(`{"name":"alias-bucket",`+rest+`,"iamConfiguration":{"bucketPolicyOnly":{"enabled":true},"publicAccessPrevention":"enforced"}}`), 200).Body.Bytes())
	read := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/alias-bucket", nil, 200).Body.Bytes())
	for name, bucket := range map[string]map[string]any{"create": created, "get": read} {
		iamConfiguration, _ := bucket["iamConfiguration"].(map[string]any)
		uniform, _ := iamConfiguration["uniformBucketLevelAccess"].(map[string]any)
		legacy, _ := iamConfiguration["bucketPolicyOnly"].(map[string]any)
		if uniform["enabled"] != true || legacy["enabled"] != true || uniform["lockedTime"] == nil || uniform["lockedTime"] != legacy["lockedTime"] {
			t.Fatalf("%s response does not report both names: %v", name, bucket)
		}
	}
	refusals := []struct{ name, iam, message string }{
		{"b-disagree", `{"uniformBucketLevelAccess":{"enabled":true},"bucketPolicyOnly":{"enabled":false},"publicAccessPrevention":"enforced"}`,
			"uniformBucketLevelAccess and bucketPolicyOnly disagree"},
		{"b-alias-off", `{"bucketPolicyOnly":{"enabled":false},"publicAccessPrevention":"enforced"}`,
			"explicit uniform bucket-level access is required; ACLs are not modeled"},
	}
	for _, refusal := range refusals {
		body := f.request(admin, "POST", "/storage/v1/b?project=project-a",
			[]byte(`{"name":"`+refusal.name+`",`+rest+`,"iamConfiguration":`+refusal.iam+`}`), 400).Body.String()
		if !strings.Contains(body, refusal.message) {
			t.Fatalf("%s: got %s, want %q", refusal.name, body, refusal.message)
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
		{"/storage/v1/b/opt-bucket?userProject=x", 400},
		{"/storage/v1/b/opt-bucket?alt=media", 400},
		{"/storage/v1/b/opt-bucket/iam?optionsRequestedPolicyVersion=2", 400},
		{"/storage/v1/b/opt-bucket?projection=full", 200}, {"/storage/v1/b/opt-bucket?projection=custom", 400},
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

// rcloneBucket is the bucket insert body rclone 1.75.2 sends, which leaves
// public access prevention and soft delete at GCP's defaults.
func rcloneBucket(name string) []byte {
	return []byte(`{"name":"` + name + `","location":"us-central1","iamConfiguration":{"bucketPolicyOnly":{"enabled":true}}}`)
}

// refused asserts a response body carries the refusal message.
func refused(t *testing.T, body, message string) {
	t.Helper()
	if !strings.Contains(body, message) {
		t.Fatalf("got %s, want %q", body, message)
	}
}

func softPolicy(t *testing.T, bucket map[string]any) map[string]any {
	t.Helper()
	policy, _ := bucket["softDeletePolicy"].(map[string]any)
	return policy
}

// expectf fails the test with the message unless ok.
// nested returns a JSON object field of a decoded document.
func nested(t *testing.T, document map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := document[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object: %v", key, document)
	}
	return value
}

func expectf(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

// TestBucketCreateDefaults covers the bucket inserts rclone and gcloud send:
// GCP's inherited public access prevention and 7-day soft delete, as recorded
// in storage-bucket-defaults and storage-gcloud-bucket-defaults.
func TestBucketCreateDefaults(t *testing.T) {
	f := setup(t)
	created := decodeBody(t, f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("rclone-bucket"), 200).Body.Bytes())
	read := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/rclone-bucket", nil, 200).Body.Bytes())
	for name, bucket := range map[string]map[string]any{"create": created, "get": read} {
		iamConfiguration, _ := bucket["iamConfiguration"].(map[string]any)
		policy := softPolicy(t, bucket)
		_, retention := bucket["objectRetention"]
		expectf(t, iamConfiguration["publicAccessPrevention"] == publicInherited && policy["retentionDurationSeconds"] == "604800" &&
			policy["effectiveTime"] != nil && policy["effectiveTime"] == bucket["timeCreated"] && !retention,
			"%s: bucket does not report GCP's defaults: %v", name, bucket)
	}
	gcloud := `{"name":"gcloud-bucket","location":"us-central1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}}}`
	f.request(admin, "POST", "/storage/v1/b?project=project-a&enableObjectRetention=False&projection=noAcl", []byte(gcloud), 200)
	f.request(admin, "POST", "/storage/v1/b?project=project-a&enableObjectRetention=false", rcloneBucket("lower-bucket"), 200)
	// The longest and shortest durations are accepted, and so is disabling.
	for name, seconds := range map[string]string{"b-min": "604800", "b-max": "7776000", "b-off": "0"} {
		body := `{"name":"` + name + `","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}},"softDeletePolicy":{"retentionDurationSeconds":"` + seconds + `"}}`
		policy := softPolicy(t, decodeBody(t, f.request(admin, "POST", "/storage/v1/b?project=project-a", []byte(body), 200).Body.Bytes()))
		_, effective := policy["effectiveTime"]
		expectf(t, policy["retentionDurationSeconds"] == seconds && effective == (seconds != "0"), "%s policy = %v", name, policy)
	}
}

// TestBucketCreateRefusals covers the bucket settings still refused, each
// with its message, and that no refused bucket is published.
func TestBucketCreateRefusals(t *testing.T) {
	f := setup(t)
	refusals := []struct{ name, query, body, message string }{
		{"b-retain", "&enableObjectRetention=maybe", string(rcloneBucket("b-retain")), "enableObjectRetention must be a boolean"},
		{"b-policy", "", `{"name":"b-policy","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}},"retentionPolicy":{"retentionPeriod":"0"}}`,
			"Retention policy must have a retention period greater than 0 and less than 100 years."},
		{"b-pap", "", `{"name":"b-pap","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"unspecified"}}`,
			"Storage coverage gap: publicAccessPrevention unspecified"},
		{"b-short", "", `{"name":"b-short","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}},"softDeletePolicy":{"retentionDurationSeconds":"3600"}}`,
			"Soft delete policy must have a retention duration between 7 days and 90 days."},
		{"b-long", "", `{"name":"b-long","location":"US","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}},"softDeletePolicy":{"retentionDurationSeconds":"7776001"}}`,
			"Soft delete policy must have a retention duration between 7 days and 90 days."},
		{"b-acl", "", `{"name":"b-acl","location":"US"}`, "Storage coverage gap: explicit uniform bucket-level access is required; ACLs are not modeled"},
	}
	scope := state.Scope{Project: testProject, Location: state.Global}
	for _, test := range refusals {
		refused(t, f.request(admin, "POST", "/storage/v1/b?project=project-a"+test.query, []byte(test.body), 400).Body.String(), test.message)
		_, found, _ := f.p.catalog.Bucket(scope, test.name)
		expectf(t, !found, "refused bucket %s was published", test.name)
	}
}

// softBucket creates a default bucket and soft-deletes one object in it,
// returning the object's metadata.
func softBucket(t *testing.T, f *fixture) map[string]any {
	t.Helper()
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("soft-bucket"), 200)
	first := decodeBody(t, f.request(admin, "POST", softUpload+"deleted.txt", []byte("soft-deleted evidence\n"), 200).Body.Bytes())
	generation, _ := first["generation"].(string)
	f.request(admin, "DELETE", softBase+"/o/deleted.txt?generation="+generation, nil, 204)
	return first
}

const (
	softBase   = "/storage/v1/b/soft-bucket"
	softUpload = "/upload/storage/v1/b/soft-bucket/o?uploadType=media&name="
)

// TestSoftDeletedReads covers how a soft-deleted object reads: hidden from
// live reads and listings, listed and read by generation with softDeleted=true
// (storage-bucket-defaults), and the read options that are gaps.
func TestSoftDeletedReads(t *testing.T) {
	f := setup(t)
	first := softBucket(t, f)
	generation, _ := first["generation"].(string)
	f.request(admin, "GET", softBase+"/o/deleted.txt", nil, 404)
	live := decodeBody(t, f.request(admin, "GET", softBase+"/o", nil, 200).Body.Bytes())
	expectf(t, live["items"] == nil, "live listing shows a soft-deleted object: %v", live)
	soft := softItems(t, f, softBase)
	expectf(t, len(soft) == 1 && soft[0]["name"] == "deleted.txt" && soft[0]["generation"] == generation, "soft-deleted listing = %v", soft)
	assertRetained(t, soft[0])
	got := decodeBody(t, f.request(admin, "GET", softBase+"/o/deleted.txt?softDeleted=true&generation="+generation, nil, 200).Body.Bytes())
	expectf(t, got["md5Hash"] == first["md5Hash"] && got["metageneration"] == "1", "soft-deleted read = %v", got)
	assertRetained(t, got)
	f.request(admin, "GET", softBase+"/o/deleted.txt?softDeleted=true&generation=1", nil, 404)
	prefixed := decodeBody(t, f.request(admin, "GET", softBase+"/o?softDeleted=true&prefix=del", nil, 200).Body.Bytes())
	expectf(t, len(itemList(prefixed["items"])) == 1, "prefix listing = %v", prefixed)
	for path, message := range map[string]string{
		"/o/deleted.txt?softDeleted=true":                                              "A generation is required to read a soft-deleted object",
		"/o/deleted.txt?softDeleted=true&alt=media&generation=" + generation:           "Storage coverage gap: soft-deleted object media",
		"/o/deleted.txt?softDeleted=true&ifGenerationMatch=1&generation=" + generation: "Storage coverage gap: softDeleted read with ifGenerationMatch",
		"/o?softDeleted=true&delimiter=/":                                              "Storage coverage gap: softDeleted listing with delimiter",
		"/o?softDeleted=true&versions=true":                                            "Storage coverage gap: softDeleted listing with versions",
		"/o?softDeleted=true&maxResults=-1":                                            "Invalid unsigned integer value: '-1'.",
		"/o?softDeleted=maybe":                                                         "softDeleted must be a boolean",
	} {
		refused(t, f.request(admin, "GET", softBase+path, nil, 400).Body.String(), message)
	}
	empty := decodeBody(t, f.request(admin, "GET", softBase+"/o?softDeleted=true&maxResults=0", nil, 200).Body.Bytes())
	expectf(t, len(itemList(empty["items"])) == 0, "maxResults=0 listing = %v", empty)
	// Soft-deleted objects expire at their hard-delete time.
	scope := state.Scope{Project: testProject, Location: state.Global}
	expired := f.p.catalog.softDeletedObjects(scope, "soft-bucket", time.Now().Add(8*24*time.Hour))
	expectf(t, len(expired) == 0, "expired soft-deleted objects remain: %v", expired)
}

// TestSoftDeleteRestore covers objects.restore: a new live generation with
// the soft-deleted bytes, the soft-deleted copy kept, the recorded denial,
// and the restore options that are refused.
func TestSoftDeleteRestore(t *testing.T) {
	f := setup(t)
	first := softBucket(t, f)
	generation, _ := first["generation"].(string)
	restore := softBase + "/o/deleted.txt/restore?generation=" + generation
	denied := f.request("stranger@project-a.test", "POST", restore, nil, 403).Body.String()
	refused(t, denied, "stranger@project-a.test does not have storage.objects.restore access to the Google Cloud Storage object. Permission 'storage.objects.restore' denied on resource '//storage.googleapis.com/projects/_/buckets/soft-bucket/objects/deleted.txt' (or it may not exist).")
	restored := decodeBody(t, f.request(admin, "POST", restore, nil, 200).Body.Bytes())
	expectf(t, restored["generation"] != generation && restored["metageneration"] == "1" && restored["softDeleteTime"] == nil, "restored object = %v", restored)
	content := f.request(admin, "GET", softBase+"/o/deleted.txt?alt=media", nil, 200).Body.String()
	expectf(t, content == "soft-deleted evidence\n", "restored bytes = %q", content)
	soft := softItems(t, f, softBase)
	expectf(t, len(soft) == 1 && soft[0]["generation"] == generation, "restore removed the soft-deleted copy: %v", soft)
	refused(t, f.request(admin, "POST", softBase+"/o/deleted.txt/restore", nil, 400).Body.String(), "Required parameter: generation")
	refused(t, f.request(admin, "POST", restore+"&restoreToken=x", nil, 400).Body.String(), "Storage coverage gap: unsupported query option restoreToken")
	refused(t, f.request(admin, "POST", restore+"&copySourceAcl=true", nil, 400).Body.String(), "Storage coverage gap: copySourceAcl=true")
	f.request(admin, "POST", restore+"&ifGenerationMatch=0", nil, 412)
	f.request(admin, "POST", softBase+"/o/absent.txt/restore?generation="+generation, nil, 404)
	// Restoring over the live object soft-deletes it.
	f.request(admin, "POST", restore, nil, 200)
	expectf(t, len(softItems(t, f, softBase)) == 2, "restore over a live object did not retain it")
}

// TestSoftDeleteOverwritesAndBucketDelete covers the other ways a generation
// is removed (overwrite, rewrite), the one-page listing limit, and a bucket
// delete that only soft-deleted objects do not block (rclone rmdir).
func TestSoftDeleteOverwritesAndBucketDelete(t *testing.T) {
	f := setup(t)
	softBucket(t, f)
	f.request(admin, "POST", softUpload+"replaced.txt", []byte("first version\n"), 200)
	f.request(admin, "POST", softUpload+"replaced.txt", []byte("second version\n"), 200)
	f.request(admin, "POST", softUpload+"source.txt", []byte("source\n"), 200)
	f.request(admin, "POST", softBase+"/o/source.txt/rewriteTo/b/soft-bucket/o/replaced.txt", nil, 200)
	names := map[string]int{}
	for _, item := range softItems(t, f, softBase) {
		name, _ := item["name"].(string)
		names[name]++
	}
	expectf(t, names["deleted.txt"] == 1 && names["replaced.txt"] == 2, "soft-deleted after overwrite and rewrite = %v", names)
	refused(t, f.request(admin, "GET", softBase+"/o?softDeleted=true&maxResults=1", nil, 400).Body.String(),
		"Storage coverage gap: soft-deleted listings longer than one page")
	f.request(admin, "DELETE", softBase+"/o/replaced.txt", nil, 204)
	f.request(admin, "DELETE", softBase+"/o/source.txt", nil, 204)
	f.request(admin, "DELETE", softBase, nil, 204)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("soft-bucket"), 200)
	soft := softItems(t, f, softBase)
	expectf(t, len(soft) == 0, "recreated bucket inherited soft-deleted objects: %v", soft)
}

// TestSoftDeletePolicyPatch covers changing the retention: refused outside
// GCP's bounds, and once disabled a soft-deleted listing is refused, new
// deletes are final, and restore is a gap (storage-gcloud-bucket-defaults).
func TestSoftDeletePolicyPatch(t *testing.T) {
	f := setup(t)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("policy-bucket"), 200)
	base := "/storage/v1/b/policy-bucket"
	upload := "/upload/storage/v1/b/policy-bucket/o?uploadType=media&name="
	refused(t, f.request(admin, "PATCH", base, []byte(`{"softDeletePolicy":{"retentionDurationSeconds":"3600"}}`), 400).Body.String(),
		"Soft delete policy must have a retention duration between 7 days and 90 days.")
	refused(t, f.request(admin, "PATCH", base, []byte(`{"softDeletePolicy":{"effectiveTime":"2020-01-01T00:00:00Z"}}`), 400).Body.String(),
		"Storage coverage gap: softDeletePolicy updates set retentionDurationSeconds only")
	refused(t, f.request(admin, "PATCH", base, []byte(`{"billing":{"requesterPays":true}}`), 400).Body.String(),
		"Storage coverage gap: bucket updates of billing")
	first := decodeBody(t, f.request(admin, "POST", upload+"kept.txt", []byte("kept"), 200).Body.Bytes())
	generation, _ := first["generation"].(string)
	f.request(admin, "DELETE", base+"/o/kept.txt", nil, 204)

	patched := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"softDeletePolicy":{"retentionDurationSeconds":"0"}}`), 200).Body.Bytes())
	policy := softPolicy(t, patched)
	if _, effective := policy["effectiveTime"]; policy["retentionDurationSeconds"] != "0" || effective || patched["metageneration"] != "2" {
		t.Fatalf("disabled policy = %v", patched)
	}
	refused(t, f.request(admin, "GET", base+"/o?softDeleted=true", nil, 400).Body.String(),
		"Soft delete policy is required to list soft-deleted versions")
	refused(t, f.request(admin, "POST", base+"/o/kept.txt/restore?generation="+generation, nil, 400).Body.String(),
		"Storage coverage gap: restore in a bucket without a soft-delete policy")
	// An object soft-deleted before the change keeps its hard-delete time.
	f.request(admin, "GET", base+"/o/kept.txt?softDeleted=true&generation="+generation, nil, 200)
	f.request(admin, "POST", upload+"final.txt", []byte("final"), 200)
	f.request(admin, "DELETE", base+"/o/final.txt", nil, 204)

	enabled := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"softDeletePolicy":{"retentionDurationSeconds":"1209600"}}`), 200).Body.Bytes())
	if policy := softPolicy(t, enabled); policy["retentionDurationSeconds"] != "1209600" || policy["effectiveTime"] == nil {
		t.Fatalf("re-enabled policy = %v", enabled)
	}
	for _, item := range softItems(t, f, base) {
		if item["name"] == "final.txt" {
			t.Fatalf("a delete under a disabled policy was retained: %v", item)
		}
	}
}

// TestBucketSoftDeleteIsAGap covers the bucket-level soft-delete surface,
// which is not modeled: deleted buckets are deleted outright, and the calls
// that would show or restore a soft-deleted bucket are refused.
func TestBucketSoftDeleteIsAGap(t *testing.T) {
	f := setup(t)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("gone-bucket"), 200)
	refused(t, f.request(admin, "GET", "/storage/v1/b?project=project-a&softDeleted=true", nil, 400).Body.String(),
		"Storage coverage gap: unsupported query option softDeleted")
	refused(t, f.request(admin, "GET", "/storage/v1/b/gone-bucket?softDeleted=true&generation=1", nil, 400).Body.String(),
		"Storage coverage gap: unsupported query option")
	refused(t, f.request(admin, "POST", "/storage/v1/b/gone-bucket/restore?generation=1", nil, 400).Body.String(),
		"Storage coverage gap: storage.buckets.restore is not implemented")
}

// TestPublicAccessPrevention covers allUsers and allAuthenticatedUsers on
// uniform buckets. Enforced prevention refuses them with GCP's recorded 412
// (storage-public-access-enforced) and overrides public grants inherited from
// the project. Inherited prevention, with no organization policy declared,
// accepts them and they grant access; that path is modeled locally, not
// recorded.
func TestPublicAccessPrevention(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "enforced-bucket", 200)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("public-bucket"), 200)
	for _, bucket := range []string{"enforced-bucket", "public-bucket"} {
		f.request(admin, "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name=public.txt", []byte("public evidence\n"), 200)
	}
	for _, member := range []string{"allUsers", "allAuthenticatedUsers"} {
		body := `{"bindings":[{"role":"roles/storage.objectViewer","members":["` + member + `"]}]}`
		refused(t, f.request(admin, "PUT", "/storage/v1/b/enforced-bucket/iam", []byte(body), 412).Body.String(),
			"The member bindings allUsers and allAuthenticatedUsers are not allowed since public access prevention is enforced.")
	}

	f.request(admin, "PUT", "/storage/v1/b/public-bucket/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`), 200)
	if f.request("", "GET", "/storage/v1/b/public-bucket/o/public.txt?alt=media", nil, 200).Body.String() != "public evidence\n" {
		t.Fatal("allUsers did not grant an anonymous read")
	}
	f.request("", "GET", "/public-bucket/public.txt", nil, 200)
	refused(t, f.request("", "POST", "/upload/storage/v1/b/public-bucket/o?uploadType=media&name=anon.txt", []byte("x"), 401).Body.String(),
		"Anonymous caller does not have storage.objects.create access")
	f.request("", "GET", "/storage/v1/b/enforced-bucket/o/public.txt", nil, 401)

	f.request(admin, "PUT", "/storage/v1/b/public-bucket/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allAuthenticatedUsers"]}]}`), 200)
	f.request("stranger@project-a.test", "GET", "/storage/v1/b/public-bucket/o/public.txt", nil, 200)
	f.request("", "GET", "/storage/v1/b/public-bucket/o/public.txt", nil, 401)
	f.request("stranger@project-a.test", "GET", "/storage/v1/b/enforced-bucket/o/public.txt", nil, 403)

	// A public grant inherited from the project grants nothing on an
	// enforced bucket, and still grants on an inherited one.
	if _, err := f.p.iam.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/storage.admin", Members: []string{"serviceAccount:" + admin}},
		{Role: "roles/storage.objectViewer", Members: []string{"allUsers"}},
	}}); err != nil {
		t.Fatal(err)
	}
	f.request("", "GET", "/storage/v1/b/enforced-bucket/o/public.txt", nil, 401)
	f.request("stranger@project-a.test", "GET", "/storage/v1/b/enforced-bucket/o/public.txt", nil, 403)
	f.request("", "GET", "/storage/v1/b/public-bucket/o/public.txt", nil, 200)
	f.request(admin, "GET", "/storage/v1/b/enforced-bucket/o/public.txt", nil, 200)
}

// declarePolicy stores one organization policy on project-a.
func declarePolicy(t *testing.T, f *fixture, constraint, spec string) {
	t.Helper()
	policy := &orgpolicypb.Policy{}
	if err := protojson.Unmarshal([]byte(`{"name":"projects/project-a/policies/`+constraint+`","spec":`+spec+`}`), policy); err != nil {
		t.Fatal(err)
	}
	if err := f.p.iam.DeclareOrgPolicy(policy, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// TestOrganizationPoliciesAtTheMutation covers the Storage constraints after
// IAM allows a request: uniform access on create and update and a policy
// member outside the permitted customers (recorded:
// orgpolicy-constraint-enforcement), and the bucket location (modeled). The
// trace names each verdict apart from the IAM checks.
func TestOrganizationPoliciesAtTheMutation(t *testing.T) {
	f := setup(t)
	declarePolicy(t, f, "storage.uniformBucketLevelAccess", `{"rules":[{"enforce":true}]}`)
	declarePolicy(t, f, "gcp.resourceLocations", `{"rules":[{"values":{"allowedValues":["in:us-locations"]}}]}`)
	declarePolicy(t, f, "iam.allowedPolicyMemberDomains", `{"rules":[{"values":{"allowedValues":["C0example"]}}]}`)
	create := "/storage/v1/b?project=project-a"
	disabled := `{"name":"disabled","location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":false}}}`
	response := f.request(admin, "POST", create, []byte(disabled), 412)
	refused(t, response.Body.String(), "Request violates constraint 'constraints/storage.uniformBucketLevelAccess'")
	traced, _ := f.p.iam.Decision(response.Header().Get("X-Gcp-Local-Decision-Id"))
	if len(traced.Signals) != 1 || traced.Signals[0].Verdict != "violation" || !traced.Checks[0].Allowed {
		t.Fatalf("trace = %+v", traced)
	}
	omitted := `{"name":"omitted","location":"US-CENTRAL1","iamConfiguration":{"publicAccessPrevention":"inherited"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`
	created := decodeBody(t, f.request(admin, "POST", create, []byte(omitted), 200).Body.Bytes())
	access, _ := created["iamConfiguration"].(map[string]any)
	uniform, _ := access["uniformBucketLevelAccess"].(map[string]any)
	expectf(t, uniform["enabled"] == true && access["publicAccessPrevention"] == publicInherited, "omitted access = %v", access)
	refused(t, f.request(admin, "POST", create, []byte(`{"name":"europe","location":"EUROPE-WEST1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}}}`), 412).Body.String(),
		"Request violates constraint 'constraints/gcp.resourceLocations'")
	refused(t, f.request(admin, "PATCH", "/storage/v1/b/omitted", []byte(`{"iamConfiguration":{"uniformBucketLevelAccess":{"enabled":false}}}`), 412).Body.String(),
		"Request violates constraint 'constraints/storage.uniformBucketLevelAccess'")
	refused(t, f.request(admin, "PUT", "/storage/v1/b/omitted/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`), 412).Body.String(),
		"One or more users named in the policy do not belong to a permitted customer.")
	// Without uniform access enforced, an omitted setting stays a coverage gap.
	declarePolicy(t, f, "storage.uniformBucketLevelAccess", `{"reset":true}`)
	refused(t, f.request(admin, "POST", create, []byte(`{"name":"gap","location":"US-CENTRAL1"}`), 400).Body.String(),
		"explicit uniform bucket-level access is required")
}

// TestPredefinedACLsUnderUniformPolicy covers gcsfs's mkdir: predefined ACLs
// on a create that names no access setting, which GCP accepts when the
// organization enforces uniform access, making the bucket uniform and
// returning no ACLs (recorded: storage-client-defaults). Every other
// predefined ACL stays a gap.
func TestPredefinedACLsUnderUniformPolicy(t *testing.T) {
	f := setup(t)
	create := "/storage/v1/b?project=project-a&predefinedAcl=projectPrivate&predefinedDefaultObjectAcl=bucketOwnerFullControl"
	body := func(name string) []byte { return []byte(`{"name":"` + name + `","location":"US-CENTRAL1"}`) }
	refused(t, f.request(admin, "POST", create, body("no-policy"), 400).Body.String(), "explicit uniform bucket-level access is required")
	declarePolicy(t, f, "storage.uniformBucketLevelAccess", `{"rules":[{"enforce":true}]}`)
	created := decodeBody(t, f.request(admin, "POST", create, body("gcsfs"), 200).Body.Bytes())
	access, _ := created["iamConfiguration"].(map[string]any)
	uniform, _ := access["uniformBucketLevelAccess"].(map[string]any)
	_, acl := created["acl"]
	_, objectACL := created["defaultObjectAcl"]
	expectf(t, uniform["enabled"] == true && !acl && !objectACL, "created = %v", created)
	refused(t, f.request(admin, "POST", "/storage/v1/b?project=project-a&predefinedAcl=publicRead", body("public"), 400).Body.String(),
		"predefinedAcl=publicRead; ACLs are not modeled")
	explicit := []byte(`{"name":"explicit","location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}}}`)
	refused(t, f.request(admin, "POST", create, explicit, 400).Body.String(),
		"predefined ACLs with an explicit uniform bucket-level access setting")
}

// TestOrganizationPublicAccessPrevention covers storage.publicAccessPrevention
// over buckets that inherit prevention (modeled, not recorded: the sandbox
// doesn't enforce it): public members are refused and public grants grant
// nothing until the policy is removed.
func TestOrganizationPublicAccessPrevention(t *testing.T) {
	f := setup(t)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("inherits"), 200)
	f.request(admin, "POST", "/upload/storage/v1/b/inherits/o?uploadType=media&name=public.txt", []byte("public\n"), 200)
	if _, err := f.p.iam.SetPolicy(BucketResource("inherits"), iam.Policy{Bindings: []iam.Binding{{Role: "roles/storage.objectViewer", Members: []string{"allUsers"}}}}); err != nil {
		t.Fatal(err)
	}
	f.request("", "GET", "/storage/v1/b/inherits/o/public.txt", nil, 200)
	declarePolicy(t, f, "storage.publicAccessPrevention", `{"rules":[{"enforce":true}]}`)
	f.request("", "GET", "/storage/v1/b/inherits/o/public.txt", nil, 401)
	refused(t, f.request(admin, "PUT", "/storage/v1/b/inherits/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allAuthenticatedUsers"]}]}`), 412).Body.String(),
		"The member bindings allUsers and allAuthenticatedUsers are not allowed since public access prevention is enforced.")
	if !f.p.iam.DeleteOrgPolicy(provider.ProjectResource(testProject), "storage.publicAccessPrevention") {
		t.Fatal("the policy was not stored")
	}
	f.request("", "GET", "/storage/v1/b/inherits/o/public.txt", nil, 200)
}

func softItems(t *testing.T, f *fixture, base string) []map[string]any {
	t.Helper()
	listing := decodeBody(t, f.request(admin, "GET", base+"/o?softDeleted=true", nil, 200).Body.Bytes())
	return itemList(listing["items"])
}

// assertRetained checks the recorded lifecycle fields of a soft-deleted
// object: both times, seven days apart, and no timeDeleted.
func assertRetained(t *testing.T, object map[string]any) {
	t.Helper()
	soft, _ := object["softDeleteTime"].(string)
	hard, _ := object["hardDeleteTime"].(string)
	start, err := time.Parse(time.RFC3339, soft)
	if err != nil {
		t.Fatalf("softDeleteTime %q: %v", soft, err)
	}
	end, err := time.Parse(time.RFC3339, hard)
	if err != nil || end.Sub(start) != 7*24*time.Hour {
		t.Fatalf("hardDeleteTime %q is not seven days after %q", hard, soft)
	}
	if _, found := object["timeDeleted"]; found {
		t.Fatalf("soft-deleted object reports timeDeleted: %v", object)
	}
	self, _ := object["selfLink"].(string)
	expectf(t, strings.HasPrefix(self, "http://") && object["mediaLink"] != nil, "soft-deleted object lacks links: %v", object)
}

// TestGcloudObjectRequests covers the requests gcloud 588.0.0 adds to an
// object workflow (storage-gcloud-object-requests): the bucket storage
// layout, apitools' single-quoted multipart boundary, and
// includeFoldersAsPrefixes on a "/"-delimited listing.
func TestGcloudObjectRequests(t *testing.T) {
	f := setup(t)
	f.request(admin, "POST", "/storage/v1/b?project=project-a", rcloneBucket("gcloud-bucket"), 200)
	layout := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/storageLayout?alt=json", nil, 200).Body.Bytes())
	_, namespace := layout["hierarchicalNamespace"]
	expectf(t, layout["kind"] == "storage#storageLayout" && layout["bucket"] == "gcloud-bucket" && layout["location"] == "US-CENTRAL1" &&
		layout["locationType"] == "region" && !namespace, "storage layout = %v", layout)
	refused(t, f.request("stranger@project-a.test", "GET", "/storage/v1/b/gcloud-bucket/storageLayout", nil, 403).Body.String(),
		"stranger@project-a.test does not have storage.objects.list access to the Google Cloud Storage bucket. Permission 'storage.objects.list' denied on resource '//storage.googleapis.com/projects/_/buckets/gcloud-bucket' (or it may not exist).")
	refused(t, f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/storageLayout?prefix=a/", nil, 400).Body.String(),
		"Storage coverage gap: unsupported query option prefix")

	upload := "/upload/storage/v1/b/gcloud-bucket/o?alt=json&uploadType=multipart"
	boundary := "===============0123456789=="
	body := "--" + boundary + "\nContent-Type: application/json\nMIME-Version: 1.0\n\n{\"name\": \"gcloud.txt\"}\n--" + boundary +
		"\nContent-Type: text/plain\nMIME-Version: 1.0\nContent-Transfer-Encoding: binary\n\nhello from gcloud\n\n--" + boundary + "--"
	stored := decodeBody(t, f.rawRequest("POST", upload, "multipart/related; boundary='"+boundary+"'", nil, []byte(body), 200).Body.Bytes())
	expectf(t, stored["name"] == "gcloud.txt" && stored["contentType"] == "text/plain", "apitools multipart upload = %v", stored)
	for _, contentType := range []string{"multipart/related; boundary='" + boundary, "multipart/mixed; boundary='" + boundary + "'"} {
		refused(t, f.rawRequest("POST", upload, contentType, nil, []byte(body), 400).Body.String(), "Expected multipart/related upload")
	}

	listing := "/storage/v1/b/gcloud-bucket/o?alt=json&delimiter=%2F&includeFoldersAsPrefixes=True&maxResults=1000&projection=noAcl&versions=False"
	listed := decodeBody(t, f.request(admin, "GET", listing, nil, 200).Body.Bytes())
	items := itemList(listed["items"])
	expectf(t, len(items) == 1 && items[0]["name"] == "gcloud.txt", "folders-as-prefixes listing = %v", listed)
	refused(t, f.request(admin, "POST", "/storage/v1/b/gcloud-bucket/folders", []byte(`{"name":"reports"}`), 409).Body.String(),
		"The bucket does not support hierarchical namespace.")
	// Without a "/" delimiter the option does not apply.
	f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/o?includeFoldersAsPrefixes=true", nil, 200)
	refused(t, f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/o?includeFoldersAsPrefixes=yes", nil, 400).Body.String(),
		"includeFoldersAsPrefixes must be a boolean")
}

// listPage is one page of a Storage list: entry names, prefixes, and the
// next page's token.
type listPage struct {
	items, prefixes []string
	next            string
}

// listPages follows a listing's page tokens to its end.
func listPages(t *testing.T, f *fixture, path string) []listPage {
	t.Helper()
	var pages []listPage
	token := ""
	for range 10 {
		target := path
		if token != "" {
			target += "&pageToken=" + url.QueryEscape(token)
		}
		body := decodeBody(t, f.request(admin, "GET", target, nil, 200).Body.Bytes())
		page := listPage{}
		for _, item := range itemList(body["items"]) {
			page.items = append(page.items, documentName(item))
		}
		prefixes, _ := body["prefixes"].([]any)
		for _, prefix := range prefixes {
			text, _ := prefix.(string)
			page.prefixes = append(page.prefixes, text)
		}
		page.next, _ = body["nextPageToken"].(string)
		pages = append(pages, page)
		if token = page.next; token == "" {
			return pages
		}
	}
	t.Fatalf("%s did not end", path)
	return nil
}

// TestObjectListPagingMatchesGCP replays storage-list-paging: a delimiter
// prefix is one entry toward maxResults, no page repeats a prefix, offsets
// select objects before they collapse, and GCP's refusals.
func TestObjectListPagingMatchesGCP(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "list-bucket", 200)
	for _, name := range []string{"e", "a-1", "d-1", "a-2", "c", "f-1", "b-1"} {
		f.request(admin, "POST", "/upload/storage/v1/b/list-bucket/o?uploadType=media&name="+name, []byte(name), 200)
	}
	base := "/storage/v1/b/list-bucket/o?"
	for query, want := range map[string]string{
		"delimiter=-&maxResults=2":                  "[{[] [a- b-]} {[c] [d-]} {[e] [f-]}]",
		"delimiter=-":                               "[{[c e] [a- b- d- f-]}]",
		"maxResults=3":                              "[{[a-1 a-2 b-1] []} {[c d-1 e] []} {[f-1] []}]",
		"prefix=a&delimiter=-":                      "[{[] [a-]}]",
		"delimiter=-&startOffset=a-2&endOffset=d-1": "[{[c] [a- b-]}]",
		"delimiter=--&prefix=a":                     "[{[a-1 a-2] []}]",
		"maxResults=0":                              "[{[] []}]",
		"maxResults=1001&delimiter=-&undefined=1":   "[{[c e] [a- b- d- f-]}]",
	} {
		pages := listPages(t, f, base+query)
		got := make([]string, 0, len(pages))
		for _, page := range pages {
			got = append(got, fmt.Sprintf("{%v %v}", page.items, page.prefixes))
		}
		expectf(t, "["+strings.Join(got, " ")+"]" == want, "%s: pages %v, want %s", query, got, want)
	}
	for query, message := range map[string]string{
		"pageToken=not-a-page-token": "Next page token not valid: not-a-page-token",
		"maxResults=-1":              "Invalid unsigned integer value: '-1'.",
		"maxResults=many":            "Invalid unsigned integer value: 'many'.",
	} {
		refused(t, f.request(admin, "GET", base+query, nil, 400).Body.String(), message)
	}
	// A token resumes after its last entry, so an object added before it
	// does not shift the next page.
	first := listPages(t, f, base+"maxResults=2")[0]
	f.request(admin, "POST", "/upload/storage/v1/b/list-bucket/o?uploadType=media&name=0", []byte("0"), 200)
	second := decodeBody(t, f.request(admin, "GET", base+"maxResults=2&pageToken="+url.QueryEscape(first.next), nil, 200).Body.Bytes())
	listed := itemList(second["items"])[0]
	expectf(t, documentName(listed) == "b-1", "page after a-2 = %v", second)
	// rclone downloads a listed object from its mediaLink.
	expectf(t, strings.HasPrefix(fmt.Sprint(listed["mediaLink"]), "http://example.com/download/storage/v1/b/list-bucket/o/b-1?"), "listed object links = %v", listed)
}

// TestBucketListPagingAndPartialSuccess pages buckets by name, lists nothing
// after a foreign token as GCP does, and serves returnPartialSuccess=true.
func TestBucketListPagingAndPartialSuccess(t *testing.T) {
	f := setup(t)
	for _, name := range []string{"page-c", "page-a", "page-b"} {
		f.bucket(testProject, name, 200)
	}
	base := "/storage/v1/b?project=project-a"
	pages := listPages(t, f, base+"&maxResults=2&projection=full&returnPartialSuccess=true")
	got := make([]string, 0, len(pages))
	for _, page := range pages {
		got = append(got, fmt.Sprint(page.items))
	}
	expectf(t, fmt.Sprint(got) == "[[page-a page-b] [page-c]]", "bucket pages = %v", got)
	first := itemList(decodeBody(t, f.request(admin, "GET", base, nil, 200).Body.Bytes())["items"])[0]
	expectf(t, first["selfLink"] == "http://example.com/storage/v1/b/page-a", "listed bucket links = %v", first)
	all := decodeBody(t, f.request(admin, "GET", base+"&maxResults=0&prefix=page-", nil, 200).Body.Bytes())
	_, unreachable := all["unreachable"]
	expectf(t, len(itemList(all["items"])) == 3 && !unreachable && all["kind"] == "storage#buckets", "maxResults=0 = %v", all)
	foreign := decodeBody(t, f.request(admin, "GET", base+"&pageToken=not-a-page-token", nil, 200).Body.Bytes())
	expectf(t, len(itemList(foreign["items"])) == 0, "foreign token = %v", foreign)
	refused(t, f.request(admin, "GET", base+"&maxResults=-1", nil, 400).Body.String(), "Invalid unsigned integer value: '-1'.")
	refused(t, f.request(admin, "GET", base+"&returnPartialSuccess=maybe", nil, 400).Body.String(), "returnPartialSuccess must be a boolean")
}

// TestRetentionPolicyAndHolds follows storage-retention-holds: a retention
// policy and each hold refuse deletes and overwrites with GCP's errors, a
// default event-based hold reaches new objects, and releasing an event-based
// hold starts the retention period.
func TestRetentionPolicyAndHolds(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "retained", 200)
	base := "/storage/v1/b/retained"
	upload := "/upload/storage/v1/b/retained/o?uploadType=media&name="
	policy := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":{"retentionPeriod":"3600"}}`), 200).Body.Bytes())
	expectf(t, fmt.Sprint(nested(t, policy, "retentionPolicy")["retentionPeriod"]) == "3600" && policy[keyDefaultHold] == nil, "policy: %v", policy)
	for body, message := range map[string]string{
		`{"retentionPolicy":{"retentionPeriod":"0"}}`:    "Retention policy must have a retention period greater than 0 and less than 100 years.",
		`{"retentionPolicy":{"retentionPeriod":"soon"}}`: `Parse Error: Invalid value for TYPE_INT64 field: '\"soon\"'.`, // JSON-escaped
	} {
		refused(t, f.request(admin, "PATCH", base, []byte(body), 400).Body.String(), message)
	}
	kept := decodeBody(t, f.request(admin, "POST", upload+"kept", []byte("kept"), 200).Body.Bytes())
	created, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(kept["timeCreated"]))
	expiry, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(kept["retentionExpirationTime"]))
	expectf(t, expiry.Equal(created.Add(time.Hour)), "retentionExpirationTime %v, created %v", expiry, created)
	retained := "Object 'retained/kept' is subject to bucket's retention policy or object retention and cannot be deleted or overwritten until "
	refused(t, f.request(admin, "DELETE", base+"/o/kept", nil, 403).Body.String(), retained)
	refused(t, f.request(admin, "POST", upload+"kept", []byte("again"), 403).Body.String(), retained)
	f.request(admin, "PATCH", base+"/o/kept", []byte(`{"metadata":{"note":"kept"}}`), 200)
	f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":null}`), 200)
	f.request(admin, "DELETE", base+"/o/kept", nil, 204)

	f.request(admin, "POST", upload+"held", []byte("held"), 200)
	for _, hold := range []struct{ field, label string }{{"temporaryHold", "Temporary"}, {"eventBasedHold", "Event-Based"}} {
		set := decodeBody(t, f.request(admin, "PATCH", base+"/o/held", []byte(`{"`+hold.field+`":true}`), 200).Body.Bytes())
		expectf(t, set[hold.field] == true, "%s not set: %v", hold.field, set)
		message := "Object 'retained/held' is under active " + hold.label + " hold and cannot be deleted, overwritten or archived until hold is removed."
		refused(t, f.request(admin, "DELETE", base+"/o/held", nil, 403).Body.String(), message)
		refused(t, f.request(admin, "POST", upload+"held", []byte("again"), 403).Body.String(), message)
		f.request(admin, "PATCH", base+"/o/held", []byte(`{"`+hold.field+`":false}`), 200)
	}
	f.request(admin, "DELETE", base+"/o/held", nil, 204)

	f.request(admin, "PATCH", base, []byte(`{"defaultEventBasedHold":true}`), 200)
	defaulted := decodeBody(t, f.request(admin, "POST", upload+"defaulted", []byte("defaulted"), 200).Body.Bytes())
	expectf(t, defaulted[keyEventBasedHold] == true, "default hold missing: %v", defaulted)
	f.request(admin, "PATCH", base, []byte(`{"defaultEventBasedHold":false}`), 200)
	later := decodeBody(t, f.request(admin, "POST", upload+"later", []byte("later"), 200).Body.Bytes())
	expectf(t, later[keyEventBasedHold] == nil, "hold after the default was cleared: %v", later)

	f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":{"retentionPeriod":"3600"}}`), 200)
	held := decodeBody(t, f.request(admin, "PATCH", base+"/o/defaulted", []byte(`{"metadata":{"k":"v"}}`), 200).Body.Bytes())
	expectf(t, held["retentionExpirationTime"] == nil, "retention runs during an event-based hold: %v", held)
	released := decodeBody(t, f.request(admin, "PATCH", base+"/o/defaulted", []byte(`{"eventBasedHold":false}`), 200).Body.Bytes())
	updated, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(released["updated"]))
	expiry, _ = time.Parse(time.RFC3339Nano, fmt.Sprint(released["retentionExpirationTime"]))
	expectf(t, expiry.Equal(updated.Add(time.Hour)), "release did not start retention: %v", released)
}

// TestBucketLock follows storage-bucket-lock: locking needs
// ifMetagenerationMatch and an unlocked policy, and a locked policy can't be
// removed or shortened.
func TestBucketLock(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "locked", 200)
	base := "/storage/v1/b/locked"
	lock := base + "/lockRetentionPolicy"
	refused(t, f.request(admin, "POST", lock+"?ifMetagenerationMatch=1", nil, 400).Body.String(), "Bucket 'locked' does not have an unlocked retention policy.")
	configured := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":{"retentionPeriod":"5"}}`), 200).Body.Bytes())
	metageneration := fmt.Sprint(configured["metageneration"])
	refused(t, f.request(admin, "POST", lock, nil, 400).Body.String(), "Required parameter: ifMetagenerationMatch")
	f.request(admin, "POST", lock+"?ifMetagenerationMatch=99", nil, 412)
	f.request("stranger@project-a.test", "POST", lock+"?ifMetagenerationMatch="+metageneration, nil, 403)
	locked := decodeBody(t, f.request(admin, "POST", lock+"?ifMetagenerationMatch="+metageneration, nil, 200).Body.Bytes())
	expectf(t, nested(t, locked, "retentionPolicy")["isLocked"] == true, "not locked: %v", locked)
	refused(t, f.request(admin, "POST", lock+"?ifMetagenerationMatch="+fmt.Sprint(locked["metageneration"]), nil, 400).Body.String(),
		"Bucket 'locked' does not have an unlocked retention policy.")
	refused(t, f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":null}`), 403).Body.String(), "Bucket 'locked' has a locked Retention Policy which cannot be removed.")
	refused(t, f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":{"retentionPeriod":"1"}}`), 403).Body.String(),
		"Cannot reduce retention duration of a locked Retention Policy for bucket 'locked'.")
	same := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"retentionPolicy":{"retentionPeriod":"5"}}`), 200).Body.Bytes())
	expectf(t, nested(t, same, "retentionPolicy")["isLocked"] == true, "lock lost: %v", same)
	f.request(admin, "DELETE", base, nil, 204)
}

// TestLifecycleConfiguration follows storage-lifecycle-rules: rules round-trip
// as set, invalid ones are refused with GCP's errors, and no rules is no
// lifecycle field.
func TestLifecycleConfiguration(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "life", 200)
	base := "/storage/v1/b/life"
	rules := `[{"action":{"type":"Delete"},"condition":{"age":0,"matchesPrefix":["logs/"]}},` +
		`{"action":{"storageClass":"NEARLINE","type":"SetStorageClass"},"condition":{"age":60,"matchesStorageClass":["STANDARD"]}},` +
		`{"action":{"type":"Delete"},"condition":{"daysSinceNoncurrentTime":7,"isLive":false,"numNewerVersions":2}}]`
	set := decodeBody(t, f.request(admin, "PATCH", base, []byte(`{"lifecycle":{"rule":`+rules+`}}`), 200).Body.Bytes())
	read := decodeBody(t, f.request(admin, "GET", base, nil, 200).Body.Bytes())
	for _, document := range []map[string]any{set, read} {
		encoded := mustMarshal(t, nested(t, document, "lifecycle")["rule"])
		expectf(t, string(encoded) == rules, "lifecycle %s, want %s", encoded, rules)
	}
	refused(t, f.request(admin, "PATCH", base, []byte(`{"lifecycle":{"rule":[{"action":{"type":"SetStorageClass","storageClass":"MULTI_REGIONAL"},"condition":{"age":1}}]}}`), 400).Body.String(),
		"Project 123456 may not create storageClass MULTI_REGIONAL buckets with locationConstraint US-CENTRAL1.")
	refused(t, f.request(admin, "PATCH", base, []byte(`{"lifecycle":{"rule":[{"condition":{"age":1}}]}}`), 400).Body.String(), "Lifecycle rules must have an action.")
	// gcloud sends the bucket's name with each update.
	f.request(admin, "PATCH", base, []byte(`{"name":"life","lifecycle":{"rule":`+rules+`}}`), 200)
	refused(t, f.request(admin, "PATCH", base, []byte(`{"name":"other"}`), 400).Body.String(), "Bucket name does not match the patch target")
	for _, cleared := range []string{`{"lifecycle":{"rule":[]}}`, `{"lifecycle":null}`} {
		f.request(admin, "PATCH", base, []byte(`{"lifecycle":{"rule":`+rules+`}}`), 200)
		patched := decodeBody(t, f.request(admin, "PATCH", base, []byte(cleared), 200).Body.Bytes())
		_, kept := patched["lifecycle"]
		expectf(t, !kept, "%s kept the lifecycle: %v", cleared, patched)
	}
	created := `{"name":"life-created","location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}},"lifecycle":{"rule":` + rules + `}}`
	fresh := decodeBody(t, f.request(admin, "POST", "/storage/v1/b?project=project-a", []byte(created), 200).Body.Bytes())
	encoded := mustMarshal(t, nested(t, fresh, "lifecycle")["rule"])
	expectf(t, string(encoded) == rules, "created lifecycle %s", encoded)
}

// TestLifecycleCustomTime follows storage-lifecycle-rules: customTime moves
// only forward and null leaves it.
func TestLifecycleCustomTime(t *testing.T) {
	f, _ := patchFixture(t)
	set := decodeBody(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"customTime":"2026-01-01T00:00:00Z"}`), 200).Body.Bytes())
	expectf(t, set[keyCustomTime] == "2026-01-01T00:00:00Z", "customTime: %v", set[keyCustomTime])
	refused(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"customTime":"2025-01-01T00:00:00Z"}`), 400).Body.String(),
		"Custom time cannot be decreased. Previously: 2026-01-01T00:00:00+00:00. Attempting to set: 2025-01-01T00:00:00+00:00.")
	kept := decodeBody(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"customTime":null}`), 200).Body.Bytes())
	expectf(t, kept[keyCustomTime] == "2026-01-01T00:00:00Z", "null changed customTime: %v", kept[keyCustomTime])
	later := decodeBody(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"customTime":"2026-02-01T12:30:00.5Z"}`), 200).Body.Bytes())
	expectf(t, later[keyCustomTime] == "2026-02-01T12:30:00.500Z", "customTime: %v", later[keyCustomTime])
}

// TestLifecycleSweep applies Delete and SetStorageClass on the provider's
// clock, never to a held object, and deletes noncurrent generations by their
// newer versions.
func TestLifecycleSweep(t *testing.T) {
	now := time.Now()
	f := setupWithClock(t, func() time.Time { return now })
	f.bucket(testProject, "swept", 200)
	upload := "/upload/storage/v1/b/swept/o?uploadType=media&name="
	for _, name := range []string{"logs/old", "logs/held", "keep"} {
		f.request(admin, "POST", upload+name, []byte(name), 200)
	}
	f.request(admin, "PATCH", "/storage/v1/b/swept/o/logs%2Fheld", []byte(`{"temporaryHold":true}`), 200)
	f.request(admin, "PATCH", "/storage/v1/b/swept", []byte(`{"lifecycle":{"rule":[`+
		`{"action":{"type":"Delete"},"condition":{"age":1,"matchesPrefix":["logs/"]}},`+
		`{"action":{"type":"SetStorageClass","storageClass":"NEARLINE"},"condition":{"age":2}}]}}`), 200)
	f.p.sweepLifecycle(t.Context())
	f.request(admin, "GET", "/storage/v1/b/swept/o/logs%2Fold", nil, 200)
	now = now.Add(25 * time.Hour)
	f.p.sweepLifecycle(t.Context())
	f.request(admin, "GET", "/storage/v1/b/swept/o/logs%2Fold", nil, 404)
	f.request(admin, "GET", "/storage/v1/b/swept/o/logs%2Fheld", nil, 200)
	now = now.Add(24 * time.Hour)
	f.p.sweepLifecycle(t.Context())
	moved := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/swept/o/keep", nil, 200).Body.Bytes())
	expectf(t, moved["storageClass"] == "NEARLINE" && moved["timeStorageClassUpdated"] == storageTime(now), "class not changed: %v", moved)

	f.bucket(testProject, "pruned", 200)
	f.request(admin, "PATCH", "/storage/v1/b/pruned", []byte(`{"versioning":{"enabled":true},"lifecycle":{"rule":[`+
		`{"action":{"type":"Delete"},"condition":{"isLive":false,"numNewerVersions":1}}]}}`), 200)
	for _, body := range []string{"one", "two", "three"} {
		f.request(admin, "POST", "/upload/storage/v1/b/pruned/o?uploadType=media&name=doc", []byte(body), 200)
	}
	f.p.sweepLifecycle(t.Context())
	listing := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/pruned/o?versions=true", nil, 200).Body.Bytes())
	expectf(t, len(itemList(listing["items"])) == 1, "noncurrent generations kept: %v", listing)
}

// published collects the messages the provider publishes through Pub/Sub.
func published(t *testing.T, f *fixture) chan *pubsubpb.PublishRequest {
	t.Helper()
	messages := make(chan *pubsubpb.PublishRequest, 32)
	if err := f.p.registry.Register(publishMethod, func(call gateway.Call, raw any) (any, error) {
		request, ok := raw.(*pubsubpb.PublishRequest)
		if !ok || call.Principal.Subject != "serviceAccount:service-123456@gs-project-accounts.iam.gserviceaccount.com" {
			t.Errorf("publish as %s of %T", call.Principal.Subject, raw)
		}
		messages <- request
		return &pubsubpb.PublishResponse{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return messages
}

// receive waits for count messages and returns their attributes and payloads.
func receive(t *testing.T, messages chan *pubsubpb.PublishRequest, count int) []*pubsubpb.PubsubMessage {
	t.Helper()
	var found []*pubsubpb.PubsubMessage
	for len(found) < count {
		select {
		case request := <-messages:
			found = append(found, request.GetMessages()...)
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of %d messages", len(found), count)
		}
	}
	return found
}

// notifiedBucket creates a bucket, its topic in the index, and a grant of the
// Storage service agent on it, and returns the configurations path and the
// published messages.
func notifiedBucket(t *testing.T, f *fixture, grant bool) (string, chan *pubsubpb.PublishRequest) {
	t.Helper()
	f.bucket(testProject, "notified", 200)
	messages := published(t, f)
	if err := f.p.index.Put("pubsub", state.Resource{Name: notifiedTopic, Service: "pubsub", Kind: "topic", Parent: provider.ProjectResource(testProject),
		Scope: state.Scope{Project: testProject, Location: state.Global}}); err != nil {
		t.Fatal(err)
	}
	if grant {
		if _, err := f.p.iam.SetPolicy(notifiedTopic, iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{"serviceAccount:" + storageAgentEmail}}}}); err != nil {
			t.Fatal(err)
		}
	}
	return "/storage/v1/b/notified/notificationConfigs", messages
}

const (
	notifiedTopic     = "//pubsub.googleapis.com/projects/project-a/topics/events"
	storageAgentEmail = "service-123456@gs-project-accounts.iam.gserviceaccount.com"
)

// TestNotificationConfigurations follows storage-notifications: the service
// agent, the publisher check, and configuration validation, listing, reads,
// and deletes.
func TestNotificationConfigurations(t *testing.T) {
	f := setup(t)
	agent := decodeBody(t, f.request(admin, "GET", "/storage/v1/projects/project-a/serviceAccount", nil, 200).Body.Bytes())
	expectf(t, agent["email_address"] == storageAgentEmail, "agent: %v", agent)
	refused(t, f.request("stranger@project-a.test", "GET", "/storage/v1/projects/project-a/serviceAccount", nil, 403).Body.String(),
		"stranger@project-a.test does not have resourcemanager.projects.get access to the Google Cloud project.")
	f.bucket(testProject, "unindexed", 200)
	refused(t, f.request(admin, "POST", "/storage/v1/b/unindexed/notificationConfigs", []byte(`{"topic":"`+notifiedTopic+`","payload_format":"NONE"}`), 400).Body.String(),
		"Cloud Pub/Sub topic '"+notifiedTopic+"' not found, or user '"+storageAgentEmail+"' does not have permission to it.")
	configs, _ := notifiedBucket(t, f, false)
	refused(t, f.request(admin, "POST", configs, []byte(`{"topic":"`+notifiedTopic+`","payload_format":"NONE"}`), 403).Body.String(),
		"The service account '"+storageAgentEmail+"' does not have permission to publish messages to "+"to the Cloud Pub/Sub topic '"+notifiedTopic+"', or that topic does not exist.")
	if _, err := f.p.iam.SetPolicy(notifiedTopic, iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.publisher", Members: []string{"serviceAccount:" + storageAgentEmail}}}}); err != nil {
		t.Fatal(err)
	}
	filtered := decodeBody(t, f.request(admin, "POST", configs, []byte(`{"topic":"`+notifiedTopic+`","payload_format":"JSON_API_V1",`+
		`"event_types":["OBJECT_FINALIZE","OBJECT_EXPLODE"],"object_name_prefix":"n-"}`), 200).Body.Bytes())
	expectf(t, fmt.Sprint(filtered["event_types"]) == "[OBJECT_FINALIZE]", "unknown event types kept: %v", filtered)
	short := decodeBody(t, f.request(admin, "POST", configs, []byte(`{"topic":"projects/project-a/topics/events","payload_format":"NONE"}`), 200).Body.Bytes())
	expectf(t, short["topic"] == notifiedTopic && short["event_types"] == nil, "short topic: %v", short)
	for body, message := range map[string]string{
		`{"topic":"` + notifiedTopic + `","payload_format":"XML"}`: "You must specify a payload format in the 'payload_format' field.",
		`{"topic":"not-a-topic","payload_format":"NONE"}`:          "Invalid Google Cloud Pub/Sub topic. It should look like '//pubsub.googleapis.com/projects/*/topics/*.'",
		`{"payload_format":"NONE"}`:                                "You must specify a Cloud Pub/Sub topic in the 'topic' field.",
	} {
		refused(t, f.request(admin, "POST", configs, []byte(body), 400).Body.String(), message)
	}
	listing := decodeBody(t, f.request(admin, "GET", configs, nil, 200).Body.Bytes())
	expectf(t, len(itemList(listing["items"])) == 2, "list: %v", listing)
	f.request(admin, "GET", configs+"/1", nil, 200)
	refused(t, f.request(admin, "GET", configs+"/9", nil, 404).Body.String(), "The requested resource was not found.")
	f.request(admin, "DELETE", configs+"/1", nil, 204)
	f.request(admin, "DELETE", configs+"/1", nil, 404)
}

// eventsOf returns each received message's event type and payload format
// with the overwrite attribute it carries.
func eventsOf(messages []*pubsubpb.PubsubMessage) map[string]string {
	events := map[string]string{}
	for _, message := range messages {
		attributes := message.GetAttributes()
		events[attributes["eventType"]+"/"+attributes["payloadFormat"]] = attributes["overwroteGeneration"] + attributes["overwrittenByGeneration"]
	}
	return events
}

// TestNotificationEvents follows storage-notifications: object changes
// publish to each matching configuration as the Storage service agent.
func TestNotificationEvents(t *testing.T) {
	f := setup(t)
	configs, messages := notifiedBucket(t, f, true)
	f.request(admin, "POST", configs, []byte(`{"topic":"`+notifiedTopic+`","payload_format":"JSON_API_V1","custom_attributes":{"source":"test"},`+
		`"event_types":["OBJECT_FINALIZE","OBJECT_DELETE"],"object_name_prefix":"n-"}`), 200)
	f.request(admin, "POST", configs, []byte(`{"topic":"`+notifiedTopic+`","payload_format":"NONE"}`), 200)
	upload := "/upload/storage/v1/b/notified/o?uploadType=media&name=n-1"
	first := decodeBody(t, f.request(admin, "POST", upload, []byte("one"), 200).Body.Bytes())
	for _, message := range receive(t, messages, 2) {
		attributes := message.GetAttributes()
		expectf(t, attributes["eventType"] == "OBJECT_FINALIZE" && attributes["objectGeneration"] == first[keyGeneration], "attributes: %v", attributes)
		var payload map[string]any
		if attributes["payloadFormat"] == "JSON_API_V1" {
			expectf(t, json.Unmarshal(message.GetData(), &payload) == nil && payload["selfLink"] != nil, "payload: %s", message.GetData())
			expectf(t, attributes["source"] == "test" && attributes["notificationConfig"] == "projects/_/buckets/notified/notificationConfigs/1", "attributes: %v", attributes)
		}
	}
	second := decodeBody(t, f.request(admin, "POST", upload, []byte("two"), 200).Body.Bytes())
	events := eventsOf(receive(t, messages, 4))
	expectf(t, events["OBJECT_FINALIZE/JSON_API_V1"] == first[keyGeneration] && events["OBJECT_DELETE/NONE"] == second[keyGeneration], "overwrite events: %v", events)
	f.request(admin, "PATCH", "/storage/v1/b/notified/o/n-1", []byte(`{"metadata":{"k":"v"}}`), 200)
	expectf(t, eventsOf(receive(t, messages, 1))["OBJECT_METADATA_UPDATE/NONE"] == "", "no metadata update")
	f.request(admin, "PATCH", "/storage/v1/b/notified", []byte(`{"versioning":{"enabled":true}}`), 200)
	f.request(admin, "DELETE", "/storage/v1/b/notified/o/n-1", nil, 204)
	archived := eventsOf(receive(t, messages, 1))
	_, ok := archived["OBJECT_ARCHIVE/NONE"]
	expectf(t, ok, "a versioned delete did not archive: %v", archived)
}

// TestHierarchicalNamespace follows storage-hierarchical-namespace: the
// bucket needs uniform access, reports the namespace in its layout, creates
// the folders above an uploaded object, and lists folders as prefixes.
func TestHierarchicalNamespace(t *testing.T) {
	f := setup(t)
	refused(t, f.request(admin, "POST", "/storage/v1/b?project=project-a", []byte(`{"name":"no-uniform","location":"US","hierarchicalNamespace":{"enabled":true}}`), 400).Body.String(),
		"Hierarchical namespace buckets must use uniform bucket-level access.")
	f.namespaceBucket(testProject, "tree")
	bucket := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/tree", nil, 200).Body.Bytes())
	layout := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/tree/storageLayout", nil, 200).Body.Bytes())
	for _, document := range []map[string]any{bucket, layout} {
		expectf(t, fmt.Sprint(document[keyNamespace]) == "map[enabled:true]", "namespace: %v", document)
	}
	f.bucket(testProject, "flat", 200)
	flat := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/flat/storageLayout", nil, 200).Body.Bytes())
	_, named := flat[keyNamespace]
	expectf(t, !named, "a flat bucket reports a namespace: %v", flat)
	folders := "/storage/v1/b/tree/folders"
	f.request(admin, "POST", folders, []byte(`{"name":"f1/"}`), 200)
	f.request(admin, "POST", "/upload/storage/v1/b/tree/o?uploadType=media&name=g%2Fh%2Fobj", []byte("nested"), 200)
	listed := decodeBody(t, f.request(admin, "GET", folders, nil, 200).Body.Bytes())
	items := itemList(listed["items"])
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, fmt.Sprint(item[keyName]))
	}
	expectf(t, fmt.Sprint(names) == "[f1/ g/ g/h/]", "folders: %v", names)
	refused(t, f.request(admin, "DELETE", folders+"/g%2Fh%2F", nil, 409).Body.String(), "The folder you tried to delete is not empty.")
	objects := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/tree/o?delimiter=/&includeFoldersAsPrefixes=True", nil, 200).Body.Bytes())
	expectf(t, fmt.Sprint(objects["prefixes"]) == "[f1/ g/]" && objects["items"] == nil, "folders as prefixes: %v", objects)
	nested := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/tree/o?delimiter=/&prefix=g/&includeFoldersAsPrefixes=true", nil, 200).Body.Bytes())
	expectf(t, fmt.Sprint(nested["prefixes"]) == "[g/h/]", "nested prefixes: %v", nested)
	f.request(admin, "DELETE", "/storage/v1/b/tree/o/g%2Fh%2Fobj", nil, 204)
	f.request(admin, "DELETE", "/storage/v1/b/tree", nil, 204)
}
