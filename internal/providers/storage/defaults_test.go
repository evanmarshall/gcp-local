package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

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
		{"b-retain", "&enableObjectRetention=true", string(rcloneBucket("b-retain")), "Storage coverage gap: enableObjectRetention=true"},
		{"b-retain2", "&enableObjectRetention=True", string(rcloneBucket("b-retain2")), "Storage coverage gap: enableObjectRetention=True"},
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
		"/o?softDeleted=true&maxResults=0":                                             "Invalid maxResults",
		"/o?softDeleted=maybe":                                                         "softDeleted must be a boolean",
	} {
		refused(t, f.request(admin, "GET", softBase+path, nil, 400).Body.String(), message)
	}
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
	refused(t, f.request(admin, "PATCH", base, []byte(`{"versioning":{"enabled":true}}`), 400).Body.String(),
		"Storage coverage gap: bucket updates currently support labels and softDeletePolicy only")
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
// the project. Inherited prevention, with no organization policy modeled,
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
	f.request(admin, "POST", "/storage/v1/b/gcloud-bucket/folders", []byte(`{"name":"reports"}`), 200)
	refused(t, f.request(admin, "GET", listing, nil, 400).Body.String(),
		"Storage coverage gap: includeFoldersAsPrefixes in a bucket with folders")
	// Without a "/" delimiter the option does not apply.
	f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/o?includeFoldersAsPrefixes=true", nil, 200)
	refused(t, f.request(admin, "GET", "/storage/v1/b/gcloud-bucket/o?includeFoldersAsPrefixes=yes", nil, 400).Body.String(),
		"includeFoldersAsPrefixes must be a boolean")
}
