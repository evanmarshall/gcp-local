package storage

import (
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// bucketBody is the create body of the hammered bucket.
var bucketBody = map[string]any{
	"name": "race-bucket", "location": "US-CENTRAL1", "storageClass": "STANDARD",
	"iamConfiguration": map[string]any{"uniformBucketLevelAccess": map[string]any{"enabled": true}, "publicAccessPrevention": "enforced"},
	"softDeletePolicy": map[string]any{"retentionDurationSeconds": "0"},
}

// TestConcurrentBucketRequests races bucket insert, get, list, label patch,
// and delete, plus folder requests inside the bucket; -race fails
// it if a handler touches catalog state unlocked.
func TestConcurrentBucketRequests(t *testing.T) {
	f := setup(t)
	bucket := "/storage/v1/b/race-bucket"
	folders := bucket + "/folders"
	providertest.Hammer(t, f.g, admin, 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: "POST", Path: "/storage/v1/b?project=" + testProject, Body: bucketBody, Want: []int{200, 409}},
			{Method: "GET", Path: bucket, Want: []int{200, 404}},
			{Method: "GET", Path: "/storage/v1/b?project=" + testProject, Want: []int{200}},
			{Method: "PATCH", Path: bucket, Body: map[string]any{"labels": map[string]any{"worker": "w"}}, Want: []int{200, 404}},
			{Method: "POST", Path: folders, Body: map[string]any{"name": "reports"}, Want: []int{200, 404, 409}},
			{Method: "GET", Path: folders + "/reports", Want: []int{200, 404}},
			{Method: "GET", Path: folders, Want: []int{200, 404}},
			{Method: "DELETE", Path: folders + "/reports", Want: []int{204, 404}},
			{Method: "DELETE", Path: bucket, Want: []int{204, 404, 409}},
		}
	})
}

// TestBucketPatchIsAtomic rejects a patch with one unsupported field without
// applying its labels.
func TestBucketPatchIsAtomic(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "atomic-bucket", 200)
	f.request(admin, "PATCH", "/storage/v1/b/atomic-bucket", []byte(`{"labels":{"a":"before"}}`), 200)
	f.request(admin, "PATCH", "/storage/v1/b/atomic-bucket", []byte(`{"labels":{"a":"after"},"versioning":{"enabled":true}}`), 400)
	f.request(admin, "PATCH", "/storage/v1/b/atomic-bucket", []byte(`{"labels":{"a":7}}`), 400)
	if body := f.request(admin, "GET", "/storage/v1/b/atomic-bucket", nil, 200).Body.String(); !strings.Contains(body, `"a":"before"`) {
		t.Fatalf("a rejected patch changed the bucket: %s", body)
	}
}

// TestObjectPatchIsAtomic rejects a patch with one unsupported field without
// applying the supported one.
func TestObjectPatchIsAtomic(t *testing.T) {
	f, _ := patchFixture(t)
	f.request(admin, "PATCH", patchObjectPath, []byte(`{"contentType":"text/plain","acl":[]}`), 400)
	if body := f.request(admin, "GET", patchObjectPath, nil, 200).Body.String(); strings.Contains(body, "text/plain") || !strings.Contains(body, `"metageneration":"1"`) {
		t.Fatalf("a rejected patch changed the object: %s", body)
	}
}
