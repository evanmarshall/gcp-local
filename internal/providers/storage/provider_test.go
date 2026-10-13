package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// testProject is the primary fixture project for Storage provider tests.
const (
	testProject = "project-a"
	peerProject = "project-b"
)

// mustMarshal encodes a fixture value or fails the test.
func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type fixture struct {
	p *Provider
	g *gateway.Gateway
	t *testing.T
}

func setup(t *testing.T) *fixture {
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{testProject, peerProject} {
		if err := index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: project, Location: state.Global}}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{{Role: "roles/storage.admin", Members: []string{"serviceAccount:admin@" + project + ".test"}}}}); err != nil {
			t.Fatal(err)
		}
	}
	registry := gateway.NewRegistry()
	manager := backend.NewManager(time.Second, time.Second)
	p, err := New(registry, manager, index, engine)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &fixture{p, g, t}
}
func (f *fixture) request(actor, method, path string, body []byte, expected int) *httptest.ResponseRecorder {
	f.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(body))
	if actor != "" {
		r.Header.Set("Authorization", "Bearer "+actor)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.g.ServeHTTP(w, r)
	if w.Code != expected {
		f.t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, expected, w.Body.String())
	}
	return w
}
func (f *fixture) bucket(project, name string, expected int) {
	body := []byte(`{"name":"` + name + `","location":"US-CENTRAL1","storageClass":"STANDARD","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`)
	f.request("admin@"+project+".test", "POST", "/storage/v1/b?project="+project, body, expected)
}
func (f *fixture) policy(t *testing.T, name, member, role string) {
	policy := iam.Policy{Bindings: []iam.Binding{}}
	if role != "" {
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: role, Members: []string{"serviceAccount:" + member}})
	}
	raw := mustMarshal(t, policy)
	f.request("admin@project-a.test", "PUT", "/storage/v1/b/"+name+"/iam", raw, 200)
}
func TestProviderIAMMutationIsolationAndReset(t *testing.T) {
	f := setup(t)
	f.request("stranger@project-a.test", "GET", "/storage/v1/b?project=project-a", nil, 403)
	if len(f.p.manager.Status()) != 0 {
		t.Fatal("denied request started backend")
	}
	f.bucket(testProject, "owned-bucket", 200)
	f.bucket(peerProject, "other-bucket", 200)
	f.bucket(peerProject, "owned-bucket", 409)
	object := "nested/a %?.json"
	path := "/storage/v1/b/owned-bucket/o/" + url.PathEscape(object)
	upload := "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=" + url.QueryEscape(object)
	f.policy(t, "owned-bucket", "caller@project-b.test", "roles/storage.objectCreator")
	f.request("caller@project-b.test", "POST", upload, []byte(`{"body":"original"}`), 200)
	denied := f.request("caller@project-b.test", "POST", upload, []byte(`{"body":"wrong"}`), 403)
	id := denied.Header().Get("X-Gcp-Local-Decision-Id")
	decision, ok := f.p.iam.Decision(id)
	if !ok || len(decision.Checks) != 2 || !decision.Checks[0].Allowed || decision.Checks[1].Permission != "storage.objects.delete" {
		t.Fatalf("wrong replacement decision: %+v", decision)
	}
	got := f.request("admin@project-a.test", "GET", path+"?alt=media", nil, 200)
	if got.Body.String() != `{"body":"original"}` {
		t.Fatal("denied replacement mutated media")
	}
	f.policy(t, "owned-bucket", "caller@project-b.test", "roles/storage.objectUser")
	f.request("caller@project-b.test", "POST", upload, []byte("updated"), 200)
	f.policy(t, "owned-bucket", "caller@project-b.test", "")
	f.request("caller@project-b.test", "DELETE", path, nil, 403)
	if f.request("admin@project-a.test", "GET", path+"?alt=media", nil, 200).Body.String() != "updated" {
		t.Fatal("revoked caller deleted object")
	}
	assertResetClearsOnlyProjectA(t, f, upload)
}

// assertResetClearsOnlyProjectA resets project-a's Storage scope and checks
// that its buckets and grants are gone while project-b's bucket survives.
func assertResetClearsOnlyProjectA(t *testing.T, f *fixture, upload string) {
	t.Helper()
	scope := state.Scope{Project: testProject, Location: state.Global}
	if err := f.p.manager.Reset(context.Background(), backend.Key{Service: "storage", Scope: scope}); err != nil {
		t.Fatal(err)
	}
	f.request("admin@project-a.test", "GET", "/storage/v1/b/owned-bucket", nil, 404)
	f.request("admin@project-b.test", "GET", "/storage/v1/b/other-bucket", nil, 200)
	f.bucket(testProject, "owned-bucket", 200)
	policy, err := f.p.iam.GetPolicy(BucketResource("owned-bucket"))
	if err != nil || len(policy.Bindings) != 0 {
		t.Fatal("reset retained bucket grants")
	}
	f.request("caller@project-b.test", "POST", upload, []byte("wrong"), 403)
	f.request("admin@project-a.test", "GET", "/storage/v1/b/owned-bucket/o", nil, 200)
}
func TestProviderFailsClosedForUnsupportedFeatures(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "owned-bucket", 200)
	// A path no route claims stays UNIMPLEMENTED; a Storage route this
	// provider does not serve is a 400 gap, which clients do not retry.
	f.request("admin@project-a.test", "GET", "/_internal/healthcheck", nil, 501)
	for _, path := range []string{"/storage/v1/b/owned-bucket/notificationConfigs", "/storage/v1/b/owned-bucket/acl"} {
		if body := f.request("admin@project-a.test", "GET", path, nil, 400).Body.String(); !strings.Contains(body, "Storage coverage gap") {
			t.Fatalf("%s: %s", path, body)
		}
	}
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=chunked&name=a", []byte(`{}`), 400)
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=a&predefinedAcl=publicRead", []byte("x"), 400)
	f.request("admin@project-a.test", "PUT", "/storage/v1/b/owned-bucket/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`), 412)
	if err := f.p.iam.MarkUnsupported(provider.ProjectResource(testProject), []string{"deny"}); err != nil {
		t.Fatal(err)
	}
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=a", []byte("x"), 400)
	if err := f.p.iam.MarkUnsupported(provider.ProjectResource(testProject), nil); err != nil {
		t.Fatal(err)
	}
	body := f.request("admin@project-a.test", "GET", "/storage/v1/b/owned-bucket/o", nil, 200).Body.String()
	if strings.Contains(body, `"name":"a"`) {
		t.Fatal("unsupported authorization forwarded a mutation")
	}
}

func TestCompetingProjectsDoNotLeaveUnindexedBuckets(t *testing.T) {
	f := setup(t)
	if counts := createContendedBucket(f); counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("unexpected create results: %v", counts)
	}
	resource, ok := f.p.index.Get(BucketResource("contended-bucket"))
	if !ok {
		t.Fatal("missing owner")
	}
	for _, project := range []string{testProject, peerProject} {
		response := f.request("admin@"+project+".test", "GET", "/storage/v1/b?project="+project, nil, 200)
		hasBucket := strings.Contains(response.Body.String(), "contended-bucket")
		if hasBucket != (resource.Scope.Project == project) {
			t.Fatal("compensation left a foreign backend bucket")
		}
		assertNoListener(t, f, project)
	}
}

// createContendedBucket races both projects to create one bucket name and
// counts the response codes.
func createContendedBucket(f *fixture) map[int]int {
	var group sync.WaitGroup
	codes := make(chan int, 2)
	for _, project := range []string{testProject, peerProject} {
		group.Add(1)
		go func(project string) {
			defer group.Done()
			body := `{"name":"contended-bucket","location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`
			r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/storage/v1/b?project="+project, strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer admin@"+project+".test")
			w := httptest.NewRecorder()
			f.g.ServeHTTP(w, r)
			codes <- w.Code
		}(project)
	}
	group.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	return counts
}

// assertNoListener checks that the project's Storage backend is in-process.
func assertNoListener(t *testing.T, f *fixture, project string) {
	t.Helper()
	err := f.p.manager.Use(context.Background(), backend.Key{Service: "storage", Scope: state.Scope{Project: project, Location: state.Global}}, func(_ context.Context, driver backend.Backend) error {
		backend, ok := driver.(*memoryBackend)
		if !ok {
			t.Error("unexpected backend type")
			return nil
		}
		if backend.server.URL() != "" {
			t.Error("API backend opened a listener")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketProjectOwnershipAndGlobalNamespace(t *testing.T) {
	index := state.NewIndex()
	catalog := NewCatalog(index)
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	bucket := Bucket{Name: "shared-name", Location: "us-central1"}
	if err := catalog.PublishBucket(a, bucket); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishBucket(b, bucket); err == nil {
		t.Fatal("bucket global name collided across projects")
	}
	if _, found, err := catalog.Bucket(b, bucket.Name); err != nil || found {
		t.Fatal("foreign project read bucket")
	}
	if catalog.PublishBucket(state.Scope{Project: a.Project, Location: "us-central1"}, bucket) == nil {
		t.Fatal("placement became a second namespace")
	}
	if catalog.PublishBucket(a, Bucket{Name: bucket.Name, Location: "europe-west1"}) == nil {
		t.Fatal("placement silently changed")
	}
	if err := catalog.ForgetBucket(b, bucket.Name); err == nil {
		t.Fatal("foreign project removed bucket index")
	}
	if resource, ok := index.Get(BucketResource(bucket.Name)); !ok || resource.Scope != a {
		t.Fatal("bucket index changed")
	}
}

func TestCatalogResetStaysInItsProject(t *testing.T) {
	index := state.NewIndex()
	catalog := NewCatalog(index)
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	bucket := Bucket{Name: "shared-name", Location: "us-central1"}
	if err := catalog.PublishBucket(a, bucket); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishBucket(b, Bucket{Name: "other-name", Location: "europe-west1"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Reset(a); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := catalog.Bucket(a, bucket.Name); found {
		t.Fatal("reset retained bucket metadata")
	}
	if _, found := index.Get(BucketResource(bucket.Name)); found {
		t.Fatal("reset retained bucket index")
	}
	if _, found, _ := catalog.Bucket(b, "other-name"); !found {
		t.Fatal("reset crossed project")
	}
	if _, found := index.Get(BucketResource("other-name")); !found {
		t.Fatal("reset crossed index scope")
	}
}

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

// Catalog fixture names.
const (
	catalogBucket = "owned"
	absentBucket  = "absent"
)

// TestFolderLifecycleAndAuthorization covers the provider-owned folders
// surface: create, conflict, get, list, delete, and denial.
func TestFolderLifecycleAndAuthorization(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "hns-bucket", 200)
	folders := "/storage/v1/b/hns-bucket/folders"
	created := decodeBody(t, f.request(admin, "POST", folders, []byte(`{"name":"reports"}`), 200).Body.Bytes())
	if created["name"] != "reports" || created["bucket"] != "hns-bucket" {
		t.Fatalf("folder = %v", created)
	}
	f.request(admin, "POST", folders, []byte(`{"name":"reports"}`), 409)
	f.request(admin, "POST", folders, []byte(`{"name":""}`), 400)
	f.request(admin, "POST", folders, []byte(`[]`), 400)
	f.request(admin, "GET", folders+"/reports", nil, 200)
	f.request(admin, "GET", folders+"/absent", nil, 404)
	f.request(admin, "POST", folders, []byte(`{"name":"archive"}`), 200)
	listed := decodeBody(t, f.request(admin, "GET", folders, nil, 200).Body.Bytes())
	if items, _ := listed["folders"].([]any); len(items) != 2 {
		t.Fatalf("folders = %v", listed)
	}

	f.request("stranger@project-a.test", "GET", folders, nil, 403)
	f.request("stranger@project-a.test", "POST", folders, []byte(`{"name":"intrusion"}`), 403)
	f.request("stranger@project-a.test", "DELETE", folders+"/reports", nil, 403)
	f.request(admin, "GET", "/storage/v1/b/absent-bucket/folders", nil, 404)

	// Folders in another project's bucket are unaffected by this bucket's state.
	f.bucket(peerProject, "peer-bucket", 200)
	peer := decodeBody(t, f.request("admin@project-b.test", "GET", "/storage/v1/b/peer-bucket/folders", nil, 200).Body.Bytes())
	if items, _ := peer["folders"].([]any); len(items) != 0 {
		t.Fatalf("another bucket listed these folders: %v", peer)
	}

	f.request(admin, "DELETE", folders+"/reports", nil, 204)
	f.request(admin, "DELETE", folders+"/reports", nil, 404)
	f.request(admin, "GET", folders+"/reports", nil, 404)
}

// TestDeletedBucketDropsItsFolders proves a bucket recreated under the same
// name does not inherit the deleted bucket's folders.
func TestDeletedBucketDropsItsFolders(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "reused-bucket", 200)
	f.request(admin, "POST", "/storage/v1/b/reused-bucket/folders", []byte(`{"name":"stale"}`), 200)
	f.request(admin, "DELETE", "/storage/v1/b/reused-bucket", nil, 204)
	f.bucket(testProject, "reused-bucket", 200)
	f.request(admin, "GET", "/storage/v1/b/reused-bucket/folders/stale", nil, 404)
}

// TestCatalogFolderAndMetadataOwnership covers the catalog's identity and
// ownership preconditions and metadata replacement directly.
func TestCatalogFolderAndMetadataOwnership(t *testing.T) {
	catalog := NewCatalog(state.NewIndex())
	a := state.Scope{Project: testProject, Location: state.Global}
	if err := catalog.UpdateMetadata(a, absentBucket, "{}"); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("update of an absent bucket = %v", err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: absentBucket, Name: "f"}); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("folder in an absent bucket = %v", err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: "x"}); !errors.Is(err, errBucketIdentity) {
		t.Fatalf("unnamed folder = %v", err)
	}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket}); !errors.Is(err, errBucketIdentity) {
		t.Fatalf("bucket without placement = %v", err)
	}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket, Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpdateMetadata(a, catalogBucket, `{"labels":{}}`); err != nil {
		t.Fatal(err)
	}
	if bucket, _, _ := catalog.Bucket(a, catalogBucket); bucket.Metadata != `{"labels":{}}` {
		t.Fatalf("metadata = %q", bucket.Metadata)
	}
	if _, _, err := catalog.Bucket(state.Scope{Project: testProject, Location: "asia-east1"}, catalogBucket); err == nil {
		t.Fatal("a regional scope was accepted for global bucket state")
	}
}

// TestCatalogFoldersStayInTheirProject covers folder publication, isolation,
// and reset.
func TestCatalogFoldersStayInTheirProject(t *testing.T) {
	catalog := NewCatalog(state.NewIndex())
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket, Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: catalogBucket, Name: "f"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishFolder(b, Folder{Bucket: catalogBucket, Name: "f"}); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("another project published into this bucket: %v", err)
	}
	if _, found, _ := catalog.Folder(b, catalogBucket, "f"); found {
		t.Fatal("another project read this folder")
	}
	if removed, _ := catalog.DeleteFolder(b, catalogBucket, "f"); removed {
		t.Fatal("another project deleted this folder")
	}
	if items, _ := catalog.Folders(a, catalogBucket); len(items) != 1 {
		t.Fatalf("folders = %v", items)
	}
	if err := catalog.Reset(a); err != nil {
		t.Fatal(err)
	}
	if items, _ := catalog.Folders(a, catalogBucket); len(items) != 0 {
		t.Fatal("reset retained folders")
	}
}

// errPayload stands for a non-status provider error whose text carries
// backend payload.
var errPayload = errors.New("payload with a secret")

// TestErrorsUseTheStorageShape checks that refusals carry the JSON API's
// errors[] list and never a retryable 5xx: a gap, an emulator fault, and a
// wrapped non-status error.
func TestErrorsUseTheStorageShape(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "shape-bucket", 200)
	body := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/shape-bucket/acl", nil, 400).Body.Bytes())
	refusal, _ := body["error"].(map[string]any)
	reasons, _ := refusal["errors"].([]any)
	first, _ := reasons[0].(map[string]any)
	if _, generic := refusal["status"]; generic || first["reason"] != "invalid" || first["domain"] != "global" {
		t.Fatalf("gap body = %v", body)
	}
	for err, want := range map[error]string{
		fault("broken invariant"): "Storage emulator fault: broken invariant",
		errPayload:                "Storage emulator fault: internal provider error",
		fmt.Errorf("forward: %w", provider.Invalid("x")): `"x"`,
	} {
		response := statusRefusal(err)
		encoded := string(mustMarshal(t, response.JSON))
		if response.Status != 400 || !strings.Contains(encoded, want) || strings.Contains(encoded, "secret") {
			t.Fatalf("%v rendered %d %s", err, response.Status, encoded)
		}
	}
}

// TestBackendServerErrorsAreFaults checks that a backend 5xx is never
// relayed: fake-gcs answers a compose of missing sources with 500.
func TestBackendServerErrorsAreFaults(t *testing.T) {
	b := &memoryBackend{}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	headers := http.Header{"Content-Type": {"application/json"}}
	if _, err := b.forward(context.Background(), "POST", "/storage/v1/b", nil, headers, []byte(`{"name":"compose-bucket"}`)); err != nil {
		t.Fatal(err)
	}
	_, err := b.forward(context.Background(), "POST", "/storage/v1/b/compose-bucket/o/out/compose", nil,
		headers, []byte(`{"sourceObjects":[{"name":"absent"}]}`))
	if response := statusRefusal(err); err == nil || response.Status != 400 || !strings.Contains(string(mustMarshal(t, response.JSON)), "HTTP 500") {
		t.Fatalf("backend 5xx = %v", err)
	}
}
