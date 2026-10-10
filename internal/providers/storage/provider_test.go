package storage

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/evanmarshall/gcp-local/internal/state"
)

// testProject is the primary fixture project for Storage provider tests.
const testProject = "project-a"

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
	for _, project := range []string{testProject, "project-b"} {
		if err := index.Put("resourcemanager", state.Resource{Name: projectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: project, Location: state.Global}}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.SetPolicy(projectResource(project), iam.Policy{Bindings: []iam.Binding{{Role: "roles/storage.admin", Members: []string{"serviceAccount:admin@" + project + ".test"}}}}); err != nil {
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
	g := gateway.New(registry, func(ctx context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &fixture{p, g, t}
}
func (f *fixture) request(actor, method, path string, body []byte, expected int) *httptest.ResponseRecorder {
	f.t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
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
func (f *fixture) policy(name, member, role string) {
	policy := iam.Policy{Bindings: []iam.Binding{}}
	if role != "" {
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: role, Members: []string{"serviceAccount:" + member}})
	}
	raw, _ := json.Marshal(policy)
	f.request("admin@project-a.test", "PUT", "/storage/v1/b/"+name+"/iam", raw, 200)
}
func TestProviderIAMMutationIsolationAndReset(t *testing.T) {
	f := setup(t)
	f.request("stranger@project-a.test", "GET", "/storage/v1/b?project=project-a", nil, 403)
	if len(f.p.manager.Status()) != 0 {
		t.Fatal("denied request started backend")
	}
	f.bucket(testProject, "owned-bucket", 200)
	f.bucket("project-b", "other-bucket", 200)
	f.bucket("project-b", "owned-bucket", 409)
	object := "nested/a %?.json"
	path := "/storage/v1/b/owned-bucket/o/" + url.PathEscape(object)
	upload := "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=" + url.QueryEscape(object)
	f.policy("owned-bucket", "caller@project-b.test", "roles/storage.objectCreator")
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
	f.policy("owned-bucket", "caller@project-b.test", "roles/storage.objectUser")
	f.request("caller@project-b.test", "POST", upload, []byte("updated"), 200)
	f.policy("owned-bucket", "caller@project-b.test", "")
	f.request("caller@project-b.test", "DELETE", path, nil, 403)
	if f.request("admin@project-a.test", "GET", path+"?alt=media", nil, 200).Body.String() != "updated" {
		t.Fatal("revoked caller deleted object")
	}
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
	for _, path := range []string{"/_internal/healthcheck", "/storage/v1/b/owned-bucket/notificationConfigs", "/storage/v1/b/owned-bucket/acl"} {
		f.request("admin@project-a.test", "GET", path, nil, 501)
	}
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=resumable&name=a", []byte(`{}`), 501)
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=a&predefinedAcl=publicRead", []byte("x"), 501)
	f.request("admin@project-a.test", "PUT", "/storage/v1/b/owned-bucket/iam", []byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`), 412)
	if err := f.p.iam.MarkUnsupported(projectResource(testProject), []string{"deny"}); err != nil {
		t.Fatal(err)
	}
	f.request("admin@project-a.test", "POST", "/upload/storage/v1/b/owned-bucket/o?uploadType=media&name=a", []byte("x"), 501)
	if err := f.p.iam.MarkUnsupported(projectResource(testProject), nil); err != nil {
		t.Fatal(err)
	}
	body := f.request("admin@project-a.test", "GET", "/storage/v1/b/owned-bucket/o", nil, 200).Body.String()
	if strings.Contains(body, `"name":"a"`) {
		t.Fatal("unsupported authorization forwarded a mutation")
	}
}

func TestCompetingProjectsDoNotLeaveUnindexedBuckets(t *testing.T) {
	f := setup(t)
	var group sync.WaitGroup
	codes := make(chan int, 2)
	for _, project := range []string{testProject, "project-b"} {
		group.Add(1)
		go func(project string) {
			defer group.Done()
			body := `{"name":"contended-bucket","location":"US-CENTRAL1","iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"},"softDeletePolicy":{"retentionDurationSeconds":"0"}}`
			r := httptest.NewRequest(http.MethodPost, "/storage/v1/b?project="+project, strings.NewReader(body))
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
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("unexpected create results: %v", counts)
	}
	resource, ok := f.p.index.Get(BucketResource("contended-bucket"))
	if !ok {
		t.Fatal("missing owner")
	}
	for _, project := range []string{testProject, "project-b"} {
		response := f.request("admin@"+project+".test", "GET", "/storage/v1/b?project="+project, nil, 200)
		hasBucket := strings.Contains(response.Body.String(), "contended-bucket")
		if hasBucket != (resource.Scope.Project == project) {
			t.Fatal("compensation left a foreign backend bucket")
		}
		err := f.p.manager.Use(context.Background(), backend.Key{Service: "storage", Scope: state.Scope{Project: project, Location: state.Global}}, func(ctx context.Context, driver backend.Backend) error {
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
}
