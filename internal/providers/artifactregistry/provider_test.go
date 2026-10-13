package artifactregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

const (
	testProject  = "demo-ar"
	testRepo     = "demo-repo"
	testToken    = "actor"
	testLocation = "us-central1"
	testBase     = "/v1/projects/" + testProject + "/locations/" + testLocation + "/repositories"
	testActor    = "serviceAccount:actor@" + testProject + ".iam.gserviceaccount.com"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	engine   *iam.Engine
	provider *Provider
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	if err := index.Put("resourcemanager", state.Resource{
		Name: "//cloudresourcemanager.googleapis.com/projects/" + testProject, Service: "resourcemanager",
		Kind: "project", Number: "123456", Scope: state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/artifactregistry.admin", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	p, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	// Memorystore's operations share the path; a stand-in answers them.
	if err := registry.Register("redis.projects.locations.operations.get", func(gateway.Call, any) (any, error) {
		return &gateway.RESTResponse{JSON: map[string]any{"owner": "redis"}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, engine: engine, provider: p}
}

// invoke issues one REST request as the fixture principal.
func (h *harness) invoke(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			h.t.Fatalf("encode the request body: %v", err)
		}
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	if encoded != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// object decodes a JSON response body.
func object(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
	return body
}

// expect fails unless the response has the status and its body, or its
// error message, contains text.
func expect(t *testing.T, recorder *httptest.ResponseRecorder, code int, text string) {
	t.Helper()
	var wire struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &wire)
	if recorder.Code != code || !strings.Contains(recorder.Body.String(), text) && !strings.Contains(wire.Error.Message, text) {
		t.Fatalf("status %d body %s; want %d containing %q", recorder.Code, recorder.Body.String(), code, text)
	}
}

// create makes one DOCKER repository in base and waits for its operation.
func (h *harness) create(base, id string) {
	h.t.Helper()
	recorder := h.invoke(http.MethodPost, base+"?repositoryId="+id, map[string]any{"format": "DOCKER"})
	if recorder.Code != 200 {
		h.t.Fatalf("create %s: %d %s", id, recorder.Code, recorder.Body.String())
	}
	h.wait(object(h.t, recorder))
}

// wait polls an operation until it is done and returns it.
func (h *harness) wait(operation map[string]any) map[string]any {
	h.t.Helper()
	for range 50 {
		if operation["done"] == true {
			return operation
		}
		time.Sleep(20 * time.Millisecond)
		operation = object(h.t, h.invoke(http.MethodGet, "/v1/"+provider.Text(operation[keyName]), nil))
	}
	h.t.Fatalf("operation never finished: %v", operation)
	return nil
}

func TestRepositoryLifecycle(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{
		"format": "DOCKER", "description": "demo", "createTime": "2020-01-01T00:00:00Z",
	})
	if recorder.Code != 200 {
		t.Fatalf("repository create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	// GCP's create operation starts not done and omits done (recorded).
	started := object(t, recorder)
	if _, present := started["done"]; present || !strings.HasPrefix(provider.Text(started[keyName]), "projects/"+testProject+"/locations/"+testLocation+"/operations/artifactregistry-") {
		t.Fatalf("create operation = %v", started)
	}
	done := h.wait(started)
	if response, _ := done["response"].(map[string]any); response["@type"] != repositoryType || response["createTime"] != nil {
		t.Fatalf("create response = %v", done)
	}
	expect(t, h.invoke(http.MethodGet, testBase+"/"+testRepo, nil), 200, `"registryUri":"us-central1-docker.pkg.dev/`)
	expect(t, h.invoke(http.MethodGet, testBase, nil), 200, testRepo)
	expect(t, h.invoke(http.MethodPatch, testBase+"/"+testRepo+"?updateMask=description", map[string]any{"description": "updated"}), 200, `"description":"updated"`)
	expect(t, h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"}), 409, "ALREADY_EXISTS")
	deleted := h.wait(object(t, h.invoke(http.MethodDelete, testBase+"/"+testRepo, nil)))
	if response, _ := deleted["response"].(map[string]any); response["@type"] != emptyType {
		t.Fatalf("delete operation = %v", deleted)
	}
	expect(t, h.invoke(http.MethodGet, testBase+"/"+testRepo, nil), 404, "Requested entity was not found.")
}

// A missing operation is GCP's 403, not 404 (recorded), and a Memorystore
// operation on the same path still reaches Memorystore.
func TestOperationsRouteByPrefix(t *testing.T) {
	h := newHarness(t)
	name := "projects/" + testProject + "/locations/" + testLocation + "/operations/artifactregistry-missing"
	expect(t, h.invoke(http.MethodGet, "/v1/"+name, nil), 403, "Permission denied on operation "+name+" (or it may not exist)")
	recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/locations/"+testLocation+"/operations/operation-1", nil)
	expect(t, recorder, 200, "redis")
}

// The same repository ID in two locations is two repositories: separate
// records, names, lists, policies, and resets.
func TestRepositoriesAreKeyedByLocation(t *testing.T) {
	h := newHarness(t)
	east := "/v1/projects/" + testProject + "/locations/us-east1/repositories"
	h.create(testBase, testRepo)
	h.create(east, testRepo)
	expect(t, h.invoke(http.MethodGet, east+"/"+testRepo, nil), 200, `"name":"projects/`+testProject+`/locations/us-east1/repositories/`+testRepo+`"`)
	expect(t, h.invoke(http.MethodGet, east+"/"+testRepo, nil), 200, `"registryUri":"us-east1-docker.pkg.dev/`)
	member := "serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"
	expect(t, h.invoke(http.MethodPost, east+"/"+testRepo+":setIamPolicy", map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/artifactregistry.reader", "members": []any{member}}},
	}}), 200, member)
	if body := h.invoke(http.MethodGet, testBase+"/"+testRepo+":getIamPolicy", nil).Body.String(); strings.Contains(body, member) {
		t.Fatalf("the us-east1 grant reached us-central1: %s", body)
	}
	h.wait(object(t, h.invoke(http.MethodDelete, east+"/"+testRepo, nil)))
	expect(t, h.invoke(http.MethodGet, testBase+"/"+testRepo, nil), 200, testLocation)
	if body := h.invoke(http.MethodGet, testBase, nil).Body.String(); strings.Count(body, `"name"`) != 1 {
		t.Fatalf("us-central1 list = %s", body)
	}
	if err := h.provider.Reset(state.Scope{Project: testProject, Location: "us-east1"}); err != nil {
		t.Fatal(err)
	}
	expect(t, h.invoke(http.MethodGet, testBase+"/"+testRepo, nil), 200, testRepo)
}

// Lists are in name order and paged with opaque tokens; the refusals are
// GCP's (recorded).
func TestListOrderAndPaging(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"repo-c", "repo-a", "repo-b"} {
		h.create(testBase, id)
	}
	names := func(recorder *httptest.ResponseRecorder) []string {
		items := provider.MapList(object(t, recorder)["repositories"])
		listed := make([]string, 0, len(items))
		for _, item := range items {
			listed = append(listed, provider.LastSegment(provider.Text(item[keyName])))
		}
		return listed
	}
	first := h.invoke(http.MethodGet, testBase+"?pageSize=2", nil)
	token, _ := object(t, first)["nextPageToken"].(string)
	second := h.invoke(http.MethodGet, testBase+"?pageSize=2&pageToken="+token, nil)
	if got := append(names(first), names(second)...); strings.Join(got, ",") != "repo-a,repo-b,repo-c" || token == "" {
		t.Fatalf("pages = %v token %q", got, token)
	}
	if got := names(h.invoke(http.MethodGet, testBase+"?orderBy=name%20desc", nil)); strings.Join(got, ",") != "repo-c,repo-b,repo-a" {
		t.Fatalf("descending = %v", got)
	}
	expect(t, h.invoke(http.MethodGet, testBase+"?pageToken=bogus", nil), 400, "Request contains an invalid argument.")
	expect(t, h.invoke(http.MethodGet, testBase+"?pageSize=-3", nil), 400, "Invalid page size: -3")
}

// Every unmodeled request shape is an explicit gap naming what it is.
func TestUnmodeledRequestsAreGaps(t *testing.T) {
	h := newHarness(t)
	h.create(testBase, testRepo)
	repository := testBase + "/" + testRepo
	for _, test := range []struct {
		verb, path string
		body       any
		want       string
	}{
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "mode": "REMOTE_REPOSITORY"}, "mode REMOTE_REPOSITORY is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "mode": "VIRTUAL_REPOSITORY"}, "mode VIRTUAL_REPOSITORY is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "cleanupPolicies": map[string]any{"p": map[string]any{"action": "DELETE"}}}, "cleanupPolicies is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "kmsKeyName": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}, "kmsKeyName is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "remoteRepositoryConfig": map[string]any{"description": "x"}}, "remoteRepositoryConfig is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "APT"}, "format APT is not modeled"},
		{http.MethodPost, testBase + "?repositoryId=repo-one", map[string]any{"format": "DOCKER", "vulnerabilityScanningConfig": map[string]any{"enablementConfig": "DISABLED"}}, "vulnerabilityScanningConfig.enablementConfig is not modeled"},
		{http.MethodPatch, repository + "?updateMask=cleanupPolicies", map[string]any{}, `update mask path "cleanupPolicies"`},
		{http.MethodPatch, repository + "?updateMask=docker_config", map[string]any{}, `update mask path "dockerConfig"`},
		{http.MethodGet, testBase + "?filter=name%3D%22x%22", nil, "list filter is not implemented"},
		{http.MethodGet, testBase + "?orderBy=create_time", nil, `list orderBy "create_time"`},
		{http.MethodGet, "/v1/projects/" + testProject + "/locations/global/repositories", nil, `location "global"`},
		{http.MethodPost, repository + ":setIamPolicy", map[string]any{"policy": map[string]any{}, "updateMask": "auditConfigs"}, "setIamPolicy updateMask audit_configs is not modeled"},
	} {
		recorder := h.invoke(test.verb, test.path, test.body)
		expect(t, recorder, 501, "Artifact Registry coverage gap: "+test.want)
	}
	// Nothing above created a repository.
	if body := h.invoke(http.MethodGet, testBase, nil).Body.String(); strings.Count(body, `"name"`) != 1 {
		t.Fatalf("a refused create stored a repository: %s", body)
	}
}

// Bodies are decoded strictly with GCP's transcoder refusals; output-only
// fields are dropped.
func TestStrictBodies(t *testing.T) {
	h := newHarness(t)
	expect(t, h.invoke(http.MethodPost, testBase+"?repositoryId=repo-one", map[string]any{"format": "DOCKER", "notAField": 1}), 400,
		`Invalid JSON payload received. Unknown name "notAField" at 'repository': Cannot find field.`)
	expect(t, h.invoke(http.MethodPost, testBase+"?repositoryId=repo-one", map[string]any{"format": "DOCKER", "labels": "x"}), 400, "INVALID_ARGUMENT")
	expect(t, h.invoke(http.MethodPost, testBase+"?repositoryId=repo-one", map[string]any{"format": "NOT_A_FORMAT"}), 400, "INVALID_ARGUMENT")
	h.create(testBase, testRepo)
	repository := testBase + "/" + testRepo
	expect(t, h.invoke(http.MethodPatch, repository+"?updateMask=description", map[string]any{
		"description": "d", "registryUri": "x", "vulnerabilityScanningConfig": map[string]any{"enablementState": "SCANNING_DISABLED"},
	}), 200, `"description":"d"`)
	// GCP ignores an unknown mask path and refuses a maskless patch that
	// would change the format (recorded).
	expect(t, h.invoke(http.MethodPatch, repository+"?updateMask=notAField", map[string]any{"description": "ignored"}), 200, `"description":"d"`)
	expect(t, h.invoke(http.MethodPatch, repository, map[string]any{"description": "no mask"}), 400, "Repository format cannot be changed")
	expect(t, h.invoke(http.MethodPatch, repository, map[string]any{"format": "DOCKER", "description": "replaced"}), 200, `"description":"replaced"`)
	expect(t, h.invoke(http.MethodPost, repository+":setIamPolicy", map[string]any{"policy": map[string]any{
		"version": 3, "bindings": []any{map[string]any{"role": "roles/artifactregistry.reader", "members": []any{testActor},
			"condition": map[string]any{"title": "t", "expression": "true"}}},
	}}), 400, "Can't set conditional policy on this resource")
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	h := newHarness(t)
	expect(t, h.invoke(http.MethodGet, testBase+"/missing", nil), 404, "Requested entity was not found.")
	expect(t, h.invoke(http.MethodGet, testBase+"/Bad_Upper", nil), 400, "INVALID_ARGUMENT")
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{
		Bindings: []iam.Binding{{Role: "roles/artifactregistry.reader", Members: []string{testActor}}},
	}); err != nil {
		t.Fatalf("switch the role: %v", err)
	}
	// The recorded denial: the resource in the message and in ErrorInfo.
	recorder := h.invoke(http.MethodPost, testBase+"?repositoryId="+testRepo, map[string]any{"format": "DOCKER"})
	expect(t, recorder, 403, "Permission 'artifactregistry.repositories.create' denied on resource '//artifactregistry.googleapis.com/projects/"+testProject+"/locations/"+testLocation+"' (or it may not exist).")
	expect(t, recorder, 403, `"domain":"artifactregistry.googleapis.com"`)
	expect(t, recorder, 403, `"resource":"projects/`+testProject+`/locations/`+testLocation+`"`)
	if _, err := h.engine.SetPolicy("//cloudresourcemanager.googleapis.com/projects/"+testProject, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	// A caller without access sees 403, not whether the repository exists.
	expect(t, h.invoke(http.MethodGet, testBase+"/missing", nil), 403, "repositories/missing' (or it may not exist).")
}

func TestEveryRouteIsHandled(t *testing.T) {
	h := newHarness(t)
	h.create(testBase, testRepo)
	gap := "No provider implements"
	probe := func(verb, path string) {
		recorder := h.invoke(verb, path, nil)
		if recorder.Code == 501 && bytes.Contains(recorder.Body.Bytes(), []byte(gap)) {
			t.Fatalf("%s %s hit the dispatch gap", verb, path)
		}
	}
	probe(http.MethodGet, testBase)
	probe(http.MethodGet, testBase+"/"+testRepo)
	probe(http.MethodDelete, testBase+"/"+testRepo)
	probe(http.MethodGet, testBase+"/"+testRepo+":getIamPolicy")
}
