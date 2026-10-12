package cloudtasks

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// testAgent is the harness project's Cloud Tasks service agent.
const testAgent = "serviceAccount:service-123456@gcp-sa-cloudtasks.iam.gserviceaccount.com"

// oidcAccount declares the worker account, lets the actor act as it, and,
// when mint is set, lets the Cloud Tasks service agent mint its ID tokens.
func (h *harness) oidcAccount(mint bool) string {
	h.t.Helper()
	account := iam.AccountResource(testProject, email("worker"))
	if err := h.index.Put("iam", state.Resource{
		Name: account, Service: "iam", Kind: "serviceAccount", Number: "100000000000000000077",
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + testProject,
		Scope:  state.Scope{Project: testProject, Location: state.Global},
	}); err != nil {
		h.t.Fatal(err)
	}
	bindings := []iam.Binding{{Role: "roles/iam.serviceAccountUser", Members: []string{"serviceAccount:" + email("actor")}}}
	if mint {
		bindings = append(bindings, iam.Binding{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{testAgent}})
	}
	if _, err := h.engine.SetPolicy(account, iam.Policy{Bindings: bindings}); err != nil {
		h.t.Fatal(err)
	}
	return email("worker")
}

// taskPath returns the REST path of a created task.
func taskPath(task map[string]any) string {
	name, _ := task["name"].(string)
	return queuePath + "/tasks/" + name[strings.LastIndex(name, "/")+1:]
}

func TestOIDCTaskDeliversAudienceBoundToken(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	worker := h.oidcAccount(true)
	tokens := make(chan string, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	created := decode(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(target.URL, worker)))
	h.pollTask(taskPath(created), func(recorder *httptest.ResponseRecorder) bool { return recorder.Code == 404 })
	header := <-tokens
	if !strings.HasPrefix(header, "Bearer ") {
		t.Fatalf("delivered Authorization = %q", header)
	}
	claims, err := h.tokens.Verify(strings.TrimPrefix(header, "Bearer "), target.URL)
	if err != nil || claims.Email != worker || claims.Subject != "100000000000000000077" {
		t.Fatalf("delivered token claims = %+v, %v", claims, err)
	}
	if _, err := h.tokens.Verify(strings.TrimPrefix(header, "Bearer "), "https://other.example"); err == nil {
		t.Fatal("the token verifies for another audience")
	}
}

// A token the service agent cannot mint fails the attempt; nothing reaches
// the target and the queue's retry policy applies.
func TestUnmintableOIDCTokenFailsTheAttempt(t *testing.T) {
	h := newHarness(t)
	h.createRetryQueue()
	worker := h.oidcAccount(false)
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	created := decode(t, h.invoke(http.MethodPost, queuePath+"/tasks", taskBody(target.URL, worker)))
	attempted := decode(t, h.pollTask(taskPath(created), func(recorder *httptest.ResponseRecorder) bool {
		last, _ := decode(t, recorder)["lastAttempt"].(map[string]any)
		return last["responseStatus"] != nil
	}))
	last, _ := attempted["lastAttempt"].(map[string]any)
	response, _ := last["responseStatus"].(map[string]any)
	message, _ := response["message"].(string)
	if response["code"] != float64(7) || !strings.Contains(message, "could not mint an OIDC token") {
		t.Fatalf("failed attempt = %v", last)
	}
	if reached.Load() != 0 {
		t.Fatalf("the target received %d unauthenticated requests", reached.Load())
	}
}
