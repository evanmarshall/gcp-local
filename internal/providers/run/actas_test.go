package run

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runpb "cloud.google.com/go/run/apiv2/runpb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Names of the actAs tests: the deployer may create and update services but
// holds no actAs; the worker is a declared runtime service account.
const (
	deployerSubject = "serviceAccount:deployer@demo-project.iam.gserviceaccount.com"
	workerEmail     = "worker@demo-project.iam.gserviceaccount.com"
	defaultCompute  = "123456-compute@developer.gserviceaccount.com"
	servicesPath    = "/v2/projects/demo-project/locations/us-central1/services"
)

// declareWorker indexes the worker account and grants the deployer
// roles/run.developer on the project, which has no iam.serviceAccounts.actAs.
func (h *harness) declareWorker(t *testing.T) {
	t.Helper()
	if err := h.index.Put("iam", state.Resource{
		Name: iam.AccountResource("demo-project", workerEmail), Service: "iam", Kind: "serviceAccount",
		Number: "100000000000000000042", Parent: "//cloudresourcemanager.googleapis.com/projects/demo-project",
		Scope: state.Scope{Project: "demo-project", Location: state.Global},
	}); err != nil {
		t.Fatal(err)
	}
	project := "//cloudresourcemanager.googleapis.com/projects/demo-project"
	policy, err := h.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/run.developer", Members: []string{deployerSubject}})
	if _, err := h.engine.SetPolicy(project, policy); err != nil {
		t.Fatal(err)
	}
}

// grantActAs lets the deployer act as the worker account.
func (h *harness) grantActAs(t *testing.T) {
	t.Helper()
	if _, err := h.engine.SetPolicy(iam.AccountResource("demo-project", workerEmail), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{deployerSubject}},
	}}); err != nil {
		t.Fatal(err)
	}
}

// deploy issues one REST request as the deployer.
func (h *harness) deploy(verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer deployer")
	recorder := httptest.NewRecorder()
	h.gateway.ServeHTTP(recorder, request)
	return recorder
}

// serviceBody is a one-container service running as account ("" is the
// project's default compute service account).
func serviceBody(account string) map[string]any {
	template := map[string]any{"containers": []any{map[string]any{"image": imageReference}}}
	if account != "" {
		template["serviceAccount"] = account
	}
	return map[string]any{"name": serviceReference, "template": template}
}

// expectActAsDenied requires Cloud Run's actAs refusal naming account.
func expectActAsDenied(t *testing.T, recorder *httptest.ResponseRecorder, account string) {
	t.Helper()
	want := "Permission 'iam.serviceaccounts.actAs' denied on service account " + account
	if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), want) {
		t.Fatalf("deploy without actAs = %d %s", recorder.Code, recorder.Body.String())
	}
}

// Deploying a revision needs iam.serviceAccounts.actAs on its runtime service
// account, or on the default compute account when the template names none
// (https://cloud.google.com/run/docs/configuring/services/service-identity). Without it a
// run.services.create holder could run code as any account in the project.
func TestDeployRequiresActAsOnRuntimeAccount(t *testing.T) {
	h := newHarness(t)
	h.declareWorker(t)
	expectActAsDenied(t, h.deploy(http.MethodPost, servicesPath, serviceBody(workerEmail)), workerEmail)
	expectActAsDenied(t, h.deploy(http.MethodPost, servicesPath, serviceBody("")), defaultCompute)
	undeclared := "ghost-sa@demo-project.iam.gserviceaccount.com"
	expectActAsDenied(t, h.deploy(http.MethodPost, servicesPath, serviceBody(undeclared)), undeclared)
	if len(h.runtime.started) != 0 {
		t.Fatalf("a refused deploy launched %v", h.runtime.started)
	}
	h.grantActAs(t)
	if recorder := h.deploy(http.MethodPost, servicesPath, serviceBody(workerEmail)); recorder.Code != http.StatusOK {
		t.Fatalf("create with actAs = %d %s", recorder.Code, recorder.Body.String())
	}
	// Every rollout is a deploy: an update that switches to an account the
	// deployer cannot act as is refused and leaves the service unchanged.
	expectActAsDenied(t, h.deploy(http.MethodPatch, "/v2/"+serviceReference, serviceBody("")), defaultCompute)
	if len(h.runtime.started) != 1 {
		t.Fatalf("a refused update launched %v", h.runtime.started)
	}
	if recorder := h.deploy(http.MethodPatch, "/v2/"+serviceReference, serviceBody(workerEmail)); recorder.Code != http.StatusOK {
		t.Fatalf("update with actAs = %d %s", recorder.Code, recorder.Body.String())
	}
}

// The gRPC methods run the same handler, so the Go client gets the same
// refusal as PERMISSION_DENIED.
func TestGRPCDeployRequiresActAs(t *testing.T) {
	g := newGRPCHarness(t)
	project := "//cloudresourcemanager.googleapis.com/projects/demo-project"
	policy, err := g.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	policy.Bindings = append(policy.Bindings, iam.Binding{Role: "roles/run.developer", Members: []string{peerSubject}})
	if _, err := g.engine.SetPolicy(project, policy); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = g.services(t, peerSubject).CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "svc", Service: &runpb.Service{
		Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: imageReference}}},
	}})
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), defaultCompute) {
		t.Fatalf("CreateService without actAs = %v", err)
	}
	if len(g.runtime.started) != 0 {
		t.Fatalf("a refused deploy launched %v", g.runtime.started)
	}
}

// An environment that declares the default compute account decides actAs on
// that account's own policy, as GCP does.
func TestDeclaredDefaultComputeAccountUsesItsOwnPolicy(t *testing.T) {
	h := newHarness(t)
	h.declareWorker(t)
	account := iam.AccountResource("demo-project", defaultCompute)
	if err := h.index.Put("iam", state.Resource{
		Name: account, Service: "iam", Kind: "serviceAccount", Number: "100000000000000000043",
		Parent: "//cloudresourcemanager.googleapis.com/projects/demo-project",
		Scope:  state.Scope{Project: "demo-project", Location: state.Global},
	}); err != nil {
		t.Fatal(err)
	}
	expectActAsDenied(t, h.deploy(http.MethodPost, servicesPath, serviceBody("")), defaultCompute)
	if _, err := h.engine.SetPolicy(account, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/iam.serviceAccountUser", Members: []string{deployerSubject}},
	}}); err != nil {
		t.Fatal(err)
	}
	if recorder := h.deploy(http.MethodPost, servicesPath, serviceBody("")); recorder.Code != http.StatusOK {
		t.Fatalf("create as the default account = %d %s", recorder.Code, recorder.Body.String())
	}
}
