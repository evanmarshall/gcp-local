package provider

import (
	"context"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gateFixture is an engine whose viewer holds cloudscheduler.jobs.get on
// project p, and calls as that viewer.
type gateFixture struct {
	engine  *iam.Engine
	index   *state.Index
	project string
	viewer  gateway.Principal
}

func newGateFixture(t *testing.T) gateFixture {
	t.Helper()
	f := gateFixture{index: state.NewIndex(), project: ProjectResource("p"), viewer: gateway.Principal{
		State: gateway.Authenticated, Subject: "serviceAccount:viewer@p.iam.gserviceaccount.com",
		Project: "p", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
	var err error
	f.engine, err = iam.New(f.index)
	must(t, err)
	must(t, f.index.Put("resourcemanager", state.Resource{Name: f.project, Service: "resourcemanager", Kind: "project",
		Number: "1", Scope: ProjectScope("p")}))
	_, err = f.engine.SetPolicy(f.project, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/cloudscheduler.viewer", Members: []string{f.viewer.Subject}},
	}})
	must(t, err)
	must(t, RegisterPermissions(f.engine, map[string]string{"test.get": "cloudscheduler.jobs.get", "test.create": "cloudscheduler.jobs.create"}))
	return f
}

func (f gateFixture) call(method string, principal gateway.Principal) gateway.Call {
	return gateway.Call{Context: context.Background(), Method: method, Location: "global", Principal: principal}
}

func TestGateAllowsAndStampsTheDecision(t *testing.T) {
	f := newGateFixture(t)
	gate := Gate{Engine: f.engine, Message: LacksPermission}
	decision, err := gate.Check(f.call("test.get", f.viewer), iam.Target{Name: f.project}, "projects/p/locations/l/jobs/j")
	if err != nil || decision.ID == "" {
		t.Fatalf("allowed: %v %v", decision, err)
	}
	if response := WithDecision(&gateway.RESTResponse{}, decision); response.Headers.Get("X-Gcp-Local-Decision-Id") != decision.ID {
		t.Fatal(response.Headers)
	}
}

func TestGateDenialCarriesErrorInfo(t *testing.T) {
	f := newGateFixture(t)
	gate := Gate{Engine: f.engine, Message: PermissionDeniedOn, Domain: "example.googleapis.com"}
	_, err := gate.Check(f.call("test.create", f.viewer), iam.Target{Name: f.project}, "projects/p/locations/l")
	s := status.Convert(err)
	if s.Code() != codes.PermissionDenied || s.Message() != "Permission 'cloudscheduler.jobs.create' denied on 'projects/p/locations/l'" || len(s.Details()) != 1 {
		t.Fatalf("denied: %v", err)
	}
	info, ok := s.Details()[0].(*errdetails.ErrorInfo)
	if !ok || info.GetReason() != "IAM_PERMISSION_DENIED" || info.GetDomain() != "example.googleapis.com" ||
		info.GetMetadata()["permission"] != "cloudscheduler.jobs.create" || info.GetMetadata()["resource"] != "projects/p/locations/l" {
		t.Fatalf("details: %v", s.Details())
	}
}

func TestGateDenialWithoutDomainHasNoDetails(t *testing.T) {
	f := newGateFixture(t)
	gate := Gate{Engine: f.engine, Message: LacksPermission}
	_, err := gate.Check(f.call("test.create", f.viewer), iam.Target{Name: f.project}, "projects/p/locations/l")
	if s := status.Convert(err); s.Message() != `The principal (user or service account) lacks IAM permission "cloudscheduler.jobs.create" for the resource "projects/p/locations/l" (or the resource may not exist).` || len(s.Details()) != 0 {
		t.Fatalf("denied without a domain: %v %v", err, s.Details())
	}
}

func TestGateRefusesAnonymousAndUnregisteredCalls(t *testing.T) {
	f := newGateFixture(t)
	gate := Gate{Engine: f.engine, Message: LacksPermission}
	_, err := gate.Check(f.call("test.get", gateway.Principal{State: gateway.Anonymous}), iam.Target{Name: f.project}, "projects/p")
	if status.Code(err) != codes.Unauthenticated || status.Convert(err).Message() != "Request had invalid authentication credentials." {
		t.Fatalf("anonymous: %v", err)
	}
	_, err = gate.Check(f.call("test.unregistered", f.viewer), iam.Target{Name: f.project}, "projects/p")
	if code := status.Code(err); code == codes.PermissionDenied || code == codes.OK || !Unverified(err) {
		t.Fatalf("unregistered method: %v", err)
	}
}

func TestNearestPicksTheIndexedTargetUnderTheScopeLock(t *testing.T) {
	f := newGateFixture(t)
	index, project, table := f.index, f.project, "//x.googleapis.com/projects/p/tables/t"
	gate := Gate{Engine: f.engine, Index: index, Message: LacksPermission}
	store := state.NewStore(func() struct{} { return struct{}{} }, state.GlobalScope)
	call := f.call("test.get", f.viewer)

	if _, target, err := Nearest(gate, store, ProjectScope("p"), call, "", table, project); err != nil || target.Name != project {
		t.Fatalf("a missing table authorizes at its project: %v %v", target, err)
	}
	must(t, index.Put("x", state.Resource{Name: table, Service: "x", Kind: "table", Parent: project, Scope: ProjectScope("p")}))
	if _, target, err := Nearest(gate, store, ProjectScope("p"), call, "", table, project); err != nil || target.Name != table {
		t.Fatalf("an indexed table is the target: %v %v", target, err)
	}
	// Unindexing under the same lock while Nearest runs never yields the
	// engine's unverified-target refusal.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 200 {
			_ = store.With(ProjectScope("p"), func(*struct{}) error { return index.Remove("x", ProjectScope("p"), table) })
			_ = store.With(ProjectScope("p"), func(*struct{}) error {
				return index.Put("x", state.Resource{Name: table, Service: "x", Kind: "table", Parent: project, Scope: ProjectScope("p")})
			})
		}
	}()
	for range 200 {
		if _, _, err := Nearest(gate, store, ProjectScope("p"), call, "", table, project); err != nil {
			t.Fatalf("racing a delete: %v", err)
		}
	}
	<-done
	if _, _, err := Nearest(gate, store, state.Scope{Project: "p", Location: "us-central1"}, call, "", table, project); err == nil || status.Code(err) == codes.OK {
		t.Fatal("a scope the store refuses was allowed")
	}
}

func TestNearestSuppliesConditionAttributes(t *testing.T) {
	f := newGateFixture(t)
	table := "//x.googleapis.com/projects/p/tables/t"
	must(t, f.index.Put("x", state.Resource{Name: table, Service: "x", Kind: "table", Parent: f.project, Scope: ProjectScope("p")}))
	// The viewer's grant holds only on resources whose resource.name is the table's.
	_, err := f.engine.SetPolicy(f.project, iam.Policy{Version: 3, Bindings: []iam.Binding{{
		Role: "roles/cloudscheduler.viewer", Members: []string{f.viewer.Subject},
		Condition: []byte(`{"title":"t","expression":"resource.name == \"projects/p/tables/t\""}`),
	}}})
	must(t, err)
	store := state.NewStore(func() struct{} { return struct{}{} }, state.GlobalScope)
	call := f.call("test.get", f.viewer)
	bare := Gate{Engine: f.engine, Index: f.index, Message: LacksPermission}
	if _, _, err := Nearest(bare, store, ProjectScope("p"), call, "", table, f.project); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("without attributes the condition must grant nothing: %v", err)
	}
	named := bare
	named.Attributes = func(target string) iam.ResourceAttributes {
		return iam.ResourceAttributes{Name: target[len("//x.googleapis.com/"):], Type: "x.googleapis.com/Table", Service: "x.googleapis.com"}
	}
	if _, target, err := Nearest(named, store, ProjectScope("p"), call, "", table, f.project); err != nil || target.Attributes.Type != "x.googleapis.com/Table" {
		t.Fatalf("attributes: %v %v", target, err)
	}
}

func TestGateDenialWithAFixedMessage(t *testing.T) {
	f := newGateFixture(t)
	gate := Gate{Engine: f.engine, Message: NotAuthorized, Domain: "iam.googleapis.com"}
	_, err := gate.Check(f.call("test.create", f.viewer), iam.Target{Name: f.project}, "projects/p/topics/t")
	s := status.Convert(err)
	if s.Message() != "User not authorized to perform this action." || len(s.Details()) != 1 {
		t.Fatalf("fixed message: %v %v", err, s.Details())
	}
}
