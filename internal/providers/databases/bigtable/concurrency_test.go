package bigtable

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPatchIsAtomic rejects an invalid instance update without changing the
// instance.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.instance()
	instance := testInstances + "/" + testInstance
	if recorder := h.invoke(http.MethodPatch, instance+"?updateMask=displayName,labels", map[string]any{"displayName": "after", "labels": "bogus"}); recorder.Code != 400 {
		t.Fatalf("invalid update status = %d", recorder.Code)
	}
	if body := h.invoke(http.MethodGet, instance, nil).Body.String(); !strings.Contains(body, `"displayName":"demo"`) {
		t.Fatalf("a rejected update changed the instance: %s", body)
	}
}

// TestConcurrentInstanceRequests races instance create, reads, updates, table
// methods, and deletes on one instance; -race fails it if a handler touches a
// record outside the scope lock.
func TestConcurrentInstanceRequests(t *testing.T) {
	h := newHarness(t)
	instance := testInstances + "/" + testInstance
	table := instance + "/tables/" + testTable
	policy := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{
		"role": "roles/bigtable.user", "members": []any{"serviceAccount:reader@" + testProject + ".iam.gserviceaccount.com"},
	}}}}
	providertest.Hammer(t, h.gate, testToken, 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testInstances, Body: createBody(), Want: []int{200, 409}},
			{Method: http.MethodGet, Path: instance, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: testInstances, Want: []int{200}},
			{Method: http.MethodPatch, Path: instance + "?updateMask=displayName,labels", Body: map[string]any{"displayName": "worker", "labels": map[string]string{"worker": "w"}}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: "/v2/operations/projects/" + testProject + "/operations", Want: []int{200}},
			{Method: http.MethodGet, Path: instance + "/clusters", Want: []int{200, 404}},
			{Method: http.MethodGet, Path: instance + "/clusters/" + testCluster, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + "/tables", Body: map[string]any{"tableId": testTable}, Want: []int{200, 404, 409}},
			{Method: http.MethodGet, Path: table, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: table + ":setIamPolicy", Body: policy, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: table + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: table, Want: []int{200, 404}},
			{Method: http.MethodDelete, Path: instance, Want: []int{200, 404}},
		}
	})
}

// TestTablePolicyRacingRecreate races policy reads against deleting and
// recreating the table and its instance. Authorization picks the table or its instance and
// checks it under the scope lock that unindexing holds, so a read sees 200 or
// 404, never 501 for a target unindexed mid-check.
func TestTablePolicyRacingRecreate(t *testing.T) {
	h := newHarness(t)
	instance := testInstances + "/" + testInstance
	table := instance + "/tables/" + testTable
	h.instance()
	providertest.Hammer(t, h.gate, testToken, 16, 40, func(worker int) []providertest.Request {
		if worker%4 == 0 {
			return []providertest.Request{
				{Method: http.MethodPost, Path: testInstances, Body: createBody(), Want: []int{200, 409}},
				{Method: http.MethodPost, Path: instance + "/tables", Body: map[string]any{"tableId": testTable}, Want: []int{200, 404, 409}},
				{Method: http.MethodDelete, Path: table, Want: []int{200, 404}},
				{Method: http.MethodDelete, Path: instance, Want: []int{200, 404}},
			}
		}
		return []providertest.Request{
			{Method: http.MethodPost, Path: table + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodPost, Path: instance + ":getIamPolicy", Body: map[string]any{}, Want: []int{200, 404}},
			{Method: http.MethodGet, Path: table, Want: []int{200, 404}},
		}
	})
}

// TestTableCreateRacingInstanceDelete races table creates and deletes against
// deleting their instance. A table whose create or delete finishes after the
// instance delete removed the instance must not stay published (its IAM
// hierarchy would be incomplete, and every later call on it a 501) or stay in
// the backend.
func TestTableCreateRacingInstanceDelete(t *testing.T) {
	h := newHarness(t)
	instance := testInstances + "/" + testInstance
	providertest.Hammer(t, h.gate, testToken, 8, 10, func(worker int) []providertest.Request {
		if worker%2 == 0 {
			return []providertest.Request{
				{Method: http.MethodPost, Path: testInstances, Body: createBody(), Want: []int{200, 409}},
				{Method: http.MethodDelete, Path: instance, Want: []int{200, 404}},
			}
		}
		return []providertest.Request{
			{Method: http.MethodPost, Path: instance + "/tables", Body: map[string]any{"tableId": testTable}, Want: []int{200, 404, 409}},
			{Method: http.MethodDelete, Path: instance + "/tables/" + testTable, Want: []int{200, 404}},
		}
	})
	for _, resource := range h.index.Owned(serviceName) {
		if _, found := h.index.Get(resource.Parent); resource.Kind == kindTable && !found {
			t.Fatalf("table %s is published without its instance", resource.Name)
		}
	}
	h.invoke(http.MethodDelete, instance, nil)
	h.instance()
	if body := h.invoke(http.MethodGet, instance+"/tables", nil).Body.String(); strings.Contains(body, testTable) {
		t.Fatalf("a deleted instance's table survived in the backend: %s", body)
	}
}

// TestInstancePolicyNeverWritesTheProjectPolicy covers an instance policy
// call authorized at the project, because the instance was unpublished when
// Nearest picked the target, that finds the instance recreated by the time it
// takes the lock. It must read NOT_FOUND and leave the project's policy alone;
// writing it there revoked the caller's own project grant.
func TestInstancePolicyNeverWritesTheProjectPolicy(t *testing.T) {
	h := newHarness(t)
	h.instance()
	project := provider.ProjectResource(testProject)
	before, err := h.engine.GetPolicy(project)
	if err != nil {
		t.Fatal(err)
	}
	call := gateway.Call{Context: context.Background(), Method: methodInstancesSetIam,
		Principal: gateway.Principal{State: gateway.Authenticated, Subject: testActor, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}}
	request := &gateway.RESTRequest{Path: map[string]string{"project": testProject, "instance": testInstance}}
	_, err = h.provider.policy(call, request, provider.ProjectScope(testProject), iam.Target{Name: project}, func(name string) (iam.Policy, error) {
		return h.engine.SetPolicy(name, iam.Policy{})
	})
	if status.Code(err) != codes.NotFound || !strings.Contains(status.Convert(err).Message(), "Instance projects/"+testProject+"/instances/"+testInstance+" not found.") {
		t.Fatalf("policy call at the project = %v", err)
	}
	if after, _ := h.engine.GetPolicy(project); len(after.Bindings) != len(before.Bindings) {
		t.Fatalf("the project policy changed: %+v", after)
	}
}
