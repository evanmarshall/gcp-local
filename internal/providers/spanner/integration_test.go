//go:build spannerintegration

package spanner

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// TestOfficialEmulatorLifecycle drives the real pinned emulator: the
// recorded admin flow, project isolation, and reset clearing emulator state.
func TestOfficialEmulatorLifecycle(t *testing.T) {
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Spanner tests in the loopback-only runner")
	}
	h := &harness{t: t, index: state.NewIndex()}
	h.engine, err = iam.New(h.index)
	must(t, err)
	for project, token := range map[string]string{testProject: adminToken, peerProject: peerToken} {
		must(t, h.index.Put(ownerProjects, state.Resource{Name: provider.ProjectResource(project), Service: ownerProjects,
			Kind: kindProject, Number: "123456", Scope: provider.ProjectScope(project)}))
		_, err := h.engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/spanner.admin", Members: []string{principal(token)}}}})
		must(t, err)
	}
	h.manager = backend.NewManager(30*time.Second, 10*time.Second)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	registry := gateway.NewRegistry()
	config := Config{Emulator: os.Getenv("GCP_LOCAL_SPANNER_EMULATOR"), Libraries: os.Getenv("GCP_LOCAL_SPANNER_LIBRARIES")}
	_, err = New(context.Background(), registry, h.manager, h.index, h.engine, config)
	must(t, err)
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})

	h.expect(h.invoke(peerToken, http.MethodPost, testInstances, instanceBody(testProject)), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a denied request started an emulator: %+v", status)
	}
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/databases", map[string]any{
		"createStatement": "CREATE DATABASE `" + testDatabase + "`",
		"extraStatements": []string{"CREATE TABLE Items (Id STRING(36) NOT NULL, Name STRING(64)) PRIMARY KEY (Id)"},
	}), http.StatusOK)
	ddl := h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases/"+testDatabase+"/ddl", nil), http.StatusOK)
	if statements, _ := ddl["statements"].([]any); len(statements) != 1 || statements[0] != "CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n) PRIMARY KEY(Id)" {
		t.Fatalf("ddl = %v", ddl)
	}
	peerInstances := "/v1/projects/" + peerProject + "/instances"
	h.expect(h.invoke(peerToken, http.MethodGet, peerInstances+"/"+testInstance, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodPost, peerInstances, instanceBody(peerProject)), http.StatusOK)

	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusNotFound)
	h.expect(h.invoke(peerToken, http.MethodGet, peerInstances+"/"+testInstance, nil), http.StatusOK)
	h.createInstance()
	h.expect(h.invoke(adminToken, http.MethodGet, testPath+"/databases/"+testDatabase, nil), http.StatusNotFound)
}
