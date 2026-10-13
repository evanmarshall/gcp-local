package resourcemanager

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanagerapi "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// bearer attaches one harness token to every RPC over plaintext loopback.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

// dial starts the harness gateway's gRPC server and dials it as token.
func (h *harness) dial(token string) *grpc.ClientConn {
	h.t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	server := h.gate.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	h.t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(bearer(token)))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// drain counts an iterator's items, failing on an error.
func drain[T any](t *testing.T, next func() (T, error)) int {
	t.Helper()
	items := 0
	for {
		_, err := next()
		if errors.Is(err, iterator.Done) {
			return items
		}
		if err != nil {
			t.Fatal(err)
		}
		items++
	}
}

// clients are the three v3 Go clients on one connection.
type clients struct {
	folders       *resourcemanagerapi.FoldersClient
	projects      *resourcemanagerapi.ProjectsClient
	organizations *resourcemanagerapi.OrganizationsClient
}

// newClients builds the v3 clients over a harness connection as token.
func (h *harness) newClients(token string) clients {
	h.t.Helper()
	ctx, connection := context.Background(), option.WithGRPCConn(h.dial(token))
	folders, err := resourcemanagerapi.NewFoldersClient(ctx, connection)
	if err != nil {
		h.t.Fatal(err)
	}
	projects, err := resourcemanagerapi.NewProjectsClient(ctx, connection)
	if err != nil {
		h.t.Fatal(err)
	}
	organizations, err := resourcemanagerapi.NewOrganizationsClient(ctx, connection)
	if err != nil {
		h.t.Fatal(err)
	}
	return clients{folders: folders, projects: projects, organizations: organizations}
}

// TestGoClientFolderOperationsOverGRPC creates and renames a folder through
// polled operations with the Go Folders client, then reads it back.
func TestGoClientFolderOperationsOverGRPC(t *testing.T) {
	h := newHarness(t)
	ctx, client := context.Background(), h.newClients(adminToken).folders
	create, err := client.CreateFolder(ctx, &resourcemanagerpb.CreateFolderRequest{Folder: &resourcemanagerpb.Folder{Parent: testOrg, DisplayName: "grpc"}})
	if err != nil {
		t.Fatal(err)
	}
	folder, err := create.Wait(ctx)
	if err != nil || folder.GetParent() != testOrg || folder.GetState() != resourcemanagerpb.Folder_ACTIVE {
		t.Fatalf("create = %v, %v", folder, err)
	}
	update, err := client.UpdateFolder(ctx, &resourcemanagerpb.UpdateFolderRequest{Folder: &resourcemanagerpb.Folder{Name: folder.GetName(), DisplayName: "renamed"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := client.GetFolder(ctx, &resourcemanagerpb.GetFolderRequest{Name: folder.GetName()}); err != nil || got.GetDisplayName() != "renamed" {
		t.Fatalf("get = %v, %v", got, err)
	}
}

// TestGoClientFolderReadsOverGRPC pages folders and reads a folder policy
// with the Go Folders client.
func TestGoClientFolderReadsOverGRPC(t *testing.T) {
	h := newHarness(t)
	ctx, client := context.Background(), h.newClients(adminToken).folders
	if listed := drain(t, client.ListFolders(ctx, &resourcemanagerpb.ListFoldersRequest{Parent: testOrg, PageSize: 1}).Next); listed != 1 {
		t.Fatalf("listed %d folders", listed)
	}
	if policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: seededFolder}); err != nil || len(policy.GetEtag()) == 0 {
		t.Fatalf("policy = %v, %v", policy, err)
	}
}

// TestGoClientProjectsOverGRPC moves and reads a project and searches by
// parent with the Go Projects client.
func TestGoClientProjectsOverGRPC(t *testing.T) {
	h := newHarness(t)
	ctx, client := context.Background(), h.newClients(adminToken).projects
	move, err := client.MoveProject(ctx, &resourcemanagerpb.MoveProjectRequest{Name: "projects/" + otherProject, DestinationParent: seededFolder})
	if err != nil {
		t.Fatal(err)
	}
	if moved, err := move.Wait(ctx); err != nil || moved.GetParent() != seededFolder {
		t.Fatalf("move = %v, %v", moved, err)
	}
	if got, err := client.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: "projects/654321"}); err != nil || got.GetProjectId() != otherProject {
		t.Fatalf("project = %v, %v", got, err)
	}
	if found := drain(t, client.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{Query: "parent:" + seededFolder}).Next); found != 1 {
		t.Fatalf("search found %d projects", found)
	}
}

// TestGoClientRefusalsOverGRPC tests permissions, a denial, and an unserved
// method over gRPC.
func TestGoClientRefusalsOverGRPC(t *testing.T) {
	h := newHarness(t)
	ctx, admin := context.Background(), h.newClients(adminToken)
	held, err := admin.organizations.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: testOrg,
		Permissions: []string{"resourcemanager.organizations.get", "resourcemanager.folders.create"}})
	if err != nil || len(held.GetPermissions()) != 2 {
		t.Fatalf("held = %v, %v", held, err)
	}
	_, err = h.newClients(readerToken).organizations.GetOrganization(ctx, &resourcemanagerpb.GetOrganizationRequest{Name: testOrg})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader get = %v", err)
	}
	if _, err := admin.projects.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: "projects/" + testProject}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("project delete = %v", err)
	}
}

// TestBootstrapHierarchy serves the organizations, folders, and projects an
// -iam-config file declares, with their bootstrap policies inherited.
func TestBootstrapHierarchy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iam.json")
	config := `{"resources": [
 {"name": "//cloudresourcemanager.googleapis.com/organizations/7", "service": "resourcemanager", "kind": "organization", "parent": "", "scope": {"project": "", "location": "global"}},
 {"name": "//cloudresourcemanager.googleapis.com/folders/8", "service": "resourcemanager", "kind": "folder", "parent": "//cloudresourcemanager.googleapis.com/organizations/7", "scope": {"project": "", "location": "global"}},
 {"name": "//cloudresourcemanager.googleapis.com/projects/boot-proj", "service": "resourcemanager", "kind": "project", "parent": "//cloudresourcemanager.googleapis.com/folders/8", "number": "9", "scope": {"project": "boot-proj", "location": "global"}}],
 "policies": {"//cloudresourcemanager.googleapis.com/folders/8": {"version": 1, "bindings": [{"role": "roles/resourcemanager.folderViewer", "members": ["serviceAccount:reader@boot-proj.iam.gserviceaccount.com"]}]}}}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	engine, index, err := iam.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	registry := gateway.NewRegistry()
	if _, err := New(context.Background(), registry, backend.NewManager(time.Second, time.Second), index, engine); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, engine: engine, index: index, gate: gateway.New(registry, func(context.Context, string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:reader@boot-proj.iam.gserviceaccount.com",
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})}
	if folder := h.expect(readerToken, http.MethodGet, "/v3/folders/8", nil, 200); folder["parent"] != "organizations/7" {
		t.Fatalf("folder = %v", folder)
	}
	if project := h.expect(readerToken, http.MethodGet, "/v3/projects/boot-proj", nil, 200); project["parent"] != "folders/8" || project["name"] != "projects/9" {
		t.Fatalf("project = %v", project)
	}
	h.expect(readerToken, http.MethodGet, "/v3/organizations/7", nil, 403)
}

// TestConcurrentFolderRequests races folder create, get, list, search,
// rename, move, delete, undelete, policy, and project moves; -race fails it
// if a handler touches the hierarchy unlocked.
func TestConcurrentFolderRequests(t *testing.T) {
	h := newHarness(t)
	providertest.Hammer(t, h.gate, adminToken, 8, 4, func(worker int) []providertest.Request {
		destination := testOrg
		if worker%2 == 1 {
			destination = seededFolder
		}
		return []providertest.Request{
			{Method: http.MethodPost, Path: "/v3/folders", Body: map[string]any{"parent": testOrg, "displayName": "race"}, Want: []int{200}},
			{Method: http.MethodGet, Path: "/v3/" + seededFolder, Want: []int{200}},
			{Method: http.MethodGet, Path: "/v3/folders?parent=" + testOrg, Want: []int{200}},
			{Method: http.MethodGet, Path: "/v3/folders:search", Want: []int{200}},
			{Method: http.MethodPatch, Path: "/v3/" + seededFolder, Body: map[string]any{"displayName": "seeded"}, Want: []int{200, 400}},
			{Method: http.MethodDelete, Path: "/v3/" + seededFolder, Want: []int{200, 400}},
			{Method: http.MethodPost, Path: "/v3/" + seededFolder + ":undelete", Body: map[string]any{}, Want: []int{200, 400}},
			{Method: http.MethodPost, Path: "/v3/" + seededFolder + ":getIamPolicy", Body: map[string]any{}, Want: []int{200}},
			{Method: http.MethodPost, Path: "/v3/projects/" + otherProject + ":move", Body: map[string]any{"destinationParent": destination}, Want: []int{200, 400}},
			{Method: http.MethodGet, Path: "/v3/projects:search", Want: []int{200}},
		}
	})
}
