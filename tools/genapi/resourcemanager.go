package main

import (
	"net/http"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/genproto/googleapis/api/annotations"
)

// resourceManagerRoutes generates the Resource Manager route slice.
func resourceManagerRoutes() []byte {
	return routeTable("ResourceManagerRoutes", "is the Resource Manager v1 and v3 route table slice.", handWritten("resourcemanager.go"), resourceManagerRows())
}

// resourceManagerRows lists the Resource Manager REST routes. The v1 rows keep
// the "resourcemanager." prefix of the first project IAM slice; the v3 rows
// use the cloudresourcemanager v3 discovery method IDs. Terraform polls folder
// operations on v1 (/v1/operations/...), so both versions serve them.
func resourceManagerRows() []route {
	const folder, organization, project = "/v3/folders/{folder}", "/v3/organizations/{organization}", "/v3/projects/{project}"
	return []route{
		{"resourcemanager.projects.getIamPolicy", http.MethodPost, "/v1/projects/{project}:getIamPolicy", true, false},
		{"resourcemanager.projects.setIamPolicy", http.MethodPost, "/v1/projects/{project}:setIamPolicy", true, false},
		{"resourcemanager.projects.testIamPermissions", http.MethodPost, "/v1/projects/{project}:testIamPermissions", true, false},
		{"resourcemanager.projects.get", http.MethodGet, "/v1/projects/{project}", false, false},
		{"resourcemanager.operations.get", http.MethodGet, "/v1/operations/{operation}", false, false},
		{"cloudresourcemanager.organizations.get", http.MethodGet, organization, false, false},
		{"cloudresourcemanager.organizations.search", http.MethodGet, "/v3/organizations:search", false, false},
		{"cloudresourcemanager.organizations.getIamPolicy", http.MethodPost, organization + ":getIamPolicy", true, false},
		{"cloudresourcemanager.organizations.setIamPolicy", http.MethodPost, organization + ":setIamPolicy", true, false},
		{"cloudresourcemanager.organizations.testIamPermissions", http.MethodPost, organization + ":testIamPermissions", true, false},
		{"cloudresourcemanager.folders.create", http.MethodPost, "/v3/folders", true, false},
		{"cloudresourcemanager.folders.list", http.MethodGet, "/v3/folders", false, false},
		{"cloudresourcemanager.folders.search", http.MethodGet, "/v3/folders:search", false, false},
		{"cloudresourcemanager.folders.get", http.MethodGet, folder, false, false},
		{"cloudresourcemanager.folders.patch", http.MethodPatch, folder, true, false},
		{"cloudresourcemanager.folders.delete", http.MethodDelete, folder, false, false},
		{"cloudresourcemanager.folders.move", http.MethodPost, folder + ":move", true, false},
		{"cloudresourcemanager.folders.undelete", http.MethodPost, folder + ":undelete", true, false},
		{"cloudresourcemanager.folders.getIamPolicy", http.MethodPost, folder + ":getIamPolicy", true, false},
		{"cloudresourcemanager.folders.setIamPolicy", http.MethodPost, folder + ":setIamPolicy", true, false},
		{"cloudresourcemanager.folders.testIamPermissions", http.MethodPost, folder + ":testIamPermissions", true, false},
		{"cloudresourcemanager.projects.get", http.MethodGet, project, false, false},
		{"cloudresourcemanager.projects.list", http.MethodGet, "/v3/projects", false, false},
		{"cloudresourcemanager.projects.search", http.MethodGet, "/v3/projects:search", false, false},
		{"cloudresourcemanager.projects.move", http.MethodPost, project + ":move", true, false},
		{"cloudresourcemanager.projects.getIamPolicy", http.MethodPost, project + ":getIamPolicy", true, false},
		{"cloudresourcemanager.projects.setIamPolicy", http.MethodPost, project + ":setIamPolicy", true, false},
		{"cloudresourcemanager.projects.testIamPermissions", http.MethodPost, project + ":testIamPermissions", true, false},
		{"cloudresourcemanager.operations.get", http.MethodGet, "/v3/operations/{operation}", false, false},
	}
}

// resourceManagerPackage is the Go package of the v3 bindings, and
// resourceManagerAlias its import alias.
const resourceManagerPackage, resourceManagerAlias = "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb", "resourcemanagerpb"

// writeResourceManager writes the route table and the three v3 transcoded
// services.
func writeResourceManager(check bool) {
	write("security/resourcemanager/resourcemanager_routes.go", resourceManagerRoutes(), check)
	write("security/resourcemanager/folders_dispatch.go", transcodedDispatch(foldersRPC), check)
	write("security/resourcemanager/organizations_dispatch.go", transcodedDispatch(organizationsRPC), check)
	write("security/resourcemanager/projects_dispatch.go", transcodedDispatch(projectsRPC), check)
}

// testIamPermissions names the IAM permission test method of every service.
const testIamPermissions = "TestIamPermissions"

// foldersRPC is the Resource Manager v3 Folders surface, plus operation
// polling. The service config (cloudresourcemanager_v3.yaml) binds
// GetOperation to /v3/{name=operations/**}; local operation IDs are one
// segment, so the binding takes operations/* and leaves deeper names
// (Bigtable's operations/projects/...) to their own services.
var foldersRPC = transcodedService{
	service: resourcemanagerpb.File_google_cloud_resourcemanager_v3_folders_proto.Services().ByName("Folders"),
	alias:   resourceManagerAlias, importPath: resourceManagerPackage,
	variable: "FoldersMethods", bindings: "FoldersTranscoding",
	rest: map[string]string{
		"GetFolder": "cloudresourcemanager.folders.get", "ListFolders": "cloudresourcemanager.folders.list",
		"SearchFolders": "cloudresourcemanager.folders.search", "CreateFolder": "cloudresourcemanager.folders.create",
		"UpdateFolder": "cloudresourcemanager.folders.patch", "MoveFolder": "cloudresourcemanager.folders.move",
		"DeleteFolder": "cloudresourcemanager.folders.delete", "UndeleteFolder": "cloudresourcemanager.folders.undelete",
		getIamPolicy: "cloudresourcemanager.folders.getIamPolicy", setIamPolicy: "cloudresourcemanager.folders.setIamPolicy",
		testIamPermissions: "cloudresourcemanager.folders.testIamPermissions",
	},
	routes: resourceManagerRows(),
	mixins: []mixinBinding{
		{longrunningOperations.Methods().ByName("GetOperation"), "cloudresourcemanager.operations.get",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v3/{name=operations/*}"}}},
	},
}

// organizationsRPC is the Resource Manager v3 Organizations surface.
var organizationsRPC = transcodedService{
	service: resourcemanagerpb.File_google_cloud_resourcemanager_v3_organizations_proto.Services().ByName("Organizations"),
	alias:   resourceManagerAlias, importPath: resourceManagerPackage,
	variable: "OrganizationsMethods", bindings: "OrganizationsTranscoding",
	rest: map[string]string{
		"GetOrganization": "cloudresourcemanager.organizations.get", "SearchOrganizations": "cloudresourcemanager.organizations.search",
		getIamPolicy: "cloudresourcemanager.organizations.getIamPolicy", setIamPolicy: "cloudresourcemanager.organizations.setIamPolicy",
		testIamPermissions: "cloudresourcemanager.organizations.testIamPermissions",
	},
	routes: resourceManagerRows(),
}

// projectsRPC is the Resource Manager v3 Projects surface. Project create,
// update, delete, and undelete have no route and stay gaps: projects exist
// only in the bootstrap configuration.
var projectsRPC = transcodedService{
	service: resourcemanagerpb.File_google_cloud_resourcemanager_v3_projects_proto.Services().ByName("Projects"),
	alias:   resourceManagerAlias, importPath: resourceManagerPackage,
	variable: "ProjectsMethods", bindings: "ProjectsTranscoding",
	rest: map[string]string{
		"GetProject": "cloudresourcemanager.projects.get", "ListProjects": "cloudresourcemanager.projects.list",
		"SearchProjects": "cloudresourcemanager.projects.search", "MoveProject": "cloudresourcemanager.projects.move",
		getIamPolicy: "cloudresourcemanager.projects.getIamPolicy", setIamPolicy: "cloudresourcemanager.projects.setIamPolicy",
		testIamPermissions: "cloudresourcemanager.projects.testIamPermissions",
	},
	routes: resourceManagerRows(),
}
