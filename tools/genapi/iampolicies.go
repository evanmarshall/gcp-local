package main

import (
	"net/http"

	iamv2pb "cloud.google.com/go/iam/apiv2/iampb"
	"google.golang.org/genproto/googleapis/api/annotations"
)

// writeIAM writes the IAM route tables and transcoded gRPC services: v1
// service accounts and roles, IAM Credentials, and the v2 Policies.
func writeIAM(check bool) {
	write("security/iam/iam_routes.go", iamRoutes(), check)
	write("security/iam/iam_admin_dispatch.go", transcodedDispatch(iamAdminRPC), check)
	write("security/iam/iam_credentials_dispatch.go", transcodedDispatch(iamCredentialsRPC), check)
	write("security/iam/iam_policies_routes.go", iamPoliciesRoutes(), check)
	write("security/iam/iam_policies_dispatch.go", transcodedDispatch(iamPoliciesRPC), check)
}

// iamPoliciesRoutes generates the IAM v2 Policies route slice.
func iamPoliciesRoutes() []byte {
	return routeTable("IAMPoliciesRoutes", "is the IAM v2 deny policy route table slice.", handWritten("iampolicies.go"), iamPoliciesRows())
}

// iamPoliciesRows lists the IAM v2 REST routes, named by their iam v2
// discovery method IDs. The attachment point is one URL-encoded segment
// (cloudresourcemanager.googleapis.com%2Fprojects%2FP).
func iamPoliciesRows() []route {
	const parent = "/v2/policies/{attachmentPoint}/{kind}"
	const policy = parent + "/{policy}"
	return []route{
		{"iam.policies.listPolicies", http.MethodGet, parent, false, false},
		{"iam.policies.createPolicy", http.MethodPost, parent, true, false},
		{"iam.policies.get", http.MethodGet, policy, false, false},
		{"iam.policies.update", http.MethodPut, policy, true, false},
		{"iam.policies.delete", http.MethodDelete, policy, false, false},
		{"iam.policies.operations.get", http.MethodGet, policy + "/operations/{operation}", false, false},
	}
}

// iamPoliciesRPC is the google.iam.v2.Policies surface. The Operations
// mixin's GetOperation binds the policy operation names, as iam_v2.yaml
// does (/v2/{name=policies/*/*/*/operations/*}).
var iamPoliciesRPC = transcodedService{
	service: iamv2pb.File_google_iam_v2_policy_proto.Services().ByName("Policies"),
	alias:   "iamv2pb", importPath: "cloud.google.com/go/iam/apiv2/iampb",
	variable: "IAMPoliciesMethods", bindings: "IAMPoliciesTranscoding",
	rest: map[string]string{
		"ListPolicies": "iam.policies.listPolicies", "CreatePolicy": "iam.policies.createPolicy",
		"GetPolicy": "iam.policies.get", "UpdatePolicy": "iam.policies.update", "DeletePolicy": "iam.policies.delete",
	},
	routes: iamPoliciesRows(),
	mixins: []mixinBinding{
		{longrunningOperations.Methods().ByName("GetOperation"), "iam.policies.operations.get",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v2/{name=policies/*/*/*/operations/*}"}}},
	},
}
