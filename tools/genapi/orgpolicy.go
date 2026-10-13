package main

import (
	"net/http"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
)

// orgPolicyParents are the three parent collections every Organization
// Policy v2 method binds, in the order of the proto's HTTP bindings: the
// primary binding names projects, the additional ones folders and
// organizations.
var orgPolicyParents = []struct{ collection, variable string }{
	{"projects", "project"}, {"folders", "folder"}, {"organizations", "organization"},
}

// writeOrgPolicy writes the route table and the transcoded service.
func writeOrgPolicy(check bool) {
	write("security/orgpolicy/orgpolicy_routes.go", orgPolicyRoutes(), check)
	write("security/orgpolicy/orgpolicy_dispatch.go", transcodedDispatch(orgPolicyRPC), check)
}

// orgPolicyRoutes generates the Organization Policy route slice.
func orgPolicyRoutes() []byte {
	return routeTable("OrgPolicyRoutes", "is the Organization Policy v2 route table slice.", handWritten("orgpolicy.go"), orgPolicyRows())
}

// orgPolicyRows lists the v2 REST routes under each parent, with the
// orgpolicy v2 discovery method IDs. Custom constraints have no route.
func orgPolicyRows() []route {
	rows := make([]route, 0, 7*len(orgPolicyParents))
	for _, parent := range orgPolicyParents {
		base := "/v2/" + parent.collection + "/{" + parent.variable + "}"
		id := "orgpolicy." + parent.collection + "."
		rows = append(rows,
			route{id + "constraints.list", http.MethodGet, base + "/constraints", false, false},
			route{id + "policies.list", http.MethodGet, base + "/policies", false, false},
			route{id + "policies.create", http.MethodPost, base + "/policies", true, false},
			route{id + "policies.get", http.MethodGet, base + "/policies/{policy}", false, false},
			route{id + "policies.getEffectivePolicy", http.MethodGet, base + "/policies/{policy}:getEffectivePolicy", false, false},
			route{id + "policies.patch", http.MethodPatch, base + "/policies/{policy}", true, false},
			route{id + "policies.delete", http.MethodDelete, base + "/policies/{policy}", false, false},
		)
	}
	return rows
}

// orgPolicyMethods pairs each served gRPC method with its REST method under
// one parent collection.
func orgPolicyMethods(collection string) map[string]string {
	id := "orgpolicy." + collection + "."
	return map[string]string{
		"ListConstraints": id + "constraints.list", "ListPolicies": id + "policies.list",
		"GetPolicy": id + "policies.get", "GetEffectivePolicy": id + "policies.getEffectivePolicy",
		"CreatePolicy": id + "policies.create", "UpdatePolicy": id + "policies.patch", "DeletePolicy": id + "policies.delete",
	}
}

// orgPolicyRPC is the google.cloud.orgpolicy.v2.OrgPolicy surface. Each
// served method's primary binding names projects; its additional bindings
// (folders, organizations) transcode onto their own REST routes. The custom
// constraint methods reach the coverage gap.
var orgPolicyRPC = func() transcodedService {
	alternates := map[string][]string{}
	for method := range orgPolicyMethods("projects") {
		alternates[method] = []string{orgPolicyMethods("folders")[method], orgPolicyMethods("organizations")[method]}
	}
	return transcodedService{
		service: orgpolicypb.File_google_cloud_orgpolicy_v2_orgpolicy_proto.Services().ByName("OrgPolicy"),
		alias:   "orgpolicypb", importPath: "cloud.google.com/go/orgpolicy/apiv2/orgpolicypb",
		variable: "OrgPolicyMethods", bindings: "OrgPolicyTranscoding",
		rest: orgPolicyMethods("projects"), alternates: alternates, routes: orgPolicyRows(),
	}
}()
