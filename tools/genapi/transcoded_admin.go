package main

import (
	btadminpb "cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	iamadminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/genproto/googleapis/api/annotations"
)

// adminpb is the package alias of the Bigtable and IAM admin bindings.
const adminpb = "adminpb"

// spannerGetOperation binds google.longrunning.Operations.GetOperation to one
// Spanner operations route, as the Spanner admin service configs
// (spanner_admin_instance.yaml, spanner_admin_database.yaml) rebind it.
func spannerGetOperation(rest, path string) mixinBinding {
	return mixinBinding{longrunningOperations.Methods().ByName("GetOperation"), rest,
		&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: path}}}
}

// spannerInstanceAdminRPC is the Spanner InstanceAdmin surface of the Spanner
// REST provider, plus instance operation polling.
var spannerInstanceAdminRPC = transcodedService{
	service: instancepb.File_google_spanner_admin_instance_v1_spanner_instance_admin_proto.Services().ByName("InstanceAdmin"),
	alias:   "instancepb", importPath: "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb",
	variable: "SpannerInstanceAdminMethods", bindings: "SpannerInstanceAdminTranscoding",
	rest: map[string]string{
		"ListInstanceConfigs": "spanner.instanceConfigs.list", "GetInstanceConfig": "spanner.instanceConfigs.get",
		"ListInstances": "spanner.instances.list", "GetInstance": "spanner.instances.get",
		"CreateInstance": "spanner.instances.create", "UpdateInstance": "spanner.instances.update",
		"DeleteInstance": "spanner.instances.delete",
		getIamPolicy:     "spanner.instances.getIamPolicy", setIamPolicy: "spanner.instances.setIamPolicy",
	},
	routes: spannerRows(),
	mixins: []mixinBinding{
		spannerGetOperation("spanner.instances.operations.get", "/v1/{name=projects/*/instances/*/operations/*}"),
	},
}

// spannerDatabaseAdminRPC is the Spanner DatabaseAdmin surface of the Spanner
// REST provider, plus database operation polling.
var spannerDatabaseAdminRPC = transcodedService{
	service: databasepb.File_google_spanner_admin_database_v1_spanner_database_admin_proto.Services().ByName("DatabaseAdmin"),
	alias:   "databasepb", importPath: "cloud.google.com/go/spanner/admin/database/apiv1/databasepb",
	variable: "SpannerDatabaseAdminMethods", bindings: "SpannerDatabaseAdminTranscoding",
	rest: map[string]string{
		"ListDatabases": "spanner.databases.list", "CreateDatabase": "spanner.databases.create",
		"GetDatabase": "spanner.databases.get", "UpdateDatabase": "spanner.databases.update",
		"UpdateDatabaseDdl": "spanner.databases.applyDdl", "DropDatabase": "spanner.databases.delete",
		"GetDatabaseDdl": "spanner.databases.getDdl", "ListBackups": "spanner.backups.list",
		getIamPolicy: "spanner.databases.getIamPolicy", setIamPolicy: "spanner.databases.setIamPolicy",
	},
	routes: spannerRows(),
	mixins: []mixinBinding{
		spannerGetOperation("spanner.databases.operations.get", "/v1/{name=projects/*/instances/*/databases/*/operations/*}"),
	},
}

// bigtableOperations binds google.longrunning.Operations to one Bigtable
// operation name pattern, as the Bigtable admin service config
// (bigtableadmin_v2.yaml) binds them under /v2/{name=operations/**}.
func bigtableOperations(rest, pattern string) []mixinBinding {
	method := longrunningOperations.Methods().ByName
	return []mixinBinding{
		{method("GetOperation"), rest + ".get", &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v2/{name=" + pattern + "}"}}},
		{method("DeleteOperation"), rest + ".delete", &annotations.HttpRule{Pattern: &annotations.HttpRule_Delete{Delete: "/v2/{name=" + pattern + "}"}}},
		{method("CancelOperation"), rest + ".cancel", &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v2/{name=" + pattern + "}:cancel"}, Body: "*"}},
	}
}

// bigtableInstanceAdminRPC is the BigtableInstanceAdmin surface of the
// Bigtable REST provider, plus instance operation polling and listing.
var bigtableInstanceAdminRPC = transcodedService{
	service: btadminpb.File_google_bigtable_admin_v2_bigtable_instance_admin_proto.Services().ByName("BigtableInstanceAdmin"),
	alias:   adminpb, importPath: "cloud.google.com/go/bigtable/admin/apiv2/adminpb",
	variable: "BigtableInstanceAdminMethods", bindings: "BigtableInstanceAdminTranscoding",
	rest: map[string]string{
		"CreateInstance": "bigtable.instances.create", "GetInstance": "bigtable.instances.get",
		"ListInstances": "bigtable.instances.list", "DeleteInstance": "bigtable.instances.delete",
		"PartialUpdateInstance": "bigtable.instances.update",
		"GetCluster":            "bigtable.clusters.get", "ListClusters": "bigtable.clusters.list",
		getIamPolicy: "bigtable.instances.getIamPolicy", setIamPolicy: "bigtable.instances.setIamPolicy",
	},
	routes: bigtableRows(),
	mixins: append(bigtableOperations("bigtable.operations", "operations/projects/*/instances/*/locations/*/operations/*"),
		mixinBinding{longrunningOperations.Methods().ByName("ListOperations"), "bigtable.operations.list",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v2/{name=operations/projects/*}/operations"}}}),
}

// bigtableTableAdminRPC is the BigtableTableAdmin surface of the Bigtable REST
// provider.
var bigtableTableAdminRPC = transcodedService{
	service: btadminpb.File_google_bigtable_admin_v2_bigtable_table_admin_proto.Services().ByName("BigtableTableAdmin"),
	alias:   adminpb, importPath: "cloud.google.com/go/bigtable/admin/apiv2/adminpb",
	variable: "BigtableTableAdminMethods", bindings: "BigtableTableAdminTranscoding",
	rest: map[string]string{
		"CreateTable": "bigtable.tables.create", "ListTables": "bigtable.tables.list",
		"GetTable": "bigtable.tables.get", "UpdateTable": "bigtable.tables.update",
		"DeleteTable": "bigtable.tables.delete", "ModifyColumnFamilies": "bigtable.tables.modifyColumnFamilies",
		getIamPolicy: "bigtable.tables.getIamPolicy", setIamPolicy: "bigtable.tables.setIamPolicy",
	},
	routes: bigtableRows(),
	mixins: bigtableOperations("bigtable.tableOperations", "operations/projects/*/instances/*/tables/*/locations/*/operations/*"),
}

// iamAdminRPC is the google.iam.admin.v1.IAM service-account and role surface
// of the IAM REST provider. The REST update route is the PATCH method, so it
// serves PatchServiceAccount. SignBlob stays a gap because the REST signBlob
// route is the IAM Credentials API, whose request and response differ, and
// CreateServiceAccountKey because its key file's token_uri names the HTTP
// origin the request arrived on, which a gRPC request does not carry. Role
// methods bind organizations (or roles/* for GetRole) first and the other
// parents as additional bindings; ListRoles without a parent (predefined
// roles) has no resource variable and stays a gap.
var iamAdminRPC = transcodedService{
	service: iamadminpb.File_google_iam_admin_v1_iam_proto.Services().ByName("IAM"),
	alias:   adminpb, importPath: "cloud.google.com/go/iam/admin/apiv1/adminpb",
	variable: "IAMAdminMethods", bindings: "IAMAdminTranscoding",
	rest: map[string]string{
		"ListServiceAccounts": "iam.serviceAccounts.list", "GetServiceAccount": "iam.serviceAccounts.get",
		"CreateServiceAccount": "iam.serviceAccounts.create", "PatchServiceAccount": "iam.serviceAccounts.update",
		"DeleteServiceAccount": "iam.serviceAccounts.delete", "ListServiceAccountKeys": "iam.serviceAccountKeys.list",
		"GetServiceAccountKey": "iam.serviceAccountKeys.get", "DeleteServiceAccountKey": "iam.serviceAccountKeys.delete",
		getIamPolicy: "iam.serviceAccounts.getIamPolicy", setIamPolicy: "iam.serviceAccounts.setIamPolicy",
		"GetRole": "iam.roles.get", "CreateRole": "iam.organizations.roles.create",
		"UpdateRole": "iam.organizations.roles.patch", "DeleteRole": "iam.organizations.roles.delete",
		"UndeleteRole": "iam.organizations.roles.undelete",
	},
	alternates: map[string][]string{
		"GetRole":      {"iam.organizations.roles.get", "iam.projects.roles.get"},
		"ListRoles":    {"iam.organizations.roles.list", "iam.projects.roles.list"},
		"CreateRole":   {"iam.projects.roles.create"},
		"UpdateRole":   {"iam.projects.roles.patch"},
		"DeleteRole":   {"iam.projects.roles.delete"},
		"UndeleteRole": {"iam.projects.roles.undelete"},
	},
	routes: iamRows(),
}

// iamCredentialsRPC is the google.iam.credentials.v1.IAMCredentials surface of
// the IAM REST provider: all four methods share the IAM Credentials routes.
var iamCredentialsRPC = transcodedService{
	service: credentialspb.File_google_iam_credentials_v1_iamcredentials_proto.Services().ByName("IAMCredentials"),
	alias:   "credentialspb", importPath: "cloud.google.com/go/iam/credentials/apiv1/credentialspb",
	variable: "IAMCredentialsMethods", bindings: "IAMCredentialsTranscoding",
	rest: map[string]string{
		"GenerateAccessToken": "iamcredentials.projects.serviceAccounts.generateAccessToken",
		"GenerateIdToken":     "iamcredentials.projects.serviceAccounts.generateIdToken",
		"SignBlob":            "iamcredentials.projects.serviceAccounts.signBlob",
		"SignJwt":             "iamcredentials.projects.serviceAccounts.signJwt",
	},
	routes: iamRows(),
}
