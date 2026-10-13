package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// transcodedService is a gRPC service whose methods share the provider of an
// existing REST route table. Every method gets a forwarding stub; only the
// methods in rest are transcoded onto their paired REST route, the rest reach
// the gateway's explicit coverage gap.
type transcodedService struct {
	service           protoreflect.ServiceDescriptor
	alias, importPath string
	variable          string
	bindings          string
	rest              map[string]string // gRPC method name to REST route ID
	// alternates pairs a method's additional HTTP bindings, in order, with
	// REST route IDs ("" leaves one unserved). The gateway picks the binding
	// whose pattern matches the request's resource.
	alternates map[string][]string
	routes     []route
	mixins     []mixinBinding
}

// mixinBinding pairs a shared mixin RPC (google.iam.v1.IAMPolicy) with one of
// this service's REST routes for the resources its path pattern names. The
// gateway picks the binding whose pattern matches the request's resource; a
// resource no binding names stays with the RPC's own owner.
type mixinBinding struct {
	method protoreflect.MethodDescriptor
	rest   string
	rule   *annotations.HttpRule
}

var secretManagerRPC = transcodedService{
	service: secretmanagerpb.File_google_cloud_secretmanager_v1_service_proto.Services().ByName("SecretManagerService"),
	alias:   "secretmanagerpb", importPath: "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb",
	variable: "SecretManagerMethods", bindings: "SecretManagerTranscoding",
	rest: map[string]string{
		"ListSecrets": "secretmanager.secrets.list", "CreateSecret": "secretmanager.secrets.create",
		"GetSecret": "secretmanager.secrets.get", "UpdateSecret": "secretmanager.secrets.update",
		"DeleteSecret": "secretmanager.secrets.delete", getIamPolicy: "secretmanager.secrets.getIamPolicy",
		"SetIamPolicy": "secretmanager.secrets.setIamPolicy", "AddSecretVersion": "secretmanager.versions.add",
		"ListSecretVersions": "secretmanager.versions.list", "GetSecretVersion": "secretmanager.versions.get",
		"AccessSecretVersion": "secretmanager.versions.access", "DestroySecretVersion": "secretmanager.versions.destroy",
		"DisableSecretVersion": "secretmanager.versions.disable", "EnableSecretVersion": "secretmanager.versions.enable",
	},
	routes: secretManagerRows(),
}

// cloudTasksRPC is the v2 gRPC surface the current client libraries use. Its
// methods transcode onto the v2beta3 REST routes, whose provider is the only
// Cloud Tasks implementation.
var cloudTasksRPC = transcodedService{
	service: cloudtaskspb.File_google_cloud_tasks_v2_cloudtasks_proto.Services().ByName("CloudTasks"),
	alias:   "cloudtaskspb", importPath: "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb",
	variable: "CloudTasksMethods", bindings: "CloudTasksTranscoding",
	rest: map[string]string{
		"ListQueues": "cloudtasks.queues.list", "GetQueue": "cloudtasks.queues.get",
		"CreateQueue": "cloudtasks.queues.create", "UpdateQueue": "cloudtasks.queues.update",
		"DeleteQueue": "cloudtasks.queues.delete", "PurgeQueue": "cloudtasks.queues.purge",
		"PauseQueue": "cloudtasks.queues.pause", "ResumeQueue": "cloudtasks.queues.resume",
		getIamPolicy: cloudTasksGetIamPolicy, "SetIamPolicy": "cloudtasks.queues.setIamPolicy",
		"ListTasks": "cloudtasks.tasks.list", "GetTask": "cloudtasks.tasks.get",
		"CreateTask": "cloudtasks.tasks.create", "DeleteTask": "cloudtasks.tasks.delete",
		"RunTask": "cloudtasks.tasks.run",
	},
	routes: cloudTasksRows(),
}

// getIamPolicy names the IAM policy read method of every service.
const getIamPolicy = "GetIamPolicy"

// iamPolicy is the shared google.iam.v1.IAMPolicy mixin service.
var iamPolicy = iampb.File_google_iam_v1_iam_policy_proto.Services().ByName("IAMPolicy")

// kmsIAM binds one IAMPolicy method to a Cloud KMS route. KMS's service config
// (google/cloud/kms/v1/cloudkms_v1.yaml) binds the mixin per resource kind
// and GetIamPolicy to GET, unlike the mixin's own POST annotation.
func kmsIAM(method, rest, path string) mixinBinding {
	rule := &annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: path}, Body: "*"}
	if method == getIamPolicy {
		rule = &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: path}}
	}
	return mixinBinding{iamPolicy.Methods().ByName(protoreflect.Name(method)), rest, rule}
}

// cloudKMSRPC is the KeyManagementService surface of the Cloud KMS REST
// provider, plus the IAM policy mixin on key rings and crypto keys.
var cloudKMSRPC = transcodedService{
	service: kmspb.File_google_cloud_kms_v1_service_proto.Services().ByName("KeyManagementService"),
	alias:   "kmspb", importPath: "cloud.google.com/go/kms/apiv1/kmspb",
	variable: "CloudKMSMethods", bindings: "CloudKMSTranscoding",
	rest: map[string]string{
		"CreateKeyRing": "cloudkms.keyRings.create", "GetKeyRing": "cloudkms.keyRings.get", "ListKeyRings": "cloudkms.keyRings.list",
		"CreateCryptoKey": "cloudkms.cryptoKeys.create", "GetCryptoKey": "cloudkms.cryptoKeys.get",
		"ListCryptoKeys": "cloudkms.cryptoKeys.list", "UpdateCryptoKey": "cloudkms.cryptoKeys.update",
		"UpdateCryptoKeyPrimaryVersion": "cloudkms.cryptoKeys.updatePrimaryVersion",
		"Encrypt":                       "cloudkms.cryptoKeys.encrypt", "Decrypt": "cloudkms.cryptoKeys.decrypt",
		"CreateCryptoKeyVersion": "cloudkms.cryptoKeyVersions.create", "GetCryptoKeyVersion": "cloudkms.cryptoKeyVersions.get",
		"ListCryptoKeyVersions": "cloudkms.cryptoKeyVersions.list", "UpdateCryptoKeyVersion": "cloudkms.cryptoKeyVersions.patch",
		"DestroyCryptoKeyVersion": "cloudkms.cryptoKeyVersions.destroy", "RestoreCryptoKeyVersion": "cloudkms.cryptoKeyVersions.restore",
		"AsymmetricSign": "cloudkms.cryptoKeyVersions.asymmetricSign", "AsymmetricDecrypt": "cloudkms.cryptoKeyVersions.asymmetricDecrypt",
		"GetPublicKey": "cloudkms.cryptoKeyVersions.getPublicKey", "MacSign": "cloudkms.cryptoKeyVersions.macSign",
		"MacVerify": "cloudkms.cryptoKeyVersions.macVerify",
	},
	routes: cloudKMSRows(),
	mixins: []mixinBinding{
		kmsIAM(getIamPolicy, "cloudkms.keyRings.getIamPolicy", "/v1/{resource=projects/*/locations/*/keyRings/*}:getIamPolicy"),
		kmsIAM("SetIamPolicy", "cloudkms.keyRings.setIamPolicy", "/v1/{resource=projects/*/locations/*/keyRings/*}:setIamPolicy"),
		kmsIAM(getIamPolicy, "cloudkms.cryptoKeys.getIamPolicy", "/v1/{resource=projects/*/locations/*/keyRings/*/cryptoKeys/*}:getIamPolicy"),
		kmsIAM("SetIamPolicy", "cloudkms.cryptoKeys.setIamPolicy", "/v1/{resource=projects/*/locations/*/keyRings/*/cryptoKeys/*}:setIamPolicy"),
	},
}

// longrunningOperations is the shared google.longrunning.Operations mixin
// service that clients poll on the same connection as the API.
var longrunningOperations = longrunningpb.File_google_longrunning_operations_proto.Services().ByName("Operations")

// runOperation is the operation name binding of the Cloud Run v2 service
// config (google/cloud/run/v2/run_v2.yaml).
const runOperation = "/v2/{name=projects/*/locations/*/operations/*}"

// runServicesRPC is the Cloud Run Admin v2 Services surface. Its mixins bind
// google.longrunning.Operations to the v2 operations routes for Cloud Run
// operation names, as run_v2.yaml rebinds them; the mixin's own binding
// (/v1/{name=operations/**}) names no resource kind.
var runServicesRPC = transcodedService{
	service: runpb.File_google_cloud_run_v2_service_proto.Services().ByName("Services"),
	alias:   "runpb", importPath: "cloud.google.com/go/run/apiv2/runpb",
	variable: "RunServicesMethods", bindings: "RunServicesTranscoding",
	rest: map[string]string{
		"CreateService": "run.services.create", "GetService": "run.services.get", "ListServices": "run.services.list",
		"UpdateService": "run.services.update", "DeleteService": "run.services.delete",
		getIamPolicy: "run.services.getIamPolicy", setIamPolicy: "run.services.setIamPolicy",
	},
	routes: runRows(),
	mixins: []mixinBinding{
		{longrunningOperations.Methods().ByName("ListOperations"), "run.operations.list",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v2/{name=projects/*/locations/*}/operations"}}},
		{longrunningOperations.Methods().ByName("GetOperation"), "run.operations.get",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: runOperation}}},
		{longrunningOperations.Methods().ByName("DeleteOperation"), "run.operations.delete",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Delete{Delete: runOperation}}},
		{longrunningOperations.Methods().ByName("WaitOperation"), "run.operations.wait",
			&annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: runOperation + ":wait"}, Body: "*"}},
	},
}

// setIamPolicy names the IAM policy write method of every service.
const setIamPolicy = "SetIamPolicy"

// runRevisionsRPC is the Cloud Run Admin v2 Revisions surface.
var runRevisionsRPC = transcodedService{
	service: runpb.File_google_cloud_run_v2_revision_proto.Services().ByName("Revisions"),
	alias:   "runpb", importPath: "cloud.google.com/go/run/apiv2/runpb",
	variable: "RunRevisionsMethods", bindings: "RunRevisionsTranscoding",
	rest:   map[string]string{"GetRevision": "run.revisions.get", "ListRevisions": "run.revisions.list", "DeleteRevision": "run.revisions.delete"},
	routes: runRows(),
}

// operationsDispatch generates the shared google.longrunning.Operations
// stubs. Like IAMPolicy its selector is unconstrained: each owning service's
// mixin bindings pick the operation names it serves, and any other name is a
// coverage gap.
func operationsDispatch() []byte {
	d := transcodedService{service: longrunningOperations, alias: "longrunningpb", importPath: "cloud.google.com/go/longrunning/autogen/longrunningpb"}
	imports := map[string]string{d.alias: d.importPath}
	var stubs, descriptors bytes.Buffer
	stubs.WriteString("type OperationsServer struct { longrunningpb.UnimplementedOperationsServer; Dispatch contract.PubSubDispatch }\n")
	descriptors.WriteString("var OperationsMethods = []contract.RPC{\n")
	for i := range longrunningOperations.Methods().Len() {
		m := longrunningOperations.Methods().Get(i)
		id := "/" + string(longrunningOperations.FullName()) + "/" + string(m.Name())
		input, output := d.messageType(imports, m.Input()), d.messageType(imports, m.Output())
		unaryStub(&stubs, "Operations", string(m.Name()), input, output, id)
		rpcDescriptor(&descriptors, id, "name", "**", false, input, output)
	}
	descriptors.WriteString("}\n")
	out := header("// Code generated by tools/genapi; DO NOT EDIT.\npackage generated\nimport (\n \"context\"\n \"github.com/evanmarshall/gcp-local/api/generated/contract\"\n")
	out.WriteString(importLines(imports))
	out.WriteString(" \"google.golang.org/grpc/codes\"\n \"google.golang.org/grpc/status\"\n \"google.golang.org/protobuf/proto\"\n)\n")
	out.Write(stubs.Bytes())
	out.Write(descriptors.Bytes())
	return out.Bytes()
}

// transcodedImports are the packages of message types outside the service's
// own package that these services use.
var transcodedImports = map[protoreflect.FullName]struct{ alias, path string }{
	"google.iam.v1":      {"iampb", "cloud.google.com/go/iam/apiv1/iampb"},
	"google.longrunning": {"longrunningpb", "cloud.google.com/go/longrunning/autogen/longrunningpb"},
	"google.protobuf":    {"emptypb", "google.golang.org/protobuf/types/known/emptypb"},
}

// transcodedDispatch generates the stubs, resource selectors, and REST
// bindings of one transcoded service.
func transcodedDispatch(d transcodedService) []byte {
	name := string(d.service.Name())
	imports := map[string]string{d.alias: d.importPath}
	typeOf := func(m protoreflect.MessageDescriptor) string { return d.messageType(imports, m) }
	var stubs, descriptors, bindings bytes.Buffer
	fmt.Fprintf(&stubs, "type %sServer struct { %s.Unimplemented%sServer; Dispatch contract.PubSubDispatch }\n", name, d.alias, name)
	fmt.Fprintf(&descriptors, "var %s = []contract.RPC{\n", d.variable)
	fmt.Fprintf(&bindings, "var %s = []contract.Transcoding{\n", d.bindings)
	paired := 0
	for i := range d.service.Methods().Len() {
		m := d.service.Methods().Get(i)
		methodName := string(m.Name())
		if m.IsStreamingClient() || m.IsStreamingServer() {
			panic("Unsupported streaming method " + methodName + "; update generator")
		}
		id := "/" + string(d.service.FullName()) + "/" + methodName
		input, output := typeOf(m.Input()), typeOf(m.Output())
		rule, _ := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule)
		_, served := d.rest[methodName]
		unaryStub(&stubs, name, methodName, input, output, id)
		field, pattern := transcodedSelector(id, rule, served)
		if alternates := d.alternates[methodName]; len(alternates) > 0 {
			field, pattern = alternateSelector(id, rule, alternates), "**"
		}
		rpcDescriptor(&descriptors, id, field, pattern, false, input, output)
		if served {
			paired++
		}
		bindings.WriteString(d.methodRows(m, id, rule, field))
	}
	if paired != len(d.rest) {
		panic("A paired method is missing from " + name)
	}
	for _, mixin := range d.mixins {
		id := "/" + string(mixin.method.Parent().FullName()) + "/" + string(mixin.method.Name())
		field, _ := bindingSelector(id, mixin.rule)
		bindings.WriteString(bindingRow(d.routes, mixin.method, id, mixin.rest, mixin.rule, field, nil))
	}
	descriptors.WriteString("}\n")
	bindings.WriteString("}\n")
	out := header("// Code generated by tools/genapi; DO NOT EDIT.\npackage generated\nimport (\n \"context\"\n \"github.com/evanmarshall/gcp-local/api/generated/contract\"\n")
	out.WriteString(importLines(imports))
	out.WriteString(" \"google.golang.org/grpc/codes\"\n \"google.golang.org/grpc/status\"\n \"google.golang.org/protobuf/proto\"\n)\n")
	out.Write(stubs.Bytes())
	out.Write(descriptors.Bytes())
	fmt.Fprintf(&out, "// %s pairs each served %s method with the REST route that shares its provider method.\n", d.bindings, name)
	out.Write(bindings.Bytes())
	return out.Bytes()
}

// methodRows renders one method's Transcoding rows: its primary binding when
// rest pairs it, and each additional binding that alternates pairs.
func (d transcodedService) methodRows(m protoreflect.MethodDescriptor, id string, rule *annotations.HttpRule, field string) string {
	var rows strings.Builder
	alternates := d.alternates[string(m.Name())]
	if restID, served := d.rest[string(m.Name())]; served {
		rows.WriteString(bindingRow(d.routes, m, id, restID, rule, field, alternates))
	}
	for index, extra := range rule.GetAdditionalBindings() {
		if index < len(alternates) && alternates[index] != "" {
			rows.WriteString(bindingRow(d.routes, m, id, alternates[index], extra, field, nil))
		}
	}
	return rows.String()
}

// importLines renders aliased imports in a stable order.
func importLines(imports map[string]string) string {
	aliases := make([]string, 0, len(imports))
	for alias := range imports {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	var lines strings.Builder
	for _, alias := range aliases {
		fmt.Fprintf(&lines, " %s %q\n", alias, imports[alias])
	}
	return lines.String()
}

// messageType names a message's Go type and records the import it needs.
func (d transcodedService) messageType(imports map[string]string, m protoreflect.MessageDescriptor) string {
	pkg := m.ParentFile().Package()
	if pkg == d.service.ParentFile().Package() {
		return d.alias + "." + string(m.Name())
	}
	external, ok := transcodedImports[pkg]
	if !ok {
		panic("Unknown external message type: " + string(m.FullName()))
	}
	imports[external.alias] = external.path
	return external.alias + "." + string(m.Name())
}

// bindingRow renders one Transcoding row after checking that the paired REST
// route exists with the binding's verb and that every other binding selects
// the same field. Additional bindings served by an alternate route are not
// listed as unserved.
func bindingRow(routes []route, m protoreflect.MethodDescriptor, id, restID string, rule *annotations.HttpRule, field string, alternates []string) string {
	verb := ruleVerb(rule)
	if !hasRoute(routes, restID, verb) {
		panic("No " + verb + " REST route " + restID + " for " + id)
	}
	unserved := []string{}
	for index, extra := range rule.GetAdditionalBindings() {
		extraField, extraPattern := bindingSelector(id, extra)
		if extraField != field {
			panic("Additional binding of " + id + " selects another field")
		}
		if index >= len(alternates) || alternates[index] == "" {
			unserved = append(unserved, fmt.Sprintf("%q", extraPattern))
		}
	}
	others := "nil"
	if len(unserved) > 0 {
		others = "[]string{" + strings.Join(unserved, ",") + "}"
	}
	fieldName := ""
	if field != "" {
		fieldName = jsonName(m.Input(), strings.SplitN(field, ".", 2)[0])
	}
	return fmt.Sprintf("{RPC: %q, REST: %q, Verb: %q, Path: %q, Field: %q, Body: %q, Unserved: %s},\n", id, restID, verb, rulePath(rule),
		fieldName, bodyName(m.Input(), rule.GetBody()), others)
}

// transcodedSelector reads a method's resource selector. A served method has
// at most one resource variable; one with none (Resource Manager ListFolders
// and SearchProjects at /v3/folders and /v3/projects:search) gets no
// selector, and its provider authorizes the parent the request names. An
// unserved method only needs a selector for its gap; one with no HTTP
// binding or no resource variable (IAM ListRoles, Spanner
// InternalUpdateGraphOperation) has none and reaches the gap.
func transcodedSelector(id string, rule *annotations.HttpRule, served bool) (string, string) {
	if served && rule == nil {
		panic("Missing HTTP annotation for " + id)
	}
	variables := len(resourceVariable.FindAllStringSubmatch(rulePath(rule), -1))
	if rule == nil || variables == 0 || !served && variables != 1 {
		return "", ""
	}
	return bindingSelector(id, rule)
}

// alternateSelector reads the resource field shared by a method's served
// additional bindings; its primary binding may have no resource variable
// (IAM ListRoles lists predefined roles at /v1/roles).
func alternateSelector(id string, rule *annotations.HttpRule, alternates []string) string {
	field := ""
	for index, extra := range rule.GetAdditionalBindings() {
		if index >= len(alternates) || alternates[index] == "" {
			continue
		}
		extraField, _ := bindingSelector(id, extra)
		if field != "" && extraField != field {
			panic("Alternate bindings of " + id + " select different fields")
		}
		field = extraField
	}
	if field == "" {
		panic("No served alternate binding for " + id)
	}
	return field
}

// bindingSelector reads the single resource variable of one HTTP binding.
func bindingSelector(id string, rule *annotations.HttpRule) (string, string) {
	matches := resourceVariable.FindAllStringSubmatch(rulePath(rule), -1)
	if len(matches) != 1 {
		panic("Expected one resource variable for " + id)
	}
	return matches[0][1], matches[0][2]
}

// ruleVerb names the HTTP verb of one binding.
func ruleVerb(rule *annotations.HttpRule) string {
	switch {
	case rule.GetGet() != "":
		return "GET"
	case rule.GetPost() != "":
		return "POST"
	case rule.GetPatch() != "":
		return "PATCH"
	case rule.GetDelete() != "":
		return "DELETE"
	case rule.GetPut() != "":
		return "PUT"
	}
	panic("Unsupported HTTP binding verb")
}

// hasRoute reports whether the REST table has a row with this ID and verb.
func hasRoute(routes []route, id, verb string) bool {
	for _, r := range routes {
		if r.ID == id && r.Verb == verb {
			return true
		}
	}
	return false
}

// jsonName returns the JSON name of one top-level request field.
func jsonName(message protoreflect.MessageDescriptor, field string) string {
	descriptor := message.Fields().ByName(protoreflect.Name(field))
	if descriptor == nil {
		panic("Unknown request field " + field + " in " + string(message.FullName()))
	}
	return descriptor.JSONName()
}

// bodyName keeps "*" and "" and renames a body field to its JSON name.
func bodyName(message protoreflect.MessageDescriptor, body string) string {
	if body == "" || body == "*" {
		return body
	}
	return jsonName(message, body)
}
