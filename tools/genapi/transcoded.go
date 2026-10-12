package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
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
	routes            []route
	mixins            []mixinBinding
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

// transcodedImports are the packages of message types outside the service's
// own package that these services use.
var transcodedImports = map[protoreflect.FullName]struct{ alias, path string }{
	"google.iam.v1":      {"iampb", "cloud.google.com/go/iam/apiv1/iampb"},
	"google.longrunning": {"longrunningpb", "cloud.google.com/go/longrunning/autogen/longrunningpb"},
	"google.protobuf":    {"emptypb", "google.golang.org/protobuf/types/known/emptypb"},
}

// transcodedDispatch generates the stubs, resource selectors, and REST
// bindings of one transcoded service. declare emits the shared binding type.
func transcodedDispatch(d transcodedService, declare bool) []byte {
	name := string(d.service.Name())
	imports := map[string]string{d.alias: d.importPath}
	typeOf := func(m protoreflect.MessageDescriptor) string { return d.messageType(imports, m) }
	var stubs, descriptors, bindings bytes.Buffer
	fmt.Fprintf(&stubs, "type %sServer struct { %s.Unimplemented%sServer; Dispatch PubSubDispatch }\n", name, d.alias, name)
	fmt.Fprintf(&descriptors, "var %s = []RPC{\n", d.variable)
	fmt.Fprintf(&bindings, "var %s = []Transcoding{\n", d.bindings)
	paired := 0
	for i := range d.service.Methods().Len() {
		m := d.service.Methods().Get(i)
		methodName := string(m.Name())
		if m.IsStreamingClient() || m.IsStreamingServer() {
			panic("Unsupported streaming method " + methodName + "; update generator")
		}
		id := "/" + string(d.service.FullName()) + "/" + methodName
		input, output := typeOf(m.Input()), typeOf(m.Output())
		rule, ok := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule)
		if !ok || rule == nil {
			panic("Missing HTTP annotation for " + id)
		}
		field, pattern := bindingSelector(id, rule)
		unaryStub(&stubs, name, methodName, input, output, id)
		rpcDescriptor(&descriptors, id, field, pattern, false, input, output)
		restID, served := d.rest[methodName]
		if !served {
			continue
		}
		paired++
		bindings.WriteString(bindingRow(d.routes, m, id, restID, rule, field))
	}
	if paired != len(d.rest) {
		panic("A paired method is missing from " + name)
	}
	for _, mixin := range d.mixins {
		id := "/" + string(mixin.method.Parent().FullName()) + "/" + string(mixin.method.Name())
		field, _ := bindingSelector(id, mixin.rule)
		bindings.WriteString(bindingRow(d.routes, mixin.method, id, mixin.rest, mixin.rule, field))
	}
	descriptors.WriteString("}\n")
	bindings.WriteString("}\n")
	out := header("// Code generated by tools/genapi; DO NOT EDIT.\npackage generated\nimport (\n \"context\"\n")
	out.WriteString(importLines(imports))
	out.WriteString(" \"google.golang.org/grpc/codes\"\n \"google.golang.org/grpc/status\"\n \"google.golang.org/protobuf/proto\"\n)\n")
	if declare {
		out.WriteString(transcodingType)
	}
	out.Write(stubs.Bytes())
	out.Write(descriptors.Bytes())
	fmt.Fprintf(&out, "// %s pairs each served %s method with the REST route that shares its provider method.\n", d.bindings, name)
	out.Write(bindings.Bytes())
	return out.Bytes()
}

// transcodingType is the generated shape of one gRPC-to-REST binding.
const transcodingType = `// Transcoding pairs one gRPC method with the REST route whose provider method
// serves it. Verb, Path, and Body come from the method's primary
// google.api.http binding; Field is the JSON name of the top-level request
// field the path binds; Body is "*", "", or a JSON field name. Unserved lists
// the resource patterns of the method's other bindings, which no route serves.
type Transcoding struct { RPC, REST, Verb, Path, Field, Body string; Unserved []string }
`

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
// the same field.
func bindingRow(routes []route, m protoreflect.MethodDescriptor, id, restID string, rule *annotations.HttpRule, field string) string {
	verb := ruleVerb(rule)
	if !hasRoute(routes, restID, verb) {
		panic("No " + verb + " REST route " + restID + " for " + id)
	}
	others := "nil"
	if extras := rule.GetAdditionalBindings(); len(extras) > 0 {
		unserved := make([]string, 0, len(extras))
		for _, extra := range extras {
			extraField, extraPattern := bindingSelector(id, extra)
			if extraField != field {
				panic("Additional binding of " + id + " selects another field")
			}
			unserved = append(unserved, fmt.Sprintf("%q", extraPattern))
		}
		others = "[]string{" + strings.Join(unserved, ",") + "}"
	}
	return fmt.Sprintf("{%q,%q,%q,%q,%q,%q,%s},\n", id, restID, verb, rulePath(rule),
		jsonName(m.Input(), strings.SplitN(field, ".", 2)[0]), bodyName(m.Input(), rule.GetBody()), others)
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
