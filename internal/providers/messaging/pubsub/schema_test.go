package pubsub

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/messaging/pubsub/schemadef"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	projectParent = "projects/project-a"
	protoSchemaID = "events"
	protoSchema1  = "projects/project-a/schemas/events"
	avroSchema1   = "projects/project-a/schemas/avro-events"
	schemaTopic   = "projects/project-a/topics/typed"
	protoEvent    = "syntax = \"proto3\";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n}\n"
	protoEventV2  = "syntax = \"proto3\";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n  string note = 3;\n}\n"
	avroEvent     = `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`
)

// applySchema stores Avro schemas the way the emulator returns them, without
// validating definitions; Protocol Buffer schemas must never reach it.
func (m *memoryPubSub) applySchema(in proto.Message) (proto.Message, bool, error) {
	if m.schemas == nil {
		m.schemas = map[string]*pb.Schema{}
	}
	switch r := in.(type) {
	case *pb.CreateSchemaRequest:
		if r.GetSchema().GetType() != pb.Schema_AVRO {
			return nil, true, status.Error(codes.Unimplemented, "Protocol buffer support not implemented in emulator")
		}
		name := r.GetParent() + "/schemas/" + r.GetSchemaId()
		m.schemas[name] = &pb.Schema{Name: name, Type: pb.Schema_AVRO, Definition: r.GetSchema().GetDefinition(), RevisionId: "a0000001"}
		return m.schemas[name], true, nil
	case *pb.GetSchemaRequest:
		if schema, found := m.schemas[r.GetName()]; found {
			return schema, true, nil
		}
		return nil, true, status.Error(codes.NotFound, errorSchemaNotFound)
	case *pb.ListSchemasRequest:
		list := &pb.ListSchemasResponse{}
		for _, schema := range m.schemas {
			list.Schemas = append(list.Schemas, schema)
		}
		return list, true, nil
	case *pb.DeleteSchemaRequest:
		delete(m.schemas, r.GetName())
		return &emptypb.Empty{}, true, nil
	}
	return nil, false, nil
}

// schemaHarness is a memory-backed provider and a caller acting as owner.
type schemaHarness struct {
	p        *Provider
	registry *gateway.Registry
	b        *memoryPubSub
	as       func(method, resource string, request proto.Message) (any, error)
}

func schemaFixture(t *testing.T) schemaHarness {
	t.Helper()
	p, registry, b := memoryFixture(t)
	return schemaHarness{p: p, registry: registry, b: b, as: func(method, resource string, request proto.Message) (any, error) {
		return invokeAs(registry, owner, method, resource, request)
	}}
}

// failIf fails the test with what and got when failed is true.
func failIf(t *testing.T, failed bool, what string, got any) {
	t.Helper()
	if failed {
		t.Fatal(what, got)
	}
}

// createProto creates the events Protocol Buffer schema as owner.
func createProto(t *testing.T, as func(string, string, proto.Message) (any, error)) *pb.Schema {
	t.Helper()
	created, err := as(schemaService+"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: protoSchemaID, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	must(t, err)
	return typed[*pb.Schema](t, created)
}

func TestProtoSchemaRevisionLifecycle(t *testing.T) {
	h := schemaFixture(t)
	b, as := h.b, h.as
	first := createProto(t, as)
	failIf(t, first.GetName() != protoSchema1 || len(first.GetRevisionId()) != 8 || first.GetRevisionCreateTime() == nil, "created", first)
	second, err := as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEventV2}})
	must(t, err)
	latest, err := as(schemaService+"GetSchema", protoSchema1, &pb.GetSchemaRequest{Name: protoSchema1})
	must(t, err)
	failIf(t, typed[*pb.Schema](t, latest).GetDefinition() != protoEventV2, "GetSchema did not return the latest revision in full", latest)
	old, err := as(schemaService+"GetSchema", protoSchema1+"@"+first.GetRevisionId(), &pb.GetSchemaRequest{Name: protoSchema1 + "@" + first.GetRevisionId(), View: pb.SchemaView_BASIC})
	must(t, err)
	s := typed[*pb.Schema](t, old)
	failIf(t, s.GetName() != protoSchema1+"@"+first.GetRevisionId() || s.GetDefinition() != "", "revision BASIC view", s)
	rolled, err := as(schemaService+"RollbackSchema", protoSchema1, &pb.RollbackSchemaRequest{Name: protoSchema1, RevisionId: first.GetRevisionId()})
	must(t, err)
	s = typed[*pb.Schema](t, rolled)
	failIf(t, s.GetDefinition() != protoEvent || s.GetRevisionId() == first.GetRevisionId() || s.GetName() != protoSchema1+"@"+s.GetRevisionId(), "rollback must add a revision copying the target", s)
	_, err = as(schemaService+"RollbackSchema", protoSchema1, &pb.RollbackSchemaRequest{Name: protoSchema1, RevisionId: "ffffffff"})
	expectRefusal(t, "rollback to an unknown revision", err, codes.NotFound, "Revision not found")
	revision := typed[*pb.Schema](t, second).GetRevisionId()
	deleted, err := as(schemaService+"DeleteSchemaRevision", protoSchema1+"@"+revision, &pb.DeleteSchemaRevisionRequest{Name: protoSchema1 + "@" + revision})
	must(t, err)
	s = typed[*pb.Schema](t, deleted)
	failIf(t, s.GetRevisionId() != revision || s.GetName() != "projects/123456/schemas/events@"+revision || s.GetDefinition() != "", "DeleteSchemaRevision returns the deleted revision under the project number", deleted)
	listed, err := as(schemaService+"ListSchemaRevisions", protoSchema1, &pb.ListSchemaRevisionsRequest{Name: protoSchema1})
	must(t, err)
	revisions := typed[*pb.ListSchemaRevisionsResponse](t, listed).GetSchemas()
	failIf(t, len(revisions) != 2 || revisions[1].GetName() != protoSchema1+"@"+first.GetRevisionId() || revisions[0].GetDefinition() != "", "revisions are listed newest first as SCHEMA@REVISION without definitions by default", revisions)
	_, err = as(schemaService+"DeleteSchemaRevision", protoSchema1+"@"+revision, &pb.DeleteSchemaRevisionRequest{Name: protoSchema1 + "@" + revision})
	expectRefusal(t, "deleted revision", err, codes.NotFound, "Schema revision not found")
	_, err = as(schemaService+"DeleteSchema", protoSchema1, &pb.DeleteSchemaRequest{Name: protoSchema1})
	must(t, err)
	_, err = as(schemaService+"GetSchema", protoSchema1, &pb.GetSchemaRequest{Name: protoSchema1})
	expectCode(t, err, codes.NotFound)
	failIf(t, b.called(schemaService+"CreateSchema")+b.called(schemaService+"CommitSchema")+b.called(schemaService+"DeleteSchema") != 0, "a Protocol Buffer schema reached the emulator", nil)
}

func TestProtoSchemaLimitsAndRefusals(t *testing.T) {
	h := schemaFixture(t)
	as := h.as
	only := createProto(t, as)
	_, err := as(schemaService+"DeleteSchemaRevision", protoSchema1+"@"+only.GetRevisionId(), &pb.DeleteSchemaRevisionRequest{Name: protoSchema1 + "@" + only.GetRevisionId()})
	expectRefusal(t, "last revision", err, codes.InvalidArgument, "Cannot delete last revision")
	deprecated := &pb.DeleteSchemaRevisionRequest{Name: protoSchema1}
	field := deprecated.ProtoReflect().Descriptor().Fields().ByName("revision_id")
	deprecated.ProtoReflect().Set(field, protoreflect.ValueOfString(only.GetRevisionId()))
	_, err = as(schemaService+"DeleteSchemaRevision", protoSchema1, deprecated)
	expectRefusal(t, "deprecated revision_id", err, codes.Unimplemented, "Pub/Sub coverage gap: field revision_id")
	_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_AVRO, Definition: avroEvent}})
	expectRefusal(t, "type change", err, codes.FailedPrecondition, "Failed with error: Types do not match)")
	_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "commit without a schema name", err, codes.InvalidArgument, "Invalid resource name given (name=).")
	_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: avroSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "commit naming another schema", err, codes.NotFound, "Resource not found (resource="+protoSchema1+").")
	_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: strings.Replace(protoEvent, "int32 count", "string count", 1)}})
	expectRefusal(t, "incompatible field type", err, codes.FailedPrecondition, "Event.count of type string is not compatible with Event.count of type int32")
	_, err = as(schemaService+"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: protoSchemaID, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "duplicate", err, codes.AlreadyExists, "Schema already exists")
	for range maxSchemaRevisions - 1 {
		_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
		must(t, err)
	}
	_, err = as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "21st revision", err, codes.ResourceExhausted, "maximum number of schema revisions")
	_, err = as(schemaService+"GetSchema", "projects/project-b/schemas/events", &pb.GetSchemaRequest{Name: "projects/project-b/schemas/events"})
	expectRefusal(t, "cross-project name", err, codes.Unimplemented, "Pub/Sub coverage gap: cross-project schemas")
	_, err = as(schemaService+"CommitSchema", protoSchema1+"@"+only.GetRevisionId(), &pb.CommitSchemaRequest{Name: protoSchema1 + "@" + only.GetRevisionId(), Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "commit to a revision name", err, codes.InvalidArgument, "Invalid schema revision name")
	_, err = as(schemaService+"DeleteSchemaRevision", protoSchema1, &pb.DeleteSchemaRevisionRequest{Name: protoSchema1})
	expectRefusal(t, "revision delete without a revision", err, codes.InvalidArgument, "SCHEMA@REVISION")
}

func TestProtoSchemaDefinitionRules(t *testing.T) {
	h := schemaFixture(t)
	b, as := h.b, h.as
	for _, tc := range []struct{ name, definition, message string }{
		{"syntax error", "message {", "Could not parse schema definition"},
		{"import", "syntax = \"proto3\";\nimport \"google/protobuf/timestamp.proto\";\nmessage Event { google.protobuf.Timestamp at = 1; }\n", "Could not parse schema definition"},
		{"two top-level types", "syntax = \"proto3\";\nmessage A {}\nmessage B {}\n", "exactly one top-level message"},
		{"enum only", "syntax = \"proto3\";\nenum Kind { KIND_UNSPECIFIED = 0; }\n", "exactly one top-level message"},
		{"oversized", strings.Repeat(" ", schemadef.MaxDefinitionBytes+1), "at most 300 KB"},
	} {
		_, err := as(schemaService+"ValidateSchema", projectParent, &pb.ValidateSchemaRequest{Parent: projectParent, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: tc.definition}})
		expectRefusal(t, tc.name, err, codes.InvalidArgument, tc.message)
	}
	nested := "syntax = \"proto2\";\nmessage Event {\n  message Inner { required string id = 1; }\n  optional Inner inner = 1;\n}\n"
	_, err := as(schemaService+"ValidateSchema", projectParent, &pb.ValidateSchemaRequest{Parent: projectParent, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: nested}})
	must(t, err)
	_, err = as(schemaService+"ValidateSchema", projectParent, &pb.ValidateSchemaRequest{Parent: projectParent, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER}})
	expectRefusal(t, "missing definition", err, codes.InvalidArgument, "Missing definition")
	failIf(t, b.called(schemaService+"ValidateSchema") != 0, "Protocol Buffer validation reached the emulator", nil)
}

func TestProtoValidateMessage(t *testing.T) {
	h := schemaFixture(t)
	as := h.as
	created := createProto(t, as)
	validate := func(request *pb.ValidateMessageRequest) error {
		request.Parent = projectParent
		_, err := as(schemaService+"ValidateMessage", projectParent, request)
		return err
	}
	named := func(name string, data []byte, encoding pb.Encoding) *pb.ValidateMessageRequest {
		return &pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Name{Name: name}, Message: data, Encoding: encoding}
	}
	must(t, validate(named(protoSchema1, []byte(`{"id":"a","count":1}`), pb.Encoding_JSON)))
	must(t, validate(named(protoSchema1+"@"+created.GetRevisionId(), []byte{0x0a, 0x01, 'a', 0x10, 0x02}, pb.Encoding_BINARY)))
	expectRefusal(t, "wrong JSON type", validate(named(protoSchema1, []byte(`{"id":5}`), pb.Encoding_JSON)), codes.InvalidArgument, "Could not parse message")
	expectRefusal(t, "unknown JSON field", validate(named(protoSchema1, []byte(`{"other":"a"}`), pb.Encoding_JSON)), codes.InvalidArgument, "Could not parse message")
	expectRefusal(t, "bad binary", validate(named(protoSchema1, []byte{0xff, 0xff}, pb.Encoding_BINARY)), codes.InvalidArgument, "Could not parse message")
	expectRefusal(t, "no encoding", validate(named(protoSchema1, []byte(`{}`), pb.Encoding_ENCODING_UNSPECIFIED)), codes.InvalidArgument, "No encoding provided.")
	expectRefusal(t, "unknown revision", validate(named(protoSchema1+"@ffffffff", []byte(`{}`), pb.Encoding_JSON)), codes.NotFound, "Revision not found")
	inline := &pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Schema{Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}}, Message: []byte(`{"id":"a"}`), Encoding: pb.Encoding_JSON}
	must(t, validate(inline))
}

func TestProtoTopicPublishValidation(t *testing.T) {
	h := schemaFixture(t)
	p, b, as := h.p, h.b, h.as
	first := createProto(t, as)
	_, err := as(schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEventV2}})
	must(t, err)
	settings := &pb.SchemaSettings{Schema: protoSchema1, Encoding: pb.Encoding_JSON, FirstRevisionId: first.GetRevisionId(), LastRevisionId: first.GetRevisionId()}
	created, err := as(publisher+"CreateTopic", schemaTopic, &pb.Topic{Name: schemaTopic, SchemaSettings: settings})
	must(t, err)
	failIf(t, !proto.Equal(typed[*pb.Topic](t, created).GetSchemaSettings(), settings) || b.topics[schemaTopic].GetSchemaSettings() != nil, "the provider keeps Protocol Buffer schema settings; the emulator sees none", created)
	_, err = as(subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: schemaTopic})
	must(t, err)
	publish := func(data string) error {
		_, err := as(publisher+"Publish", schemaTopic, &pb.PublishRequest{Topic: schemaTopic, Messages: []*pb.PubsubMessage{{Data: []byte(data)}}})
		return err
	}
	must(t, publish(`{"id":"a","count":1}`))
	// note exists only in the second revision, outside the topic's range.
	expectRefusal(t, "outside the revision range", publish(`{"id":"a","note":"x"}`), codes.InvalidArgument, "Could not parse message")
	expectRefusal(t, "invalid JSON", publish(`{"id":5}`), codes.InvalidArgument, "Could not parse message")
	failIf(t, b.called(publisher+"Publish") != 1, "an invalid message reached the emulator", nil)
	attributes := b.queues[testSub][0].GetAttributes()
	failIf(t, attributes["googclient_schemaname"] != protoSchema1 || attributes["googclient_schemaencoding"] != "JSON" || attributes["googclient_schemarevisionid"] != first.GetRevisionId(), "schema attributes", attributes)
	listed, err := as(publisher+"ListTopics", projectParent, &pb.ListTopicsRequest{Project: projectParent})
	must(t, err)
	topics := typed[*pb.ListTopicsResponse](t, listed).GetTopics()
	failIf(t, len(topics) != 1 || topics[0].GetSchemaSettings().GetSchema() != protoSchema1, "ListTopics settings", topics)
	_, err = as(schemaService+"DeleteSchema", protoSchema1, &pb.DeleteSchemaRequest{Name: protoSchema1})
	must(t, err)
	topic, err := as(publisher+"GetTopic", schemaTopic, &pb.GetTopicRequest{Topic: schemaTopic})
	must(t, err)
	s := typed[*pb.Topic](t, topic).GetSchemaSettings()
	failIf(t, s.GetSchema() != deletedSchema || s.GetFirstRevisionId() != deletedRevision || s.GetEncoding() != pb.Encoding_JSON, "deleted schema settings", s)
	expectRefusal(t, "deleted schema", publish(`{"id":"a"}`), codes.NotFound, "Schema associated with topic deleted.")
	_, err = as(publisher+"DeleteTopic", schemaTopic, &pb.DeleteTopicRequest{Topic: schemaTopic})
	must(t, err)
	if _, bound := p.topicSchemas.Load(canonical(schemaTopic)); bound {
		t.Fatal("topic deletion kept its schema binding")
	}
}

func TestTopicSchemaSettingsRefusals(t *testing.T) {
	h := schemaFixture(t)
	b, as := h.b, h.as
	first := createProto(t, as)
	for _, tc := range []struct {
		name     string
		settings *pb.SchemaSettings
		code     codes.Code
		message  string
	}{
		{"missing schema", &pb.SchemaSettings{Schema: "projects/project-a/schemas/absent", Encoding: pb.Encoding_JSON}, codes.NotFound, "Schema does not exist"},
		{"no encoding", &pb.SchemaSettings{Schema: protoSchema1}, codes.InvalidArgument, "Invalid schema encoding provided"},
		{"unknown first revision", &pb.SchemaSettings{Schema: protoSchema1, Encoding: pb.Encoding_JSON, FirstRevisionId: "ffffffff"}, codes.NotFound, "First revision could not be found"},
		{"unknown last revision", &pb.SchemaSettings{Schema: protoSchema1, Encoding: pb.Encoding_BINARY, FirstRevisionId: first.GetRevisionId(), LastRevisionId: "ffffffff"}, codes.NotFound, "Last revision could not be found"},
		{"revision name", &pb.SchemaSettings{Schema: protoSchema1 + "@" + first.GetRevisionId(), Encoding: pb.Encoding_JSON}, codes.InvalidArgument, "Invalid Pub/Sub resource name"},
		{"cross-project schema", &pb.SchemaSettings{Schema: "projects/project-b/schemas/events", Encoding: pb.Encoding_JSON}, codes.Unimplemented, "Pub/Sub coverage gap: cross-project schemas"},
	} {
		_, err := as(publisher+"CreateTopic", schemaTopic, &pb.Topic{Name: schemaTopic, SchemaSettings: tc.settings})
		expectRefusal(t, tc.name, err, tc.code, tc.message)
	}
	failIf(t, b.called(publisher+"CreateTopic") != 0, "a refused topic reached the emulator", nil)
}

func TestSchemaAuthorizationBeforeTheBackend(t *testing.T) {
	h := schemaFixture(t)
	p, registry, b, as := h.p, h.registry, h.b, h.as
	createProto(t, as)
	before := b.total()
	for _, tc := range []struct {
		method, resource string
		request          proto.Message
	}{
		{"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: "denied", Schema: &pb.Schema{Type: pb.Schema_AVRO, Definition: avroEvent}}},
		{"GetSchema", protoSchema1, &pb.GetSchemaRequest{Name: protoSchema1}},
		{"ListSchemas", projectParent, &pb.ListSchemasRequest{Parent: projectParent}},
		{"DeleteSchema", protoSchema1, &pb.DeleteSchemaRequest{Name: protoSchema1}},
		{"ValidateSchema", projectParent, &pb.ValidateSchemaRequest{Parent: projectParent, Schema: &pb.Schema{Type: pb.Schema_AVRO, Definition: avroEvent}}},
	} {
		_, err := invokeAs(registry, other, schemaService+tc.method, tc.resource, tc.request)
		expectCode(t, err, codes.PermissionDenied)
	}
	failIf(t, b.total() != before, "a denied schema call reached the emulator", nil)
	// A schema-level Viewer grant allows reads but not commits.
	_, err := p.iam.SetPolicy(canonical(protoSchema1), iam.Policy{Bindings: []iam.Binding{{Role: roleViewer, Members: []string{reader}}}})
	must(t, err)
	_, err = invokeAs(registry, reader, schemaService+"GetSchema", protoSchema1, &pb.GetSchemaRequest{Name: protoSchema1})
	must(t, err)
	_, err = invokeAs(registry, reader, schemaService+"CommitSchema", protoSchema1, &pb.CommitSchemaRequest{Name: protoSchema1, Schema: &pb.Schema{Name: protoSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectCode(t, err, codes.PermissionDenied)
	// Without pubsub.schemas.attach, a topic cannot name the schema.
	admin := iam.Binding{Role: roleAdmin, Members: []string{owner}}
	_, err = p.iam.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Version: 3, Bindings: []iam.Binding{admin}, DenyRules: []iam.DenyRule{{DeniedPrincipals: []string{owner}, DeniedPermissions: []string{"pubsub.schemas.attach"}}}})
	must(t, err)
	_, err = as(publisher+"CreateTopic", schemaTopic, &pb.Topic{Name: schemaTopic, SchemaSettings: &pb.SchemaSettings{Schema: protoSchema1, Encoding: pb.Encoding_JSON}})
	expectCode(t, err, codes.PermissionDenied)
	_, err = as(publisher+"CreateTopic", schemaTopic, &pb.Topic{Name: schemaTopic})
	must(t, err)
}

func TestListSchemasMergesBothOwners(t *testing.T) {
	h := schemaFixture(t)
	b, as := h.b, h.as
	createProto(t, as)
	_, err := as(schemaService+"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: "avro-events", Schema: &pb.Schema{Name: "ignored", Type: pb.Schema_AVRO, Definition: avroEvent, RevisionId: "ignored"}})
	must(t, err)
	failIf(t, b.schemas[avroSchema1].GetRevisionId() != "a0000001", "an Avro schema must be created by the emulator", b.schemas)
	var names []string
	token := ""
	for {
		listed, err := as(schemaService+"ListSchemas", projectParent, &pb.ListSchemasRequest{Parent: projectParent, PageSize: 1, PageToken: token})
		must(t, err)
		page := typed[*pb.ListSchemasResponse](t, listed)
		for _, schema := range page.GetSchemas() {
			if schema.GetDefinition() != "" {
				t.Fatal("ListSchemas omits definitions by default", schema)
			}
			names = append(names, schema.GetName())
		}
		if token = page.GetNextPageToken(); token == "" {
			break
		}
	}
	failIf(t, !sort.StringsAreSorted(names) || len(names) != 2, "merged listing", names)
	_, err = as(schemaService+"ListSchemas", projectParent, &pb.ListSchemasRequest{Parent: projectParent, PageToken: "bogus"})
	expectRefusal(t, "page token", err, codes.InvalidArgument, "Invalid page token")
	_, err = as(schemaService+"CommitSchema", avroSchema1, &pb.CommitSchemaRequest{Name: avroSchema1, Schema: &pb.Schema{Name: avroSchema1, Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "Avro to Protocol Buffer", err, codes.FailedPrecondition, "Types do not match")
	_, err = as(schemaService+"DeleteSchema", avroSchema1, &pb.DeleteSchemaRequest{Name: avroSchema1})
	must(t, err)
	_, err = as(schemaService+"GetSchema", avroSchema1, &pb.GetSchemaRequest{Name: avroSchema1})
	expectCode(t, err, codes.NotFound)
}

func TestSchemaStateClearsOnReset(t *testing.T) {
	h := schemaFixture(t)
	p, as := h.p, h.as
	createProto(t, as)
	_, err := as(publisher+"CreateTopic", schemaTopic, &pb.Topic{Name: schemaTopic, SchemaSettings: &pb.SchemaSettings{Schema: protoSchema1, Encoding: pb.Encoding_BINARY}})
	must(t, err)
	must(t, p.afterReset(state.Scope{Project: testProject, Location: state.Global}))
	_, owned := p.protoSchemaOf(protoSchema1)
	_, bound := p.topicSchemas.Load(canonical(schemaTopic))
	failIf(t, owned || bound, "reset kept provider-owned schema state", nil)
}

func TestSchemaFieldsFollowTheReference(t *testing.T) {
	h := schemaFixture(t)
	as := h.as
	unknown := &pb.GetSchemaRequest{Name: protoSchema1}
	unknown.ProtoReflect().SetUnknown([]byte{0xa8, 0x1f, 0x01})
	_, err := as(schemaService+"GetSchema", protoSchema1, unknown)
	expectRefusal(t, "unknown field", err, codes.Unimplemented, "Pub/Sub coverage gap: unknown protobuf fields")
	_, err = as(schemaService+"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: "x", Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoEvent}})
	expectRefusal(t, "short schema ID", err, codes.InvalidArgument, "Invalid Pub/Sub resource name")
	_, err = as(schemaService+"CreateSchema", projectParent, &pb.CreateSchemaRequest{Parent: projectParent, SchemaId: protoSchemaID})
	expectRefusal(t, "missing schema", err, codes.InvalidArgument, "Missing schema")
	_, err = as(schemaService+"ListSchemas", projectParent, &pb.ListSchemasRequest{Parent: projectParent, PageSize: -1})
	expectRefusal(t, "negative page", err, codes.InvalidArgument, "Page size must not be negative")
	_, err = as(schemaService+"RollbackSchema", protoSchema1, &pb.RollbackSchemaRequest{Name: protoSchema1})
	expectRefusal(t, "missing revision", err, codes.InvalidArgument, "Missing revision_id")
}

const restSchemas = "/v1/projects/project-a/schemas"

// TestRESTSchemaLifecycle drives projects.schemas.* and a typed topic over REST.
func TestRESTSchemaLifecycle(t *testing.T) {
	_, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	body := `{"type":"PROTOCOL_BUFFER","definition":` + quoteJSON(protoEvent) + `}`
	created := c.do(owner, http.MethodPost, restSchemas+"?schemaId=events", body, 200)
	failIf(t, created["name"] != protoSchema1 || created["type"] != "PROTOCOL_BUFFER" || created["revisionCreateTime"] == nil, "created", created)
	first := typed[string](t, created["revisionId"])
	committed := c.do(owner, http.MethodPost, restSchemas+"/events:commit", `{"schema":{"name":"`+protoSchema1+`","type":"PROTOCOL_BUFFER","definition":`+quoteJSON(protoEventV2)+`}}`, 200)
	failIf(t, !strings.HasPrefix(typed[string](t, committed["name"]), protoSchema1+"@"), "committed", committed)
	pinned := c.do(owner, http.MethodGet, restSchemas+"/events@"+first+"?view=BASIC", "", 200)
	failIf(t, pinned["name"] != protoSchema1+"@"+first || pinned["definition"] != nil, "pinned", pinned)
	revisions := c.do(owner, http.MethodGet, restSchemas+"/events:listRevisions?pageSize=1", "", 200)
	list, _ := revisions["schemas"].([]any)
	failIf(t, len(list) != 1 || revisions["nextPageToken"] != first, "revisions", revisions)
	c.do(owner, http.MethodGet, restSchemas+"?view=FULL", "", 200)
	message := base64.StdEncoding.EncodeToString([]byte(`{"id":"a"}`))
	c.do(owner, http.MethodPost, restSchemas+":validateMessage", `{"name":"`+protoSchema1+`","message":"`+message+`","encoding":"JSON"}`, 200)
	c.do(owner, http.MethodPost, restSchemas+":validate", `{"schema":{"type":"PROTOCOL_BUFFER","definition":"message {"}}`, http.StatusBadRequest)
	c.do(owner, http.MethodPost, restSchemas+":validate", `{"schema":{"type":"PROTOCOL_BUFFER","definition":"x"},"unknown":1}`, http.StatusBadRequest)
	c.do(other, http.MethodGet, restSchemas+"/events", "", http.StatusForbidden)
	c.do(owner, http.MethodPost, restSchemas+"/events:setIamPolicy", `{"policy":{"bindings":[{"role":"roles/pubsub.viewer","members":["`+reader+`"]}]}}`, 200)
	c.do(reader, http.MethodGet, restSchemas+"/events", "", 200)
	policy := c.do(owner, http.MethodGet, restSchemas+"/events:getIamPolicy", "", 200)
	bindings, _ := policy["bindings"].([]any)
	failIf(t, len(bindings) != 1, "policy", policy)
	topic := c.do(owner, http.MethodPut, "/v1/projects/project-a/topics/typed", `{"schemaSettings":{"schema":"`+protoSchema1+`","encoding":"JSON","firstRevisionId":"`+first+`"}}`, 200)
	settings, _ := topic["schemaSettings"].(map[string]any)
	failIf(t, settings["schema"] != protoSchema1 || settings["firstRevisionId"] != first, "topic", topic)
	publish := "/v1/projects/project-a/topics/typed:publish"
	c.do(owner, http.MethodPost, publish, `{"messages":[{"data":"`+message+`"}]}`, 200)
	invalid := base64.StdEncoding.EncodeToString([]byte(`{"id":5}`))
	c.do(owner, http.MethodPost, publish, `{"messages":[{"data":"`+invalid+`"}]}`, http.StatusBadRequest)
	failIf(t, b.called(publisher+"Publish") != 1, "an invalid REST publish reached the emulator", nil)
	refused := c.do(owner, http.MethodDelete, restSchemas+"/events:deleteRevision?revisionId="+first, "", http.StatusNotImplemented)
	failIf(t, !strings.Contains(fmt.Sprint(refused), "deleteRevision revisionId"), "revisionId refusal", refused)
	c.do(owner, http.MethodDelete, restSchemas+"/events@"+first+":deleteRevision", "", 200)
	c.do(owner, http.MethodDelete, restSchemas+"/events", "", 200)
	c.do(owner, http.MethodGet, restSchemas+"/events", "", http.StatusNotFound)
	failIf(t, b.called(schemaService+"CreateSchema") != 0, "a Protocol Buffer schema reached the emulator over REST", nil)
}

// quoteJSON renders a string as a JSON literal.
func quoteJSON(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + replacer.Replace(value) + `"`
}

// A schema recreated with a deleted schema's name starts with an empty
// policy: removing the schema from the index drops its grants, as GCP does.
func TestRecreatedSchemaStartsWithEmptyPolicy(t *testing.T) {
	h := schemaFixture(t)
	createProto(t, h.as)
	set := &iampb.SetIamPolicyRequest{Resource: protoSchema1, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.viewer", Members: []string{consumer}}}}}
	_, err := h.as(setIAM, protoSchema1, set)
	must(t, err)
	_, err = h.as(schemaService+"DeleteSchema", protoSchema1, &pb.DeleteSchemaRequest{Name: protoSchema1})
	must(t, err)
	createProto(t, h.as)
	raw, err := h.as(getIAM, protoSchema1, &iampb.GetIamPolicyRequest{Resource: protoSchema1})
	must(t, err)
	policy, ok := raw.(*iampb.Policy)
	failIf(t, !ok || len(policy.GetBindings()) != 0, "the recreated schema kept the deleted schema's grants", raw)
}
