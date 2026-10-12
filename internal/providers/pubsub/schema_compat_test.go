package pubsub

import (
	"context"
	"testing"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
)

// The expected reasons are the ones the GCP sandbox returned on 2026-10-12.
func TestRevisionCompatibilityFollowsGCP(t *testing.T) {
	const record = `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}`
	const proto3 = "syntax = \"proto3\";\nmessage Event {\n  string id = 1;\n  int32 count = 2;\n}\n"
	avro := func(fields string) *pb.Schema {
		return &pb.Schema{Type: pb.Schema_AVRO, Definition: record + fields + "]}", RevisionId: "4a68d64f"}
	}
	protobuf := func(definition string) *pb.Schema {
		return &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: definition, RevisionId: "23787fb1"}
	}
	for _, tc := range []struct {
		name         string
		latest, next *pb.Schema
		code         codes.Code
		reason       string
	}{
		{"avro optional field", avro(""), avro(`,{"name":"note","type":["null","string"],"default":null}`), codes.OK, ""},
		{"avro default without null", avro(""), avro(`,{"name":"d","type":"string","default":""}`), codes.OK, ""},
		{"avro required field", avro(""), avro(`,{"name":"extra","type":"string"}`), codes.FailedPrecondition, "Presence of non-optional field extra is inconsistent."},
		{"avro nullable without default", avro(""), avro(`,{"name":"n","type":["null","string"]}`), codes.FailedPrecondition, "Presence of non-optional field n is inconsistent."},
		{"avro removed field", avro(""), &pb.Schema{Type: pb.Schema_AVRO, Definition: `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`}, codes.FailedPrecondition, "Presence of non-optional field count is inconsistent."},
		{"avro int to long", avro(""), &pb.Schema{Type: pb.Schema_AVRO, Definition: `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"count","type":"long"}]}`}, codes.FailedPrecondition, "Field count has type mismatch: Types do not match."},
		{"avro renamed record", avro(""), &pb.Schema{Type: pb.Schema_AVRO, Definition: `{"type":"record","name":"Other","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`}, codes.FailedPrecondition, "Type name .Event does not match or has no alias for .Other"},
		{"avro nested change", avro(`,{"name":"inner","type":{"type":"record","name":"I","fields":[]},"default":{}}`), avro(`,{"name":"inner","type":{"type":"record","name":"J","fields":[]},"default":{}}`), codes.Unimplemented, "Pub/Sub coverage gap: compatibility of changed nested Avro types"},
		{"type change", avro(""), protobuf(proto3), codes.FailedPrecondition, "Failed with error: Types do not match)"},
		{"proto removed field", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Event {\n  string id = 1;\n}\n"), codes.OK, ""},
		{"proto renamed message", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Other {\n  string id = 1;\n  int32 count = 2;\n}\n"), codes.OK, ""},
		{"proto field type", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Event {\n  string id = 1;\n  int64 count = 2;\n}\n"), codes.FailedPrecondition, "Event.count of type int64 is not compatible with Event.count of type int32"},
		{"proto repeated", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Event {\n  repeated string id = 1;\n  int32 count = 2;\n}\n"), codes.FailedPrecondition, "Event.id is repeated but Event.id is not"},
		{"proto renumbered", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Event {\n  string id = 3;\n  int32 count = 2;\n}\n"), codes.FailedPrecondition, "Event.id with tag 3 has the same name as Event.id with tag 1"},
		{"proto renamed field", protobuf(proto3), protobuf("syntax = \"proto3\";\nmessage Event {\n  string ident = 1;\n  int32 count = 2;\n}\n"), codes.FailedPrecondition, "Event.ident has the same tag (1) but different name than Event.id"},
		{"proto required", protobuf(proto3), protobuf("syntax = \"proto2\";\nmessage Event {\n  optional string id = 1;\n  required int32 count = 2;\n}\n"), codes.FailedPrecondition, "Event.count is a required field but Event.count is not"},
		{"proto required made optional", protobuf("syntax = \"proto2\";\nmessage Event {\n  required int32 count = 2;\n}\n"), protobuf("syntax = \"proto2\";\nmessage Event {\n  optional int32 count = 2;\n}\n"), codes.Unimplemented, "Pub/Sub coverage gap: compatibility of a required Protocol Buffer field made optional"},
	} {
		err := checkCompatible(context.Background(), protoSchema1, tc.latest, tc.next)
		if tc.code == codes.OK {
			must(t, err)
			continue
		}
		expectRefusal(t, tc.name, err, tc.code, tc.reason)
	}
}
