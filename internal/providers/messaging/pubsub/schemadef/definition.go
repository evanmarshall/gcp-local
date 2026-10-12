// Package schemadef compiles Pub/Sub schema definitions, checks messages
// against them, and checks revision compatibility. The rules follow Google's
// schema guide (https://cloud.google.com/pubsub/docs/schemas) and the
// pubsub-schemas recording; the official emulator implements none of them for
// Protocol Buffer schemas.
package schemadef

import (
	"context"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/bufbuild/protocompile"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// MaxDefinitionBytes bounds one schema definition (300 KB).
const MaxDefinitionBytes = 300 << 10

// schemaFile names the single in-memory file a definition compiles as.
const schemaFile = "schema.proto"

var gap = provider.CoverageGap("Pub/Sub")

// Compile parses one definition into its single top-level message.
func Compile(ctx context.Context, definition string) (protoreflect.MessageDescriptor, error) {
	if len(definition) > MaxDefinitionBytes {
		return nil, provider.Invalid("Schema definition must be at most 300 KB")
	}
	// Only the definition itself resolves, so any import fails to compile.
	// Editions files are rejected by the compiler's default settings.
	compiler := protocompile.Compiler{Resolver: &protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(map[string]string{schemaFile: definition})}}
	files, err := compiler.Compile(ctx, schemaFile)
	if err != nil || len(files) != 1 {
		return nil, provider.Invalid("Could not parse schema definition")
	}
	file := files[0]
	if file.Messages().Len() != 1 || file.Enums().Len() != 0 || file.Extensions().Len() != 0 || file.Services().Len() != 0 {
		return nil, provider.Invalid("A Protocol Buffer schema must define exactly one top-level message")
	}
	return file.Messages().Get(0), nil
}

// Conforms reports whether data decodes as the message in the encoding:
// protobuf binary for BINARY and Google's JSON mapping for JSON.
func Conforms(message protoreflect.MessageDescriptor, data []byte, encoding pb.Encoding) bool {
	decoded := dynamicpb.NewMessage(message)
	switch encoding {
	case pb.Encoding_JSON:
		return protojson.Unmarshal(data, decoded) == nil
	case pb.Encoding_BINARY:
		return proto.Unmarshal(data, decoded) == nil
	default:
		return false
	}
}
