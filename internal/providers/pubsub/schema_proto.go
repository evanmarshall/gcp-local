package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/bufbuild/protocompile"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Protocol Buffer schemas are provider-owned: the official emulator answers
// every PROTOCOL_BUFFER request with UNIMPLEMENTED ("Protocol buffer support
// not implemented in emulator"). Avro schemas stay with the emulator. The
// limits and rules below follow Google's schema guide
// (https://cloud.google.com/pubsub/docs/schemas): proto2 or proto3, one
// top-level type, no imports, 20 revisions, and a 300 KB definition.
const (
	maxSchemaRevisions  = 20
	maxDefinitionBytes  = 300 << 10
	deletedSchema       = "_deleted-schema_"
	deletedRevision     = "_deleted-schema-revision_"
	schemaFile          = "schema.proto"
	errorSchemaNotFound = "Schema not found"
)

// protoRevision is one immutable revision and its compiled message type.
type protoRevision struct {
	schema  *pb.Schema
	message protoreflect.MessageDescriptor
}

// protoSchema holds a schema's revisions, oldest first, as the emulator lists
// Avro revisions. It is replaced, never mutated, so readers need no lock.
type protoSchema struct {
	revisions []protoRevision
}

// topicSchema binds a topic to a provider-owned schema. The revision range is
// resolved to creation times when the topic is created, so a later revision
// deletion does not move the range.
type topicSchema struct {
	settings *pb.SchemaSettings
	from, to time.Time // to is zero when the range is open-ended
}

// compileProtoSchema parses one definition into its single top-level message.
func compileProtoSchema(ctx context.Context, definition string) (protoreflect.MessageDescriptor, error) {
	if len(definition) > maxDefinitionBytes {
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

// conforms reports whether data decodes as the message in the encoding:
// protobuf binary for BINARY and Google's JSON mapping for JSON.
func conforms(message protoreflect.MessageDescriptor, data []byte, encoding pb.Encoding) bool {
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

// newProtoRevision compiles a definition into a new revision of name.
func newProtoRevision(ctx context.Context, name, definition string) (protoRevision, error) {
	message, err := compileProtoSchema(ctx, definition)
	if err != nil {
		return protoRevision{}, err
	}
	id := make([]byte, 4)
	if _, err := rand.Read(id); err != nil {
		return protoRevision{}, fmt.Errorf("generate a schema revision ID: %w", err)
	}
	schema := &pb.Schema{Name: name, Type: pb.Schema_PROTOCOL_BUFFER, Definition: definition, RevisionId: hex.EncodeToString(id), RevisionCreateTime: timestamppb.Now()}
	return protoRevision{schema: schema, message: message}, nil
}

// protoSchemaOf returns the provider-owned schema with this relative name.
func (p *Provider) protoSchemaOf(name string) (*protoSchema, bool) {
	value, ok := p.schemas.Load(canonical(name))
	if !ok {
		return nil, false
	}
	schema, ok := value.(*protoSchema)
	return schema, ok
}

// revision finds one revision by ID; an empty ID selects the latest.
func (s *protoSchema) revision(id string) (protoRevision, bool) {
	if id == "" {
		return s.revisions[len(s.revisions)-1], true
	}
	for _, revision := range s.revisions {
		if revision.schema.GetRevisionId() == id {
			return revision, true
		}
	}
	return protoRevision{}, false
}

// appendRevision stores a new latest revision, within the revision limit.
func (p *Provider) appendRevision(name string, current *protoSchema, revision protoRevision) (*pb.Schema, error) {
	if len(current.revisions) >= maxSchemaRevisions {
		return nil, status.Error(codes.ResourceExhausted, "Already at maximum number of schema revisions")
	}
	p.schemas.Store(canonical(name), &protoSchema{revisions: append(slices.Clone(current.revisions), revision)})
	return proto.CloneOf(revision.schema), nil
}

// commitProtoSchema adds a revision with a new definition.
func (p *Provider) commitProtoSchema(ctx context.Context, name string, current *protoSchema, schema *pb.Schema) (*pb.Schema, error) {
	revision, err := newProtoRevision(ctx, name, schema.GetDefinition())
	if err != nil {
		return nil, err
	}
	return p.appendRevision(name, current, revision)
}

// rollbackProtoSchema adds a revision that copies an earlier one.
func (p *Provider) rollbackProtoSchema(ctx context.Context, name string, current *protoSchema, id string) (*pb.Schema, error) {
	source, found := current.revision(id)
	if !found {
		return nil, status.Error(codes.NotFound, "Revision not found")
	}
	revision, err := newProtoRevision(ctx, name, source.schema.GetDefinition())
	if err != nil {
		return nil, err
	}
	return p.appendRevision(name, current, revision)
}

// deleteProtoRevision removes one revision and returns it; the last revision
// can only go with DeleteSchema.
func (p *Provider) deleteProtoRevision(name string, current *protoSchema, id string) (*pb.Schema, error) {
	index := slices.IndexFunc(current.revisions, func(r protoRevision) bool { return r.schema.GetRevisionId() == id })
	if index < 0 {
		return nil, status.Error(codes.NotFound, "Schema revision not found")
	}
	if len(current.revisions) == 1 {
		return nil, provider.Invalid("Cannot delete last revision. Please use DeleteSchema.")
	}
	deleted := current.revisions[index].schema
	p.schemas.Store(canonical(name), &protoSchema{revisions: slices.Delete(slices.Clone(current.revisions), index, index+1)})
	return proto.CloneOf(deleted), nil
}

// deleteProtoSchema drops a schema and marks the topics bound to it, as the
// emulator does for Avro: their settings name _deleted-schema_ and publishing
// fails.
func (p *Provider) deleteProtoSchema(name string) {
	p.schemas.Delete(canonical(name))
	p.topicSchemas.Range(func(key, value any) bool {
		binding, ok := value.(*topicSchema)
		if !ok || binding.settings.GetSchema() != name {
			return true
		}
		settings := proto.CloneOf(binding.settings)
		settings.Schema = deletedSchema
		if settings.GetFirstRevisionId() != "" {
			settings.FirstRevisionId = deletedRevision
		}
		if settings.GetLastRevisionId() != "" {
			settings.LastRevisionId = deletedRevision
		}
		p.topicSchemas.Store(key, &topicSchema{settings: settings})
		return true
	})
}

// bindTopicSchema resolves a topic's revision range against a provider-owned
// schema. It reports false when the schema belongs to the emulator.
func (p *Provider) bindTopicSchema(settings *pb.SchemaSettings) (*topicSchema, bool, error) {
	schema, owned := p.protoSchemaOf(settings.GetSchema())
	if !owned {
		return nil, false, nil
	}
	binding := &topicSchema{settings: proto.CloneOf(settings)}
	for _, bound := range []struct {
		id, label string
		at        *time.Time
	}{{settings.GetFirstRevisionId(), "First", &binding.from}, {settings.GetLastRevisionId(), "Last", &binding.to}} {
		if bound.id == "" {
			continue
		}
		revision, found := schema.revision(bound.id)
		if !found {
			return nil, true, status.Error(codes.NotFound, bound.label+" revision could not be found")
		}
		*bound.at = revision.schema.GetRevisionCreateTime().AsTime()
	}
	return binding, true, nil
}

// withoutProtoSchema strips the schema settings from a forwarded topic that
// binds a provider-owned schema, after checking its revision range. The
// emulator would answer "Schema could not be found".
func (p *Provider) withoutProtoSchema(request proto.Message) (proto.Message, error) {
	topic, ok := request.(*pb.Topic)
	if !ok || topic.GetSchemaSettings() == nil {
		return request, nil
	}
	_, owned, err := p.bindTopicSchema(topic.GetSchemaSettings())
	if err != nil || !owned {
		return request, err
	}
	forwarded := proto.CloneOf(topic)
	forwarded.SchemaSettings = nil
	return forwarded, nil
}

// storeTopicSchema records a created topic's binding to a provider-owned schema.
func (p *Provider) storeTopicSchema(topic *pb.Topic) {
	if topic.GetSchemaSettings() == nil {
		return
	}
	if binding, owned, err := p.bindTopicSchema(topic.GetSchemaSettings()); owned && err == nil {
		p.topicSchemas.Store(canonical(topic.GetName()), binding)
	}
}

// conformingRevision returns the newest revision in the topic's range that
// accepts the data (publishing guide: newest allowed revision first).
func (b *topicSchema) conformingRevision(schema *protoSchema, data []byte) (*pb.Schema, bool) {
	for _, revision := range slices.Backward(schema.revisions) {
		created := revision.schema.GetRevisionCreateTime().AsTime()
		if created.Before(b.from) || (!b.to.IsZero() && created.After(b.to)) {
			continue
		}
		if conforms(revision.message, data, b.settings.GetEncoding()) {
			return revision.schema, true
		}
	}
	return nil, false
}

// validateSchemaPublish checks every message published to a topic bound to a
// provider-owned schema and adds the googclient_schema* attributes Google
// documents. Topics without such a binding pass through unchanged.
func (p *Provider) validateSchemaPublish(request *pb.PublishRequest) (*pb.PublishRequest, error) {
	value, bound := p.topicSchemas.Load(canonical(request.GetTopic()))
	if !bound {
		return request, nil
	}
	binding, ok := value.(*topicSchema)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid topic schema binding")
	}
	schema, found := p.protoSchemaOf(binding.settings.GetSchema())
	if !found {
		return nil, status.Error(codes.NotFound, "Schema associated with topic deleted.")
	}
	forwarded := proto.CloneOf(request)
	for _, message := range forwarded.GetMessages() {
		revision, ok := binding.conformingRevision(schema, message.GetData())
		if !ok {
			return nil, provider.Invalid("Could not parse message")
		}
		if message.Attributes == nil {
			message.Attributes = map[string]string{}
		}
		message.Attributes["googclient_schemaname"] = revision.GetName()
		message.Attributes["googclient_schemaencoding"] = binding.settings.GetEncoding().String()
		message.Attributes["googclient_schemarevisionid"] = revision.GetRevisionId()
	}
	return forwarded, nil
}

// decorateTopic restores the schema settings of a topic bound to a
// provider-owned schema; the emulator holds the topic without them.
func (p *Provider) decorateTopic(topic *pb.Topic) {
	if topic == nil {
		return
	}
	if value, ok := p.topicSchemas.Load(canonical(topic.GetName())); ok {
		if binding, ok := value.(*topicSchema); ok {
			topic.SchemaSettings = proto.CloneOf(binding.settings)
		}
	}
}
