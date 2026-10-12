package pubsub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/messaging/pubsub/schemadef"
	"github.com/evanmarshall/gcp-local/internal/providers/messaging/pubsub/validation"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SchemaService: Avro schemas are forwarded to the official emulator, which
// implements them; Protocol Buffer schemas are served by the provider
// (schema_proto.go). Both are indexed, so IAM targets the schema resource.

// handleSchema authorizes a SchemaService call, then runs it inside the
// backend gate against the schema's owner.
func (p *Provider) handleSchema(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub schema request")
	}
	if err := validation.SchemaRequest(call.Project, request); err != nil {
		return nil, err
	}
	// Revision names (SCHEMA@REVISION) authorize against the schema.
	call.Resource, _, _ = strings.Cut(call.Resource, "@")
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	var response proto.Message
	err := p.manager.Use(call.Context, keyOf(call), func(session context.Context, active backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		if err := p.authorize(call, request); err != nil {
			return err
		}
		b, err := driverOf(active)
		if err != nil {
			return err
		}
		response, err = p.executeSchema(call, b, request)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("run the pub/sub schema session: %w", err)
	}
	return response, nil
}

// executeSchema runs one authorized call against the provider store or the
// emulator.
func (p *Provider) executeSchema(call gateway.Call, b driver, request proto.Message) (proto.Message, error) {
	switch r := request.(type) {
	case *pb.CreateSchemaRequest:
		return p.createSchema(call, b, r)
	case *pb.ListSchemasRequest:
		return p.listSchemas(call.Context, b, r)
	case *pb.ValidateSchemaRequest:
		if r.GetSchema().GetType() == pb.Schema_PROTOCOL_BUFFER {
			_, err := schemadef.Compile(call.Context, r.GetSchema().GetDefinition())
			return &pb.ValidateSchemaResponse{}, err
		}
	case *pb.ValidateMessageRequest:
		if response, owned, err := p.validateProtoMessage(call.Context, r); owned {
			return response, err
		}
	}
	if projectMethod(call.Method) {
		return p.forwardSchema(call, b, request)
	}
	return p.schemaCall(call, b, request)
}

// schemaCall serves a name-based call through the schema's owner, then
// shapes the response as GCP returns it.
func (p *Provider) schemaCall(call gateway.Call, b driver, request proto.Message) (proto.Message, error) {
	if list, ok := request.(*pb.ListSchemaRevisionsRequest); ok {
		return p.listRevisions(call, b, list)
	}
	schema, owned := p.protoSchemaOf(call.Resource)
	if commit, ok := request.(*pb.CommitSchemaRequest); ok {
		latest, err := p.latestSchema(call, b, schema, owned)
		if err != nil {
			return nil, err
		}
		if err := schemadef.CheckCompatible(call.Context, call.Resource, latest, commit.GetSchema()); err != nil {
			return nil, err
		}
	}
	var response proto.Message
	var err error
	if owned {
		response, err = p.protoSchemaCall(call, schema, request)
	} else {
		response, err = p.forwardSchema(call, b, request)
	}
	if err != nil {
		return nil, err
	}
	return p.presentSchema(call, request, response), nil
}

// latestSchema returns the newest revision of a schema from its owner.
func (p *Provider) latestSchema(call gateway.Call, b driver, schema *protoSchema, owned bool) (*pb.Schema, error) {
	if owned {
		return schema.revisions[len(schema.revisions)-1].schema, nil
	}
	response, err := invoke(call.Context, b, schemaService+"GetSchema", &pb.GetSchemaRequest{Name: call.Resource, View: pb.SchemaView_FULL})
	if err != nil {
		return nil, err
	}
	latest, ok := response.(*pb.Schema)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid emulator schema")
	}
	return latest, nil
}

// presentSchema names revisions as GCP does: SCHEMA@REVISION for commits,
// rollbacks, and revision reads, and the project number with no definition
// for a deleted revision.
func (p *Provider) presentSchema(call gateway.Call, request, response proto.Message) proto.Message {
	schema, ok := response.(*pb.Schema)
	if !ok {
		return response
	}
	switch r := request.(type) {
	case *pb.CommitSchemaRequest, *pb.RollbackSchemaRequest:
		schema.Name = call.Resource + "@" + schema.GetRevisionId()
	case *pb.GetSchemaRequest:
		if strings.Contains(r.GetName(), "@") {
			schema.Name = call.Resource + "@" + schema.GetRevisionId()
		}
	case *pb.DeleteSchemaRevisionRequest:
		number := call.Project
		if project, found := p.index.Get(provider.ProjectResource(call.Project)); found && project.Number != "" {
			number = project.Number
		}
		_, id, _ := strings.Cut(call.Resource, "/schemas/")
		schema.Name = "projects/" + number + "/schemas/" + id + "@" + schema.GetRevisionId()
		schema.Definition = ""
	}
	return schema
}

// listRevisions lists every revision from the schema's owner, newest first
// with SCHEMA@REVISION names, and pages the result.
func (p *Provider) listRevisions(call gateway.Call, b driver, r *pb.ListSchemaRevisionsRequest) (proto.Message, error) {
	all, err := p.revisionsOf(call, b)
	if err != nil {
		return nil, err
	}
	newest := make([]*pb.Schema, 0, len(all))
	for _, stored := range slices.Backward(all) {
		revision := proto.CloneOf(stored)
		revision.Name = call.Resource + "@" + revision.GetRevisionId()
		newest = append(newest, revision)
	}
	page, next, err := pageSchemas(newest, r.GetPageSize(), r.GetPageToken(), (*pb.Schema).GetRevisionId)
	return &pb.ListSchemaRevisionsResponse{Schemas: viewSchemas(page, r.GetView()), NextPageToken: next}, err
}

// createSchema creates the schema with its owner and indexes it.
func (p *Provider) createSchema(call gateway.Call, b driver, r *pb.CreateSchemaRequest) (proto.Message, error) {
	call.Resource = r.GetParent() + "/schemas/" + r.GetSchemaId()
	if _, found := p.index.Get(canonical(call.Resource)); found {
		return nil, status.Error(codes.AlreadyExists, "Schema already exists")
	}
	if r.GetSchema().GetType() != pb.Schema_PROTOCOL_BUFFER {
		forwarded := proto.CloneOf(r)
		// The reference ignores the schema's name and its output-only fields.
		forwarded.Schema = &pb.Schema{Type: r.GetSchema().GetType(), Definition: r.GetSchema().GetDefinition()}
		response, err := invoke(call.Context, b, call.Method, forwarded)
		return response, p.settleMutation(call.Context, b, call, response, r, nil, true, err)
	}
	revision, err := newProtoRevision(call.Context, call.Resource, r.GetSchema().GetDefinition())
	if err != nil {
		return nil, err
	}
	if err := p.publish(call); err != nil {
		return nil, err
	}
	p.schemas.Store(canonical(call.Resource), &protoSchema{revisions: []protoRevision{revision}})
	return proto.CloneOf(revision.schema), nil
}

// revisionsOf returns every revision of a schema, oldest first, from its owner.
func (p *Provider) revisionsOf(call gateway.Call, b driver) ([]*pb.Schema, error) {
	var all []*pb.Schema
	if schema, owned := p.protoSchemaOf(call.Resource); owned {
		for _, revision := range schema.revisions {
			all = append(all, revision.schema)
		}
		return all, nil
	}
	request := &pb.ListSchemaRevisionsRequest{Name: call.Resource, View: pb.SchemaView_FULL, PageSize: 100}
	for {
		response, err := invoke(call.Context, b, schemaService+"ListSchemaRevisions", request)
		if err != nil {
			return nil, err
		}
		page, ok := response.(*pb.ListSchemaRevisionsResponse)
		if !ok {
			return nil, status.Error(codes.Internal, "Invalid emulator revision list")
		}
		all = append(all, page.GetSchemas()...)
		if request.PageToken = page.GetNextPageToken(); request.GetPageToken() == "" {
			return all, nil
		}
	}
}

// forwardSchema invokes an Avro schema call on the emulator and settles a
// deletion in the index.
func (p *Provider) forwardSchema(call gateway.Call, b driver, request proto.Message) (proto.Message, error) {
	if commit, ok := request.(*pb.CommitSchemaRequest); ok {
		// The emulator needs only the definition; the name was checked already.
		forwarded := proto.CloneOf(commit)
		forwarded.Schema = &pb.Schema{Type: commit.GetSchema().GetType(), Definition: commit.GetSchema().GetDefinition()}
		request = forwarded
	}
	response, err := invoke(call.Context, b, call.Method, request)
	if call.Method == schemaService+"DeleteSchema" {
		return response, p.settleMutation(call.Context, b, call, response, request, nil, false, err)
	}
	return response, err
}

// protoSchemaCall serves a name-based call on a provider-owned schema.
func (p *Provider) protoSchemaCall(call gateway.Call, schema *protoSchema, request proto.Message) (proto.Message, error) {
	switch r := request.(type) {
	case *pb.GetSchemaRequest:
		_, id, _ := strings.Cut(r.GetName(), "@")
		revision, found := schema.revision(id)
		if !found {
			return nil, status.Error(codes.NotFound, "Schema revision not found")
		}
		return viewSchema(revision.schema, r.GetView(), true), nil
	case *pb.CommitSchemaRequest:
		return p.commitProtoSchema(call.Context, call.Resource, schema, r.GetSchema())
	case *pb.RollbackSchemaRequest:
		return p.rollbackProtoSchema(call.Context, call.Resource, schema, r.GetRevisionId())
	case *pb.DeleteSchemaRevisionRequest:
		_, id, _ := strings.Cut(r.GetName(), "@")
		return p.deleteProtoRevision(call.Resource, schema, id)
	case *pb.DeleteSchemaRequest:
		if err := p.forget(call); err != nil {
			return nil, err
		}
		p.deleteProtoSchema(call.Resource)
		return &emptypb.Empty{}, nil
	}
	return nil, status.Error(codes.Internal, "Unrouted Pub/Sub schema method")
}

// validateProtoMessage validates a message against an inline or named
// Protocol Buffer schema. It reports false when the emulator owns the call.
func (p *Provider) validateProtoMessage(ctx context.Context, r *pb.ValidateMessageRequest) (proto.Message, bool, error) {
	definition := r.GetSchema().GetDefinition()
	if inline := r.GetSchema(); inline == nil || inline.GetType() != pb.Schema_PROTOCOL_BUFFER {
		base, id, _ := strings.Cut(r.GetName(), "@")
		schema, owned := p.protoSchemaOf(base)
		if inline != nil || !owned {
			return nil, false, nil
		}
		revision, found := schema.revision(id)
		if !found {
			return nil, true, status.Error(codes.NotFound, "Revision not found")
		}
		definition = revision.schema.GetDefinition()
	}
	if r.GetEncoding() == pb.Encoding_ENCODING_UNSPECIFIED {
		return nil, true, provider.Invalid("No encoding provided.")
	}
	message, err := schemadef.Compile(ctx, definition)
	if err != nil {
		return nil, true, err
	}
	if !schemadef.Conforms(message, r.GetMessage(), r.GetEncoding()) {
		return nil, true, provider.Invalid("Could not parse message")
	}
	return &pb.ValidateMessageResponse{}, true, nil
}

// listSchemas merges the emulator's Avro schemas with the provider's Protocol
// Buffer schemas in name order and pages the result.
func (p *Provider) listSchemas(ctx context.Context, b driver, r *pb.ListSchemasRequest) (proto.Message, error) {
	var all []*pb.Schema
	request := &pb.ListSchemasRequest{Parent: r.GetParent(), View: pb.SchemaView_FULL, PageSize: 1000}
	for {
		response, err := invoke(ctx, b, schemaService+"ListSchemas", request)
		if err != nil {
			return nil, err
		}
		page, ok := response.(*pb.ListSchemasResponse)
		if !ok {
			return nil, status.Error(codes.Internal, "Invalid emulator schema list")
		}
		all = append(all, page.GetSchemas()...)
		if request.PageToken = page.GetNextPageToken(); request.GetPageToken() == "" {
			break
		}
	}
	prefix := canonical(r.GetParent() + "/schemas/")
	p.schemas.Range(func(key, value any) bool {
		name, _ := key.(string)
		if schema, ok := value.(*protoSchema); ok && strings.HasPrefix(name, prefix) {
			all = append(all, schema.revisions[len(schema.revisions)-1].schema)
		}
		return true
	})
	slices.SortFunc(all, func(a, b *pb.Schema) int { return strings.Compare(a.GetName(), b.GetName()) })
	page, next, err := pageSchemas(all, r.GetPageSize(), r.GetPageToken(), (*pb.Schema).GetName)
	return &pb.ListSchemasResponse{Schemas: viewSchemas(page, r.GetView()), NextPageToken: next}, err
}

// pageSchemas returns one page. A token is the key of the page's first item.
func pageSchemas(all []*pb.Schema, size int32, token string, key func(*pb.Schema) string) ([]*pb.Schema, string, error) {
	start := 0
	if token != "" {
		if start = slices.IndexFunc(all, func(s *pb.Schema) bool { return key(s) == token }); start < 0 {
			return nil, "", provider.Invalid("Invalid page token")
		}
	}
	end := len(all)
	if size > 0 && start+int(size) < end {
		end = start + int(size)
	}
	next := ""
	if end < len(all) {
		next = key(all[end])
	}
	return all[start:end], next, nil
}

// viewSchemas applies a list view: the reference omits the definition unless
// the view is FULL.
func viewSchemas(schemas []*pb.Schema, view pb.SchemaView) []*pb.Schema {
	viewed := make([]*pb.Schema, 0, len(schemas))
	for _, schema := range schemas {
		viewed = append(viewed, viewSchema(schema, view, false))
	}
	return viewed
}

// viewSchema copies a schema, omitting its definition for BASIC, or for an
// unset view when the method's default is BASIC.
func viewSchema(schema *pb.Schema, view pb.SchemaView, fullByDefault bool) *pb.Schema {
	viewed := proto.CloneOf(schema)
	if view == pb.SchemaView_BASIC || (view == pb.SchemaView_SCHEMA_VIEW_UNSPECIFIED && !fullByDefault) {
		viewed.Definition = ""
	}
	return viewed
}

// Protocol Buffer schemas are provider-owned: the official emulator answers
// every PROTOCOL_BUFFER request with UNIMPLEMENTED ("Protocol buffer support
// not implemented in emulator"). Avro schemas stay with the emulator. The
// limits and rules below follow Google's schema guide
// (https://cloud.google.com/pubsub/docs/schemas): proto2 or proto3, one
// top-level type, no imports, 20 revisions, and a 300 KB definition.
const (
	maxSchemaRevisions  = 20
	deletedSchema       = "_deleted-schema_"
	deletedRevision     = "_deleted-schema-revision_"
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

// newProtoRevision compiles a definition into a new revision of name.
func newProtoRevision(ctx context.Context, name, definition string) (protoRevision, error) {
	message, err := schemadef.Compile(ctx, definition)
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
		if schemadef.Conforms(revision.message, data, b.settings.GetEncoding()) {
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
