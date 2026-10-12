package pubsub

import (
	"context"
	"fmt"
	"slices"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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
	if err := validateSchemaRequest(call.Project, request); err != nil {
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
			_, err := compileProtoSchema(call.Context, r.GetSchema().GetDefinition())
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
		if err := checkCompatible(call.Context, call.Resource, latest, commit.GetSchema()); err != nil {
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
	message, err := compileProtoSchema(ctx, definition)
	if err != nil {
		return nil, true, err
	}
	if !conforms(message, r.GetMessage(), r.GetEncoding()) {
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
