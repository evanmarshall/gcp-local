package validation

import (
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SchemaRequest checks every field of a SchemaService request against
// google/pubsub/v1/schema.proto. Each populated field is supported or refused.
func SchemaRequest(project string, request proto.Message) error {
	switch r := request.(type) {
	case *pb.CreateSchemaRequest:
		if err := Fields(r, "parent", "schema", "schema_id"); err != nil {
			return err
		}
		return schemaBody(r.GetSchema(), project, r.GetParent()+"/schemas/"+r.GetSchemaId())
	case *pb.GetSchemaRequest:
		return schemaName(Fields(r, "name", "view"), project, r.GetName(), true)
	case *pb.ListSchemasRequest:
		return pageRequest(Fields(r, "parent", "view", "page_size", "page_token"), r.GetPageSize())
	case *pb.ListSchemaRevisionsRequest:
		return pageRequest(schemaName(Fields(r, "name", "view", "page_size", "page_token"), project, r.GetName(), false), r.GetPageSize())
	case *pb.CommitSchemaRequest:
		return validateCommit(project, r)
	case *pb.RollbackSchemaRequest:
		if err := schemaName(Fields(r, "name", "revision_id"), project, r.GetName(), false); err != nil {
			return err
		}
		return required(r.GetRevisionId(), "revision_id")
	default:
		return validateSchemaOther(project, request)
	}
}

// validateCommit checks a CommitSchema request and the name in its body.
func validateCommit(project string, r *pb.CommitSchemaRequest) error {
	if err := schemaName(Fields(r, "name", "schema"), project, r.GetName(), false); err != nil {
		return err
	}
	if err := schemaBody(r.GetSchema(), project, ""); err != nil {
		return err
	}
	return committedName(r.GetName(), r.GetSchema().GetName())
}

// validateSchemaOther checks the deletion and validation requests.
func validateSchemaOther(project string, request proto.Message) error {
	switch r := request.(type) {
	case *pb.DeleteSchemaRevisionRequest:
		// The reference deprecates revision_id in favor of SCHEMA@REVISION
		// names, so a populated revision_id is refused as a field gap.
		if err := schemaName(Fields(r, "name"), project, r.GetName(), true); err != nil {
			return err
		}
		if !strings.Contains(r.GetName(), "@") {
			return provider.Invalid("Name the revision to delete as SCHEMA@REVISION")
		}
		return nil
	case *pb.DeleteSchemaRequest:
		return schemaName(Fields(r, "name"), project, r.GetName(), false)
	case *pb.ValidateSchemaRequest:
		if err := Fields(r, "parent", "schema"); err != nil {
			return err
		}
		return schemaBody(r.GetSchema(), project, "")
	case *pb.ValidateMessageRequest:
		return validateMessageRequest(project, r)
	}
	return status.Error(codes.Internal, "Unrouted Pub/Sub schema request")
}

// validateMessageRequest checks a ValidateMessage request's schema selector.
func validateMessageRequest(project string, r *pb.ValidateMessageRequest) error {
	if err := Fields(r, "parent", "name", "schema", "message", "encoding"); err != nil {
		return err
	}
	if r.GetSchema() != nil {
		return schemaBody(r.GetSchema(), project, "")
	}
	if r.GetName() == "" {
		return nil
	}
	return schemaName(nil, project, r.GetName(), true)
}

// schemaName validates a schema name in the call's project. Revision suffixes
// (SCHEMA@REVISION) are accepted only where the reference names revisions.
func schemaName(previous error, project, name string, revision bool) error {
	if previous != nil {
		return previous
	}
	base, id, hasRevision := strings.Cut(name, "@")
	if hasRevision && (!revision || id == "" || strings.ContainsAny(id, "/@")) {
		return provider.Invalid("Invalid schema revision name")
	}
	owner, err := ResourceName(base, "schemas")
	if err != nil {
		return err
	}
	if owner != project {
		return gap("cross-project schemas")
	}
	return nil
}

// schemaBody checks a Schema message. The name and the output-only revision
// fields are ignored, as the reference documents for CreateSchema.
func schemaBody(schema *pb.Schema, project, name string) error {
	if schema == nil {
		return provider.Invalid("Missing schema")
	}
	if err := Fields(schema, "name", "type", "definition", "revision_id", "revision_create_time"); err != nil {
		return err
	}
	if name != "" {
		if err := schemaName(nil, project, name, false); err != nil {
			return err
		}
	}
	return required(schema.GetDefinition(), "definition")
}

// committedName checks the schema name a CommitSchema body carries. GCP
// requires it (recorded pubsub-schemas) and answers NOT_FOUND when it names
// another schema; a revision suffix is accepted.
func committedName(name, bodyName string) error {
	if bodyName == "" {
		return provider.Invalid("Invalid resource name given (name=).")
	}
	if base, _, _ := strings.Cut(bodyName, "@"); base != name {
		return status.Error(codes.NotFound, "Resource not found (resource="+name+").")
	}
	return nil
}

// pageRequest bounds a list page size.
func pageRequest(previous error, size int32) error {
	if previous != nil {
		return previous
	}
	if size < 0 {
		return provider.Invalid("Page size must not be negative")
	}
	return nil
}

// required rejects an empty required string field.
func required(value, field string) error {
	if value == "" {
		return provider.Invalid("Missing " + field)
	}
	return nil
}

// validateSchemaSettings checks a topic's schema settings (SchemaSettings
// reference). The schema must exist; the provider checks that when it
// authorizes pubsub.schemas.attach.
func validateSchemaSettings(settings *pb.SchemaSettings) error {
	if settings == nil {
		return nil
	}
	if err := Fields(settings, "schema", "encoding", "first_revision_id", "last_revision_id"); err != nil {
		return err
	}
	if _, err := ResourceName(settings.GetSchema(), "schemas"); err != nil {
		return err
	}
	if settings.GetEncoding() != pb.Encoding_JSON && settings.GetEncoding() != pb.Encoding_BINARY {
		return provider.Invalid("Invalid schema encoding provided")
	}
	return nil
}
