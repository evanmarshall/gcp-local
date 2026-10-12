package pubsub

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The schema REST methods (projects.schemas.*) decode into the gRPC request,
// run through handleSchema or the policy handler, and encode the response with
// Google's JSON mapping, which is the REST wire shape.
const restSchemaPrefix = "pubsub.projects.schemas."

// registerRestSchemas binds each schema REST route and its twin's permission.
func (p *Provider) registerRestSchemas(registry *gateway.Registry) error {
	for _, route := range generated.PubSubSchemaRestRoutes {
		twin, permission := restSchemaTwin(route.Method)
		if twin == "" {
			return fmt.Errorf("%w: %s", errRestUnknownTwin, route.Method)
		}
		if err := p.iam.Register(route.Method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", route.Method, err)
		}
		if err := registry.Register(route.Method, p.handleRestSchema); err != nil {
			return fmt.Errorf("register %s: %w", route.Method, err)
		}
	}
	return nil
}

// restSchemaTwin returns the gRPC method a REST schema method runs and the
// permission it evaluates.
func restSchemaTwin(method string) (string, string) {
	twin := map[string]string{
		"create": "CreateSchema", "list": "ListSchemas", "validate": "ValidateSchema", "validateMessage": "ValidateMessage",
		"get": "GetSchema", "delete": "DeleteSchema", "listRevisions": "ListSchemaRevisions", "commit": "CommitSchema",
		"rollback": "RollbackSchema", "deleteRevision": "DeleteSchemaRevision", "getIamPolicy": "", "setIamPolicy": "",
	}
	verb := strings.TrimPrefix(method, restSchemaPrefix)
	rpc, found := twin[verb]
	switch {
	case !found:
		return "", ""
	case rpc == "":
		// The policy RPC's permission is per resource kind (pubsub.schemas.*).
		return policyRPC + strings.ToUpper(verb[:1]) + verb[1:], "pubsub.schemas." + verb
	}
	return schemaService + rpc, permissions[schemaService+rpc]
}

// handleRestSchema serves one schema REST call through its gRPC twin.
func (p *Provider) handleRestSchema(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub REST request")
	}
	twin, _ := restSchemaTwin(call.Method)
	call.Project = request.Path["project"]
	parent := "projects/" + call.Project
	call.Resource = parent
	if schema := request.Path["schema"]; schema != "" {
		call.Resource = parent + "/schemas/" + schema
	}
	call.Method = twin
	message, err := restSchemaProto(twin, call.Resource, request)
	if err != nil {
		return nil, err
	}
	var result any
	if strings.HasPrefix(twin, policyRPC) {
		result, err = p.policy(call, message)
	} else {
		result, err = p.handleSchema(call, message)
	}
	if err != nil {
		return nil, err
	}
	response, ok := result.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub schema response")
	}
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the schema response")
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded)}, nil
}

// restSchemaProto builds the gRPC request: the body (when the method takes
// one) with Google's JSON mapping, the path resource in the contract's
// resource field, then the documented query parameters.
func restSchemaProto(twin, resource string, request *gateway.RESTRequest) (proto.Message, error) {
	var rpc generated.RPC
	for _, candidate := range append(append([]generated.RPC(nil), generated.PubSubMethods...), generated.IAMMethods...) {
		if candidate.Method == twin {
			rpc = candidate
		}
	}
	if rpc.NewRequest == nil {
		return nil, fmt.Errorf("%w: %s", errRestNoProtoMapping, twin)
	}
	message := rpc.NewRequest()
	target := message
	if create, ok := message.(*pb.CreateSchemaRequest); ok {
		create.Schema = &pb.Schema{}
		target = create.GetSchema() // The create body is the Schema itself.
	}
	if err := restBody(request, target); err != nil {
		return nil, err
	}
	reflected := message.ProtoReflect()
	reflected.Set(reflected.Descriptor().Fields().ByName(protoreflect.Name(rpc.ResourceField)), protoreflect.ValueOfString(resource))
	if request.Query.Get("revisionId") != "" {
		return nil, gap("deleteRevision revisionId; name the revision as SCHEMA@REVISION")
	}
	if version := request.Query.Get("options.requestedPolicyVersion"); version != "" {
		get, ok := message.(*iampb.GetIamPolicyRequest)
		number, err := strconv.ParseInt(version, 10, 32)
		if !ok || err != nil {
			return nil, provider.Invalid("Invalid options.requestedPolicyVersion")
		}
		get.Options = &iampb.GetPolicyOptions{RequestedPolicyVersion: int32(number)}
	}
	return message, restQuery(reflected, request.Query)
}

// restQuery sets the schema methods' query parameters (schemaId, view,
// pageSize, pageToken) on the request fields of the same name.
func restQuery(message protoreflect.Message, query url.Values) error {
	for parameter, name := range map[string]protoreflect.Name{"schemaId": "schema_id", "view": "view", "pageSize": "page_size", "pageToken": "page_token"} {
		value := query.Get(parameter)
		field := message.Descriptor().Fields().ByName(name)
		if value == "" || field == nil {
			continue
		}
		switch field.Kind() {
		case protoreflect.EnumKind:
			number := field.Enum().Values().ByName(protoreflect.Name(value))
			if number == nil || number.Number() == 0 {
				return provider.Invalid("Invalid " + parameter)
			}
			message.Set(field, protoreflect.ValueOfEnum(number.Number()))
		case protoreflect.Int32Kind:
			size, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return provider.Invalid("Invalid " + parameter)
			}
			message.Set(field, protoreflect.ValueOfInt32(int32(size)))
		default:
			message.Set(field, protoreflect.ValueOfString(value))
		}
	}
	return nil
}
