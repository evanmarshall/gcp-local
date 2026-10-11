package spanner

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Emulator gRPC services.
const (
	instanceAdmin          = "/google.spanner.admin.instance.v1.InstanceAdmin/"
	databaseAdmin          = "/google.spanner.admin.database.v1.DatabaseAdmin/"
	rpcListInstanceConfigs = instanceAdmin + "ListInstanceConfigs"
	rpcGetOperation        = "/google.longrunning.Operations/GetOperation"
)

// Request fields that receive path-bound resource names.
const (
	fieldName   = "name"
	fieldParent = "parent"
)

// binding renders the resource name a path binds into the request.
type binding func(path map[string]string) string

func projectPath(path map[string]string) string { return "projects/" + path[pathProject] }
func configPath(path map[string]string) string {
	return projectPath(path) + "/instanceConfigs/" + path["instanceConfig"]
}
func instancePath(path map[string]string) string {
	return instanceName(path[pathProject], path[pathInstance])
}
func databasePath(path map[string]string) string {
	return databaseName(path[pathProject], path[pathInstance], path[pathDatabase])
}
func instanceOperationPath(path map[string]string) string {
	return instancePath(path) + "/operations/" + path["operation"]
}
func databaseOperationPath(path map[string]string) string {
	return databasePath(path) + "/operations/" + path["operation"]
}

// rpc maps one REST method onto an emulator gRPC call, following the
// google.api.http annotations of the Spanner admin v1 protos: body is "",
// "*", or the request field that receives the body; field receives the
// resource name bound from the URL path.
type rpc struct {
	method            string
	request, response proto.Message
	body, field       string
	bind              binding
}

var rpcs = map[string]rpc{
	methodConfigsGet:           {instanceAdmin + "GetInstanceConfig", &instancepb.GetInstanceConfigRequest{}, &instancepb.InstanceConfig{}, "", fieldName, configPath},
	methodConfigsList:          {rpcListInstanceConfigs, &instancepb.ListInstanceConfigsRequest{}, &instancepb.ListInstanceConfigsResponse{}, "", fieldParent, projectPath},
	methodInstancesCreate:      {instanceAdmin + "CreateInstance", &instancepb.CreateInstanceRequest{}, &longrunningpb.Operation{}, "*", fieldParent, projectPath},
	methodInstancesGet:         {instanceAdmin + "GetInstance", &instancepb.GetInstanceRequest{}, &instancepb.Instance{}, "", fieldName, instancePath},
	methodInstancesList:        {instanceAdmin + "ListInstances", &instancepb.ListInstancesRequest{}, &instancepb.ListInstancesResponse{}, "", fieldParent, projectPath},
	methodInstancesUpdate:      {instanceAdmin + "UpdateInstance", &instancepb.UpdateInstanceRequest{}, &longrunningpb.Operation{}, "*", "instance.name", instancePath},
	methodInstancesDelete:      {instanceAdmin + "DeleteInstance", &instancepb.DeleteInstanceRequest{}, &emptypb.Empty{}, "", fieldName, instancePath},
	methodInstanceOperationGet: {rpcGetOperation, &longrunningpb.GetOperationRequest{}, &longrunningpb.Operation{}, "", fieldName, instanceOperationPath},
	methodDatabasesCreate:      {databaseAdmin + "CreateDatabase", &databasepb.CreateDatabaseRequest{}, &longrunningpb.Operation{}, "*", fieldParent, instancePath},
	methodDatabasesGet:         {databaseAdmin + "GetDatabase", &databasepb.GetDatabaseRequest{}, &databasepb.Database{}, "", fieldName, databasePath},
	methodDatabasesList:        {databaseAdmin + "ListDatabases", &databasepb.ListDatabasesRequest{}, &databasepb.ListDatabasesResponse{}, "", fieldParent, instancePath},
	methodDatabasesUpdate:      {databaseAdmin + "UpdateDatabase", &databasepb.UpdateDatabaseRequest{}, &longrunningpb.Operation{}, pathDatabase, "database.name", databasePath},
	methodDatabasesDelete:      {databaseAdmin + "DropDatabase", &databasepb.DropDatabaseRequest{}, &emptypb.Empty{}, "", pathDatabase, databasePath},
	methodDatabasesApplyDdl:    {databaseAdmin + "UpdateDatabaseDdl", &databasepb.UpdateDatabaseDdlRequest{}, &longrunningpb.Operation{}, "*", pathDatabase, databasePath},
	methodDatabasesGetDdl:      {databaseAdmin + "GetDatabaseDdl", &databasepb.GetDatabaseDdlRequest{}, &databasepb.GetDatabaseDdlResponse{}, "", pathDatabase, databasePath},
	methodDatabaseOperationGet: {rpcGetOperation, &longrunningpb.GetOperationRequest{}, &longrunningpb.Operation{}, "", fieldName, databaseOperationPath},
	methodBackupsList:          {databaseAdmin + "ListBackups", &databasepb.ListBackupsRequest{}, &databasepb.ListBackupsResponse{}, "", fieldParent, instancePath},
}

// systemParameters are Google API system query parameters with no request field.
var systemParameters = map[string]bool{"alt": true, "prettyPrint": true, "$.xgafv": true, "quotaUser": true}

// build transcodes one REST request into the emulator request message. The
// path binding is applied last, so a body cannot redirect the call to another
// resource or project.
func (r rpc) build(request *gateway.RESTRequest) (proto.Message, error) {
	message := r.request.ProtoReflect().New()
	if r.body != "" && len(request.JSON) > 0 {
		target := message
		if r.body != "*" {
			target = message.Mutable(message.Descriptor().Fields().ByName(protoreflect.Name(r.body))).Message()
		}
		if err := protojson.Unmarshal(request.JSON, target.Interface()); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid JSON payload: %v", err)
		}
	}
	for key, values := range request.Query {
		if err := setQuery(message, key, values); err != nil {
			return nil, err
		}
	}
	setName(message, r.field, r.bind(request.Path))
	if create, ok := message.Interface().(*instancepb.CreateInstanceRequest); ok && create.GetInstance() != nil {
		create.Instance.Name = create.GetParent() + "/instances/" + create.GetInstanceId()
	}
	return message.Interface(), nil
}

// setName stores a resource name at a dotted proto field path.
func setName(message protoreflect.Message, path, value string) {
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		message = message.Mutable(message.Descriptor().Fields().ByName(protoreflect.Name(part))).Message()
	}
	message.Set(message.Descriptor().Fields().ByName(protoreflect.Name(parts[len(parts)-1])), protoreflect.ValueOfString(value))
}

// setQuery binds one query parameter to a top-level scalar or field-mask field.
func setQuery(message protoreflect.Message, key string, values []string) error {
	if systemParameters[key] {
		return nil
	}
	field := message.Descriptor().Fields().ByJSONName(key)
	if field == nil || field.IsList() || len(values) != 1 {
		return status.Errorf(codes.InvalidArgument, "Invalid query parameter %q", key)
	}
	value, err := queryValue(field, key, values[0])
	if err != nil {
		return err
	}
	message.Set(field, value)
	return nil
}

// queryValue converts one query string to the field's value.
func queryValue(field protoreflect.FieldDescriptor, key, value string) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(value), nil
	case protoreflect.Int32Kind:
		number, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return protoreflect.Value{}, status.Errorf(codes.InvalidArgument, "Invalid integer for %q", key)
		}
		return protoreflect.ValueOfInt32(int32(number)), nil
	case protoreflect.MessageKind:
		if field.Message().FullName() == "google.protobuf.FieldMask" {
			return protoreflect.ValueOfMessage((&fieldmaskpb.FieldMask{Paths: strings.Split(value, ",")}).ProtoReflect()), nil
		}
	default:
	}
	return protoreflect.Value{}, gap("query parameter %q", key)
}

// forward transcodes, invokes the emulator under the scope's mutation gate,
// settles index references for creates and deletes, and renders proto3 JSON
// with the service-managed fields the emulator does not model.
func (p *Provider) forward(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	route, ok := rpcs[call.Method]
	if !ok {
		return nil, gap("method %s", call.Method)
	}
	message, err := route.build(request)
	if err != nil {
		return nil, err
	}
	if err := supported(message); err != nil {
		return nil, err
	}
	var document map[string]any
	key := backend.Key{Service: serviceName, Scope: scope}
	err = p.manager.Use(call.Context, key, func(ctx context.Context, active backend.Backend) error {
		b, err := driverOf(active)
		if err != nil {
			return err
		}
		response := route.response.ProtoReflect().New().Interface()
		if err := p.invoke(ctx, b, route, message, response, scope); err != nil {
			return err
		}
		if document, err = render(response); err != nil {
			return err
		}
		return decorator{b: b, scope: scope, created: p.created, now: p.clock()}.decorate(ctx, call.Method, document)
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // emulator gRPC statuses carry the client-visible code and message
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// invoke performs one emulator call and settles its index references.
// Backups are not exposed locally: after confirming the instance exists, the
// provider reports the empty collection instead of the emulator's
// UNIMPLEMENTED, since no backup can be created through this provider.
func (p *Provider) invoke(ctx context.Context, b driver, route rpc, message, response proto.Message, scope state.Scope) error {
	if list, ok := message.(*databasepb.ListBackupsRequest); ok {
		return b.client().Invoke(ctx, instanceAdmin+"GetInstance", &instancepb.GetInstanceRequest{Name: list.GetParent()}, &instancepb.Instance{})
	}
	mutation := mutationOf(ctx, b, message, scope)
	return p.settle(ctx, b, mutation, b.client().Invoke(ctx, route.method, message, response))
}

// render converts a response to a mutable proto3 JSON document.
func render(response proto.Message) (map[string]any, error) {
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "render the Spanner response: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		return nil, status.Errorf(codes.Internal, "decode the Spanner response: %v", err)
	}
	return document, nil
}
