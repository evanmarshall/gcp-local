package spanner

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	return provider.InstanceName(path[pathProject], path[pathInstance])
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
	// unmodeled are body fields the emulator would accept and ignore; set to
	// a non-default value, they are a gap.
	unmodeled []string
}

var rpcs = map[string]rpc{
	methodConfigsGet:           {instanceAdmin + "GetInstanceConfig", &instancepb.GetInstanceConfigRequest{}, &instancepb.InstanceConfig{}, "", fieldName, configPath, nil},
	methodConfigsList:          {rpcListInstanceConfigs, &instancepb.ListInstanceConfigsRequest{}, &instancepb.ListInstanceConfigsResponse{}, "", fieldParent, projectPath, nil},
	methodInstancesCreate:      {instanceAdmin + "CreateInstance", &instancepb.CreateInstanceRequest{}, &longrunningpb.Operation{}, "*", fieldParent, projectPath, nil},
	methodInstancesGet:         {instanceAdmin + "GetInstance", &instancepb.GetInstanceRequest{}, &instancepb.Instance{}, "", fieldName, instancePath, nil},
	methodInstancesList:        {instanceAdmin + "ListInstances", &instancepb.ListInstancesRequest{}, &instancepb.ListInstancesResponse{}, "", fieldParent, projectPath, nil},
	methodInstancesUpdate:      {instanceAdmin + "UpdateInstance", &instancepb.UpdateInstanceRequest{}, &longrunningpb.Operation{}, "*", "instance.name", instancePath, nil},
	methodInstancesDelete:      {instanceAdmin + "DeleteInstance", &instancepb.DeleteInstanceRequest{}, &emptypb.Empty{}, "", fieldName, instancePath, nil},
	methodInstanceOperationGet: {rpcGetOperation, &longrunningpb.GetOperationRequest{}, &longrunningpb.Operation{}, "", fieldName, instanceOperationPath, nil},
	// The emulator accepts a CMEK key and ignores it.
	methodDatabasesCreate:      {databaseAdmin + "CreateDatabase", &databasepb.CreateDatabaseRequest{}, &longrunningpb.Operation{}, "*", fieldParent, instancePath, []string{"encryptionConfig"}},
	methodDatabasesGet:         {databaseAdmin + "GetDatabase", &databasepb.GetDatabaseRequest{}, &databasepb.Database{}, "", fieldName, databasePath, nil},
	methodDatabasesList:        {databaseAdmin + "ListDatabases", &databasepb.ListDatabasesRequest{}, &databasepb.ListDatabasesResponse{}, "", fieldParent, instancePath, nil},
	methodDatabasesUpdate:      {databaseAdmin + "UpdateDatabase", &databasepb.UpdateDatabaseRequest{}, &longrunningpb.Operation{}, pathDatabase, "database.name", databasePath, nil},
	methodDatabasesDelete:      {databaseAdmin + "DropDatabase", &databasepb.DropDatabaseRequest{}, &emptypb.Empty{}, "", pathDatabase, databasePath, nil},
	methodDatabasesApplyDdl:    {databaseAdmin + "UpdateDatabaseDdl", &databasepb.UpdateDatabaseDdlRequest{}, &longrunningpb.Operation{}, "*", pathDatabase, databasePath, nil},
	methodDatabasesGetDdl:      {databaseAdmin + "GetDatabaseDdl", &databasepb.GetDatabaseDdlRequest{}, &databasepb.GetDatabaseDdlResponse{}, "", pathDatabase, databasePath, nil},
	methodDatabaseOperationGet: {rpcGetOperation, &longrunningpb.GetOperationRequest{}, &longrunningpb.Operation{}, "", fieldName, databaseOperationPath, nil},
	methodBackupsList:          {databaseAdmin + "ListBackups", &databasepb.ListBackupsRequest{}, &databasepb.ListBackupsResponse{}, "", fieldParent, instancePath, nil},
}

// pageParameters are the list query parameters list applies itself.
var pageParameters = map[string]bool{"pageSize": true, "pageToken": true, "filter": true}

// systemParameters are Google API system query parameters with no request field.
var systemParameters = map[string]bool{"alt": true, "prettyPrint": true, "$.xgafv": true, "quotaUser": true}

// build transcodes one REST request into the emulator request message. The
// path binding is applied last, so a body cannot redirect the call to another
// resource or project.
func (r rpc) build(request *gateway.RESTRequest) (proto.Message, error) {
	message := r.request.ProtoReflect().New()
	if r.body != "" {
		target, root := message, ""
		if r.body != "*" {
			target, root = message.Mutable(message.Descriptor().Fields().ByName(protoreflect.Name(r.body))).Message(), r.body
		}
		if err := provider.DecodeProto(request.JSON, root, target.Interface(), gap, r.unmodeled...); err != nil {
			return nil, err
		}
	}
	paged := message.Descriptor().Fields().ByName("page_token") != nil
	for key, values := range request.Query {
		if paged && pageParameters[key] {
			continue // list pages it reads
		}
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
	return protoreflect.Value{}, gap("query parameter " + strconv.Quote(key))
}

// forward transcodes, invokes the emulator under the scope's mutation gate,
// settles index references for creates and deletes, and renders proto3 JSON
// with the service-managed fields the emulator does not model.
func (p *Provider) forward(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	route, ok := rpcs[call.Method]
	if !ok {
		return nil, gap("method " + call.Method)
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
		if err := p.invoke(ctx, b, route, message, response, request.Query, scope); err != nil {
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

// invoke performs one emulator call and settles its index references; list
// requests are paged by list.
func (p *Provider) invoke(ctx context.Context, b driver, route rpc, message, response proto.Message, query url.Values, scope state.Scope) error {
	if message.ProtoReflect().Descriptor().Fields().ByName("page_token") != nil {
		return list(ctx, b, route, message, response, query)
	}
	mutation := mutationOf(ctx, b, message, scope)
	return p.settle(ctx, b, mutation, b.client().Invoke(ctx, route.method, message, response))
}

// errPageToken marks a page token list didn't issue; list renders it as
// Spanner's refusal.
var errPageToken = status.Error(codes.InvalidArgument, "page token")

// paging is Spanner's list page size: a size of 0 or less is the maximum
// (ListDatabasesRequest reference; recorded for -1). Max is a local choice.
var paging = provider.Paging{Default: 1000, Max: 1000, InvalidToken: errPageToken}

// list reads every page of a list from the emulator, orders the items by
// name, and fills response with the page query asks for, so tokens, order,
// and refusals are the provider's rather than the emulator's. filter is a
// gap.
func list(ctx context.Context, b driver, route rpc, message, response proto.Message, query url.Values) error {
	out := response.ProtoReflect()
	field := out.Descriptor().Fields().ByName(listField(out.Descriptor()))
	items, err := listAll(ctx, b, route, message, field)
	if err != nil {
		return err
	}
	slices.SortFunc(items, func(a, b protoreflect.Message) int { return strings.Compare(itemName(a), itemName(b)) })
	if size, err := strconv.Atoi(query.Get("pageSize")); err == nil && size < 0 {
		query = maps.Clone(query)
		query.Del("pageSize")
	}
	page, next, err := provider.Page(items, query, paging, gap)
	if errors.Is(err, errPageToken) {
		// Recorded: "Invalid ListDatabases request" with a page_token violation.
		rpc := route.method[strings.LastIndex(route.method, "/")+1:]
		return provider.BadRequest("Invalid "+rpc+" request", "page_token", "Got: "+query.Get("pageToken")) //nolint:wrapcheck // Spanner's recorded refusal
	}
	if err != nil {
		return err
	}
	listed := out.Mutable(field).List()
	for _, item := range page {
		listed.Append(protoreflect.ValueOfMessage(item))
	}
	if next != "" {
		out.Set(out.Descriptor().Fields().ByName("next_page_token"), protoreflect.ValueOfString(next))
	}
	return nil
}

// listAll reads every item of a list from the emulator. Backups are not
// exposed locally: after confirming the instance exists, the list is empty
// instead of the emulator's UNIMPLEMENTED, since no backup can be created
// through this provider.
func listAll(ctx context.Context, b driver, route rpc, message proto.Message, field protoreflect.FieldDescriptor) ([]protoreflect.Message, error) {
	if backups, ok := message.(*databasepb.ListBackupsRequest); ok {
		return nil, b.client().Invoke(ctx, instanceAdmin+"GetInstance", &instancepb.GetInstanceRequest{Name: backups.GetParent()}, &instancepb.Instance{})
	}
	var items []protoreflect.Message
	request := message.ProtoReflect()
	for {
		page := route.response.ProtoReflect().New()
		if err := b.client().Invoke(ctx, route.method, message, page.Interface()); err != nil {
			return nil, err
		}
		listed := page.Get(field).List()
		for i := range listed.Len() {
			items = append(items, listed.Get(i).Message())
		}
		next := page.Get(page.Descriptor().Fields().ByName("next_page_token")).String()
		if next == "" {
			return items, nil
		}
		request.Set(request.Descriptor().Fields().ByName("page_token"), protoreflect.ValueOfString(next))
	}
}

// listField names a list response's repeated resource field.
func listField(response protoreflect.MessageDescriptor) protoreflect.Name {
	fields := response.Fields()
	for i := range fields.Len() {
		if field := fields.Get(i); field.IsList() && field.Kind() == protoreflect.MessageKind {
			return field.Name()
		}
	}
	panic("Spanner list response without a resource field: " + string(response.FullName()))
}

// itemName is a listed resource's name.
func itemName(item protoreflect.Message) string {
	return item.Get(item.Descriptor().Fields().ByName("name")).String()
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
