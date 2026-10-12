package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/api/generated"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// errUntranscodable hides a request or response that does not fit between a
// gRPC method and its REST route; it is a contract defect, never client input.
var errUntranscodable = status.Error(codes.Internal, "The gRPC method does not fit its REST provider method")

// transcode serves a gRPC method through the REST route that shares its
// provider method, following Google's google.api.http mapping: the bound
// resource fills the path, the body field becomes the JSON body, and the
// remaining fields become query parameters. The provider sees the request a
// REST client would send, so both transports share one implementation,
// permission plan, and error path.
func (g *Gateway) transcode(ctx context.Context, binding generated.Transcoding, request proto.Message) (proto.Message, error) {
	call, err := g.rpcCall(ctx, binding.RPC, request)
	if err != nil {
		if pattern, ok := unservedBinding(request, g.rpcs[binding.RPC].ResourceField, binding.Unserved); ok && status.Code(err) == codes.InvalidArgument {
			return nil, status.Errorf(codes.Unimplemented, "No provider implements %s for %s resources", binding.RPC, pattern)
		}
		return nil, err
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-gcp-local-trace-id", call.TraceID))
	rest, path, err := g.restFor(binding, call.Resource, request)
	if err != nil {
		return nil, err
	}
	// The call matches what the REST gateway builds for this route.
	restCall := Call{Context: call.Context, Method: binding.REST, Location: "global", Principal: call.Principal, TraceID: call.TraceID, Resource: path}
	result, err := g.Registry.Invoke(restCall, rest)
	if err != nil {
		return nil, err
	}
	return g.rpcResponse(ctx, binding, result)
}

// transcodingFor picks the binding that serves one request. A service's own
// RPC has at most one. A shared mixin RPC (IAMPolicy) has one per resource
// kind a transcoded service owns, chosen by the request's resource; any other
// resource keeps the RPC's own dispatch (Pub/Sub). A shared RPC that no
// binding serves for an owned resource kind, such as TestIamPermissions on a
// crypto key, is a coverage gap rather than a Pub/Sub request.
func (g *Gateway) transcodingFor(method string, request proto.Message) (generated.Transcoding, bool, error) {
	bindings := g.transcoded[method]
	if !g.shared[method] {
		if len(bindings) == 0 {
			return generated.Transcoding{}, false, nil
		}
		return bindings[0], true, nil
	}
	// A request without a resource keeps the RPC's own dispatch, which
	// refuses it.
	resource, ok := requestResource(request, g.rpcs[method].ResourceField)
	if !ok {
		return generated.Transcoding{}, false, nil
	}
	for _, binding := range bindings {
		if bindResource(&Call{}, resource, bindingPattern(binding)) == nil {
			return binding, true, nil
		}
	}
	for shared := range g.shared {
		for _, binding := range g.transcoded[shared] {
			if pattern := bindingPattern(binding); bindResource(&Call{}, resource, pattern) == nil {
				return generated.Transcoding{}, false, status.Errorf(codes.Unimplemented, "No provider implements %s for %s resources", method, pattern)
			}
		}
	}
	return generated.Transcoding{}, false, nil
}

// requestResource reads a request's resource selector field.
func requestResource(request proto.Message, field string) (string, bool) {
	resource, err := resourceField(request, field)
	return resource, err == nil
}

// bindingPattern returns the resource pattern of a binding's path variable.
func bindingPattern(binding generated.Transcoding) string {
	_, pattern, _ := strings.Cut(variables.FindStringSubmatch(binding.Path)[1], "=")
	return pattern
}

// rpcResponse renders a provider's REST result as the gRPC response or
// status, and forwards the IAM decision ID as gRPC header metadata.
func (g *Gateway) rpcResponse(ctx context.Context, binding generated.Transcoding, result any) (proto.Message, error) {
	response, ok := result.(*RESTResponse)
	if !ok || response == nil || response.Body != nil {
		return nil, errUntranscodable
	}
	if decision := response.Headers.Get("X-Gcp-Local-Decision-Id"); decision != "" {
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-gcp-local-decision-id", decision))
	}
	encoded, err := json.Marshal(response.JSON)
	if err != nil {
		return nil, errUntranscodable
	}
	if response.Status >= http.StatusBadRequest {
		return nil, restError(response.Status, encoded)
	}
	message := g.rpcs[binding.RPC].NewResponse()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(encoded, message); err != nil {
		return nil, errUntranscodable
	}
	return message, nil
}

// unservedBinding reports which of a method's other HTTP bindings names the
// request's resource, such as a regional Secret Manager secret.
func unservedBinding(request proto.Message, field string, patterns []string) (string, bool) {
	resource, err := resourceField(request, field)
	if err != nil {
		return "", false
	}
	for _, pattern := range patterns {
		if bindResource(&Call{}, resource, pattern) == nil {
			return pattern, true
		}
	}
	return "", false
}

// restFor renders one gRPC request as the REST request of its paired route
// and returns that route's unescaped path.
func (g *Gateway) restFor(binding generated.Transcoding, resource string, request proto.Message) (*RESTRequest, string, error) {
	fields, err := requestFields(request)
	if err != nil {
		return nil, "", err
	}
	body, query := splitFields(binding, fields)
	escaped := strings.Split(resource, "/")
	for i, segment := range escaped {
		escaped[i] = url.PathEscape(segment)
	}
	suffix := variables.ReplaceAllLiteralString(binding.Path, strings.Join(escaped, "/"))
	route, path, matches := g.routeFor(binding, suffix)
	if route == nil && g.hasRoute(binding) {
		// The binding's pattern is wider than the route: Encrypt binds
		// cryptoKeys/**, so it may name a version the REST route never takes.
		return nil, "", status.Errorf(codes.Unimplemented, "No provider implements %s for %s", binding.RPC, resource)
	}
	if route == nil {
		return nil, "", errUntranscodable
	}
	rest := &RESTRequest{Path: map[string]string{}, Query: query, Headers: http.Header{}}
	for index, name := range route.names {
		// Each segment was escaped above, so it unescapes cleanly.
		rest.Path[name], _ = url.PathUnescape(matches[index+1])
	}
	if route.contract.JSONBody {
		rest.JSON = json.RawMessage("{}")
		if body != nil {
			rest.JSON, _ = json.Marshal(body) //nolint:errchkjson // re-encodes decoded protojson
		}
	}
	unescaped, _ := url.PathUnescape(path)
	return rest, unescaped, nil
}

// splitFields separates the JSON body from the query parameters. A path
// field stays only inside a body field that contains it (secret.name).
func splitFields(binding generated.Transcoding, fields map[string]any) (any, url.Values) {
	var body any
	switch binding.Body {
	case "*":
		delete(fields, binding.Field)
		body, fields = fields, nil
	case "":
		delete(fields, binding.Field)
	default:
		body = fields[binding.Body]
		delete(fields, binding.Body)
		delete(fields, binding.Field)
	}
	query := url.Values{}
	for name, value := range fields {
		flattenQuery(query, name, value)
	}
	return body, query
}

// routeFor finds the binding's REST route for one rendered binding path and
// returns the route's own path and variable matches. REST and gRPC versions
// may differ (Cloud Tasks v2 onto v2beta3), so the route keeps its version.
func (g *Gateway) routeFor(binding generated.Transcoding, rendered string) (*compiledRoute, string, []string) {
	_, suffix, _ := strings.Cut(strings.TrimPrefix(rendered, "/"), "/")
	for i := range g.routes {
		route := &g.routes[i]
		if route.contract.Method != binding.REST || route.contract.Verb != binding.Verb {
			continue
		}
		version, _, _ := strings.Cut(strings.TrimPrefix(route.contract.Path, "/"), "/")
		path := "/" + version + "/" + suffix
		if matches := route.expression.FindStringSubmatch(path); matches != nil {
			return route, path, matches
		}
	}
	return nil, "", nil
}

// hasRoute reports whether the binding's REST method has a route on its verb.
func (g *Gateway) hasRoute(binding generated.Transcoding) bool {
	for i := range g.routes {
		if g.routes[i].contract.Method == binding.REST && g.routes[i].contract.Verb == binding.Verb {
			return true
		}
	}
	return false
}

// requestFields decodes a request's protojson fields, keeping numbers exact.
func requestFields(request proto.Message) (map[string]any, error) {
	encoded, err := protojson.Marshal(request)
	if err != nil {
		return nil, errUntranscodable
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	fields := map[string]any{}
	if err := decoder.Decode(&fields); err != nil {
		return nil, errUntranscodable
	}
	return fields, nil
}

// flattenQuery adds one JSON field as query parameters: scalars as text,
// repeated fields as repeated parameters, and messages as dotted names.
func flattenQuery(query url.Values, name string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			flattenQuery(query, name+"."+key, nested)
		}
	case []any:
		for _, item := range typed {
			flattenQuery(query, name, item)
		}
	case string:
		query.Add(name, typed)
	case json.Number:
		query.Add(name, typed.String())
	case bool:
		query.Add(name, strconv.FormatBool(typed))
	}
}

// restError turns a provider's REST error body into the gRPC status a Google
// API returns for it. An HTTP 401 is UNAUTHENTICATED whatever its body says.
func restError(code int, encoded []byte) error {
	var body struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(encoded, &body) != nil {
		return errUntranscodable
	}
	if code == http.StatusUnauthorized {
		return status.Error(codes.Unauthenticated, body.Error.Message)
	}
	for wire := codes.Canceled; wire <= codes.Unauthenticated; wire++ {
		if statusName(wire) == body.Error.Status {
			return status.Error(wire, body.Error.Message)
		}
	}
	return errUntranscodable
}

// statusName spells a code the way Google's REST error bodies do.
func statusName(code codes.Code) string {
	if name := statusNames[code]; name != "" {
		return name
	}
	return strings.ToUpper(code.String())
}
