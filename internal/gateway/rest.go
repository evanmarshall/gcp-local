package gateway

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/evanmarshall/gcp-local/api/generated"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RESTRequest struct {
	Path    map[string]string
	Query   url.Values
	Headers http.Header
	JSON    json.RawMessage
	Media   io.Reader
}
type RESTResponse struct {
	Status  int
	Headers http.Header
	JSON    any
	Body    io.Reader
}
type compiledRoute struct {
	contract   generated.Route
	expression *regexp.Regexp
	names      []string
}
type Gateway struct {
	Registry *Registry
	resolve  Resolver
	routes   []compiledRoute
	rpcs     map[string]generated.RPC
}

func New(registry *Registry, resolve Resolver) *Gateway {
	if registry == nil {
		registry = NewRegistry()
	}
	g := &Gateway{Registry: registry, resolve: resolve, rpcs: map[string]generated.RPC{}}
	for _, contract := range contracts() {
		g.routes = append(g.routes, compileRoute(contract))
	}
	for _, rpc := range generated.PubSubMethods {
		g.rpcs[rpc.Method] = rpc
	}
	for _, rpc := range generated.IAMMethods {
		g.rpcs[rpc.Method] = rpc
	}
	return g
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Local administrative paths are never service contracts, even when mounted alone.
	if strings.HasPrefix(r.URL.Path, "/_local/") {
		writeError(w, gap("local diagnostics route"))
		return
	}
	for _, route := range g.routes {
		if !route.matches(r) {
			continue
		}
		request, call, err := g.restRequest(r, &route)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("X-Gcp-Local-Trace-Id", call.TraceID)
		g.dispatchREST(w, call, request)
		return
	}
	writeError(w, gap("this REST API method"))
}

// contracts returns the REST route tables, custom-method suffixes first so the
// generic variable pattern cannot swallow the literal ":method" suffix.
func contracts() []generated.Route {
	routes := append(append(append([]generated.Route(nil), generated.StorageRoutes...), generated.RunRoutes...), generated.ComputeRoutes...)
	routes = append(routes, generated.SecretManagerRoutes...)
	routes = append(routes, generated.CloudKMSRoutes...)
	routes = append(routes, generated.FirestoreRoutes...)
	routes = append(routes, generated.BigQueryRoutes...)
	routes = append(routes, generated.SpannerRoutes...)
	routes = append(routes, generated.BigtableRoutes...)
	routes = append(routes, generated.ArtifactRegistryRoutes...)
	routes = append(routes, generated.ResourceManagerRoutes...)
	routes = append(routes, generated.IAMRoutes...)
	routes = append(routes, generated.CloudTasksRoutes...)
	routes = append(routes, generated.CloudSQLRoutes...)
	routes = append(routes, generated.PubSubRestRoutes...)
	routes = append(routes, generated.RedisRoutes...)
	sort.SliceStable(routes, func(i, j int) bool {
		left, right := strings.Contains(routes[i].Path, ":"), strings.Contains(routes[j].Path, ":")
		return left && !right
	})
	return routes
}

// variables is the REST path template variable shape.
var variables = regexp.MustCompile(`\{([^{}]+)\}`)

// compileRoute turns one wire contract into its match table entry.
func compileRoute(contract generated.Route) compiledRoute {
	compiled := compiledRoute{contract: contract}
	var pattern strings.Builder
	pattern.WriteString("^")
	last := 0
	for _, bounds := range variables.FindAllStringSubmatchIndex(contract.Path, -1) {
		pattern.WriteString(regexp.QuoteMeta(contract.Path[last:bounds[0]]))
		pattern.WriteString("([^/]+)")
		compiled.names = append(compiled.names, contract.Path[bounds[2]:bounds[3]])
		last = bounds[1]
	}
	pattern.WriteString(regexp.QuoteMeta(contract.Path[last:]))
	pattern.WriteString("$")
	compiled.expression = regexp.MustCompile(pattern.String())
	return compiled
}

// matches reports whether a request selects a route by method and path shape.
func (route *compiledRoute) matches(r *http.Request) bool {
	if r.Method != route.contract.Verb {
		return false
	}
	return route.expression.FindStringSubmatch(r.URL.EscapedPath()) != nil
}

// restRequest builds the REST request and call for one matched route.
func (g *Gateway) restRequest(r *http.Request, route *compiledRoute) (*RESTRequest, Call, error) {
	matches := route.expression.FindStringSubmatch(r.URL.EscapedPath())
	call, err := g.newCall(r.Context(), route.contract.Method, r.Header.Values("Authorization"), r.Header.Values("Traceparent"))
	if err != nil {
		return nil, Call{}, err
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, Call{}, status.Error(codes.InvalidArgument, "Malformed query string")
	}
	request := &RESTRequest{Path: map[string]string{}, Query: query, Headers: r.Header.Clone()}
	request.Headers.Del("Authorization")
	for index, name := range route.names {
		value, err := url.PathUnescape(matches[index+1])
		if err != nil {
			return nil, Call{}, status.Error(codes.InvalidArgument, "Malformed resource path")
		}
		request.Path[name] = value
	}
	call.Resource = r.URL.Path
	// Bucket/object names do not encode a project. Providers resolve that owner.
	if route.contract.Method == "storage.buckets.insert" || route.contract.Method == "storage.buckets.list" {
		call.Project = query.Get("project")
	}
	if strings.HasPrefix(route.contract.Method, "pubsub.http.") {
		call.Project = request.Path["project"]
	}
	if err := restBody(r, route, request); err != nil {
		return nil, Call{}, err
	}
	return request, call, nil
}

// restBody attaches the request body the route contract expects: streaming
// media, a bounded JSON object, or nothing.
func restBody(r *http.Request, route *compiledRoute, request *RESTRequest) error {
	if route.contract.Media {
		request.Media = r.Body
		return nil
	}
	if !route.contract.JSONBody {
		return nil
	}
	const maxJSON = 2 << 20
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSON+1))
	if err != nil {
		return status.Error(codes.InvalidArgument, "Unable to read JSON body")
	}
	if len(body) > maxJSON {
		return status.Error(codes.ResourceExhausted, "JSON request exceeds two MiB")
	}
	if len(body) == 0 {
		// Google's front end reads an absent body as the empty request
		// message; clients send none for methods like getIamPolicy.
		body = []byte("{}")
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return status.Error(codes.InvalidArgument, "Expected a JSON object body")
	}
	request.JSON = body
	return nil
}

// streamBody commits the status and copies a streaming body to the client.
// A copy failure cannot retract the committed status.
func streamBody(w http.ResponseWriter, code int, body io.Reader) {
	w.WriteHeader(code)
	_, _ = io.Copy(w, body)
}

// dispatchREST invokes the provider and writes the response body it returns.
func (g *Gateway) dispatchREST(w http.ResponseWriter, call Call, request *RESTRequest) {
	result, err := g.Registry.Invoke(call, request)
	if err != nil {
		writeError(w, err)
		return
	}
	response, ok := result.(*RESTResponse)
	if !ok || response == nil {
		writeError(w, status.Error(codes.Internal, "Provider returned an invalid response type"))
		return
	}
	maps.Copy(w.Header(), response.Headers)
	code := response.Status
	if code == 0 {
		code = 200
	}
	if response.Body != nil {
		streamBody(w, code, response.Body)
		return
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if response.JSON != nil {
		// Marshal before committing the status so an encoding failure is a
		// reportable 500, not a silent truncated body.
		encoded, err := json.Marshal(response.JSON)
		if err != nil {
			writeError(w, status.Error(codes.Internal, "Response encoding failed"))
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write(encoded)
		return
	}
	w.WriteHeader(code)
}
