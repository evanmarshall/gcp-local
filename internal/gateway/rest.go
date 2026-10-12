package gateway

import (
	"bytes"
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
	// Verb, Host, and RawPath are the request line as received. Storage needs
	// them to continue resumable sessions and to verify V4 signatures, which
	// sign the method, the Host header, and the escaped path.
	Verb, Host, RawPath string
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
	multi      []bool // a {name=**} variable spans one or more segments
	// whole marks a {name=***} variable: the rest of the path, unescaped as
	// one value, so an escaped slash belongs to it (a Storage object name).
	whole []bool
}
type Gateway struct {
	Registry *Registry
	resolve  Resolver
	routes   []compiledRoute
	rpcs     map[string]generated.RPC
	// transcoded gRPC methods run through a REST route's provider method. A
	// shared mixin RPC (IAMPolicy) has one binding per resource kind it
	// serves; shared marks those RPCs.
	transcoded map[string][]generated.Transcoding
	shared     map[string]bool
}

func New(registry *Registry, resolve Resolver) *Gateway {
	if registry == nil {
		registry = NewRegistry()
	}
	g := &Gateway{Registry: registry, resolve: resolve, rpcs: map[string]generated.RPC{}, transcoded: map[string][]generated.Transcoding{}, shared: map[string]bool{}}
	for _, contract := range contracts() {
		g.routes = append(g.routes, compileRoute(contract))
	}
	for _, rpc := range generated.PubSubMethods {
		g.rpcs[rpc.Method] = rpc
	}
	for _, table := range [][]generated.RPC{generated.IAMMethods, generated.SpannerMethods, generated.BigtableMethods, generated.SecretManagerMethods, generated.CloudTasksMethods, generated.CloudKMSMethods,
		generated.RunServicesMethods, generated.RunRevisionsMethods, generated.OperationsMethods,
		generated.SpannerInstanceAdminMethods, generated.SpannerDatabaseAdminMethods,
		generated.BigtableInstanceAdminMethods, generated.BigtableTableAdminMethods, generated.IAMAdminMethods} {
		for _, rpc := range table {
			g.rpcs[rpc.Method] = rpc
		}
	}
	g.addTranscoding()
	for _, rpc := range generated.FirestoreMethods {
		g.rpcs[rpc.Method] = rpc
	}
	for _, rpc := range generated.LoggingMethods {
		g.rpcs[rpc.Method] = rpc
	}
	return g
}

// addTranscoding indexes every transcoded binding by RPC and marks the
// shared IAMPolicy and Operations mixin RPCs, which pick a binding by resource.
func (g *Gateway) addTranscoding() {
	for _, rpc := range append(append([]generated.RPC(nil), generated.IAMMethods...), generated.OperationsMethods...) {
		g.shared[rpc.Method] = true
	}
	for _, table := range [][]generated.Transcoding{generated.SecretManagerTranscoding, generated.CloudTasksTranscoding, generated.CloudKMSTranscoding,
		generated.RunServicesTranscoding, generated.RunRevisionsTranscoding,
		generated.SpannerInstanceAdminTranscoding, generated.SpannerDatabaseAdminTranscoding,
		generated.BigtableInstanceAdminTranscoding, generated.BigtableTableAdminTranscoding, generated.IAMAdminTranscoding} {
		for _, binding := range table {
			g.transcoded[binding.RPC] = append(g.transcoded[binding.RPC], binding)
		}
	}
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
	routes = append(routes, generated.PubSubSchemaRestRoutes...)
	routes = append(routes, generated.RedisRoutes...)
	routes = append(routes, generated.VertexAIRoutes...)
	routes = append(routes, generated.LoggingRoutes...)
	sort.SliceStable(routes, func(i, j int) bool {
		left, right := strings.Contains(routes[i].Path, ":"), strings.Contains(routes[j].Path, ":")
		return left && !right
	})
	// The XML API's /{bucket}/{object} shape matches almost any path, so it
	// is tried only after every service route.
	return append(routes, generated.StorageXMLRoutes...)
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
		name, whole := strings.CutSuffix(contract.Path[bounds[2]:bounds[3]], "=***")
		name, multi := strings.CutSuffix(name, "=**")
		if multi || whole {
			pattern.WriteString("([^/]+(?:/[^/]+)*)")
		} else {
			pattern.WriteString("([^/]+)")
		}
		compiled.names = append(compiled.names, name)
		compiled.multi = append(compiled.multi, multi)
		compiled.whole = append(compiled.whole, whole)
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
	request := &RESTRequest{Path: map[string]string{}, Query: query, Headers: r.Header.Clone(),
		Verb: r.Method, Host: r.Host, RawPath: r.URL.EscapedPath()}
	request.Headers.Del("Authorization")
	for index, name := range route.names {
		value, err := pathVariable(matches[index+1], route.multi[index] && !route.whole[index])
		if err != nil {
			return nil, Call{}, err
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

// pathVariable unescapes one matched route variable. A multi-segment value is
// unescaped segment by segment, so an escaped slash cannot forge a segment
// boundary and dot segments are refused.
func pathVariable(raw string, multi bool) (string, error) {
	if !multi {
		value, err := url.PathUnescape(raw)
		if err != nil {
			return "", status.Error(codes.InvalidArgument, "Malformed resource path")
		}
		return value, nil
	}
	segments := strings.Split(raw, "/")
	for i, segment := range segments {
		value, err := url.PathUnescape(segment)
		if err != nil || value == "" || value == "." || value == ".." || strings.Contains(value, "/") {
			return "", status.Error(codes.InvalidArgument, "Malformed resource path")
		}
		segments[i] = value
	}
	return strings.Join(segments, "/"), nil
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
	if trimmed := bytes.TrimSpace(body); len(trimmed) == 0 || string(trimmed) == "null" {
		// Google's front end reads an absent body as the empty request
		// message; clients send none for methods like getIamPolicy. The
		// generated Go API clients send a JSON null for a nil request object
		// (objects.rewrite without metadata), which it reads the same way.
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
// A copy failure cannot retract the committed status. A closable body is a
// live stream (server-sent events, a JSON array): every write is flushed so
// it reaches the client as the provider produces it, and the body is closed
// when the copy ends so a provider writing into a pipe stops when the client
// goes away. A fixed payload is copied unflushed.
func streamBody(w http.ResponseWriter, code int, body io.Reader) {
	closer, live := body.(io.Closer)
	if live {
		defer closer.Close()
	}
	w.WriteHeader(code)
	flusher, ok := w.(http.Flusher)
	if !live || !ok {
		_, _ = io.Copy(w, body)
		return
	}
	_, _ = io.Copy(flushWriter{w, flusher}, body)
}

// flushWriter flushes the response after each write.
type flushWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	f.f.Flush()
	return n, err //nolint:wrapcheck // the copy loop only needs the transport error
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
