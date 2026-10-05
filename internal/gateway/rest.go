package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
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
	variables := regexp.MustCompile(`\{([^{}]+)\}`)
	for _, contract := range generated.StorageRoutes {
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
		g.routes = append(g.routes, compiled)
	}
	for _, rpc := range generated.PubSubMethods {
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
		if r.Method != route.contract.Verb {
			continue
		}
		matches := route.expression.FindStringSubmatch(r.URL.EscapedPath())
		if matches == nil {
			continue
		}
		call, err := g.newCall(r.Context(), route.contract.Method, r.Header.Values("Authorization"), r.Header.Values("Traceparent"))
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("X-Gcp-Local-Trace-Id", call.TraceID)
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeError(w, status.Error(codes.InvalidArgument, "Malformed query string"))
			return
		}
		request := &RESTRequest{Path: map[string]string{}, Query: query, Headers: r.Header.Clone()}
		request.Headers.Del("Authorization")
		for index, name := range route.names {
			value, err := url.PathUnescape(matches[index+1])
			if err != nil {
				writeError(w, status.Error(codes.InvalidArgument, "Malformed resource path"))
				return
			}
			request.Path[name] = value
		}
		call.Resource = r.URL.Path
		// Bucket/object names do not encode a project. Providers resolve that owner.
		if route.contract.Method == "storage.buckets.insert" || route.contract.Method == "storage.buckets.list" {
			call.Project = query.Get("project")
		}
		if route.contract.Media {
			request.Media = r.Body
		} else if route.contract.JSONBody {
			const maxJSON = 2 << 20
			body, err := io.ReadAll(io.LimitReader(r.Body, maxJSON+1))
			if err != nil {
				writeError(w, status.Error(codes.InvalidArgument, "Unable to read JSON body"))
				return
			}
			if len(body) > maxJSON {
				writeError(w, status.Error(codes.ResourceExhausted, "JSON request exceeds two MiB"))
				return
			}
			var object map[string]json.RawMessage
			if json.Unmarshal(body, &object) != nil || object == nil {
				writeError(w, status.Error(codes.InvalidArgument, "Expected a JSON object body"))
				return
			}
			request.JSON = body
		}
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
		for key, values := range response.Headers {
			w.Header()[key] = values
		}
		code := response.Status
		if code == 0 {
			code = 200
		}
		if response.Body != nil {
			if closer, ok := response.Body.(io.Closer); ok {
				defer closer.Close()
			}
			w.WriteHeader(code)
			_, _ = io.Copy(w, response.Body)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if response.JSON != nil {
			_ = json.NewEncoder(w).Encode(response.JSON)
		}
		return
	}
	writeError(w, gap("this REST API method"))
}
