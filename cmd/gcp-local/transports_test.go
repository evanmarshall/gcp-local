package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var updateTransports = flag.Bool("update-transports", false, "rewrite the matrix in docs/architecture/transports.md")

const transportsDoc = "../../docs/architecture/transports.md"

// gcpTransport is what Google serves for one gcp-local service, from its
// published protos (googleapis BUILD.bazel transports) and discovery documents.
type gcpTransport struct {
	summary string
	// grpc names Google's gRPC service for REST routes no pinned proto pairs;
	// empty when Google serves them on REST only.
	grpc string
	// restOnly lists route ID prefixes Google serves on REST only, though
	// the service has gRPC.
	restOnly []string
}

var gcpTransports = map[string]gcpTransport{
	"artifactregistry": {summary: "REST, gRPC", grpc: "google.devtools.artifactregistry.v1"},
	"bigquery":         {summary: "REST (v2 protos are reference only)"},
	"bigtable":         {summary: "REST and gRPC admin; gRPC data"},
	"cloudkms":         {summary: "REST, gRPC"},
	"cloudscheduler":   {summary: "REST, gRPC"},
	"cloudsql":         {summary: "REST; gRPC through the Java client", grpc: "google.cloud.sql.v1beta4"},
	"cloudtasks":       {summary: "REST, gRPC"},
	"compute":          {summary: "Compute REST; Network Management REST, gRPC", grpc: "google.cloud.networkmanagement.v1", restOnly: []string{"compute."}},
	"firestore":        {summary: "REST, gRPC; Listen and Write gRPC", grpc: "google.firestore.admin.v1.FirestoreAdmin"},
	"iam":              {summary: "REST, gRPC"},
	"logging":          {summary: "REST, gRPC"},
	"orgpolicy":        {summary: "REST, gRPC"},
	"pubsub":           {summary: "REST, gRPC; StreamingPull gRPC"},
	"redis":            {summary: "REST, gRPC (client default)", grpc: "google.cloud.redis.v1.CloudRedis"},
	"resourcemanager":  {summary: "v3 REST, gRPC; v1 REST", restOnly: []string{"resourcemanager."}},
	"run":              {summary: "REST, gRPC"},
	"secretmanager":    {summary: "REST, gRPC"},
	"spanner":          {summary: "REST, gRPC", grpc: "google.longrunning.Operations"},
	// google.storage.v2 has no notification, HMAC key, service account,
	// channel, or ACL RPCs, and no copy; the XML API is REST.
	"storage": {summary: "JSON and XML REST; gRPC v2", grpc: "google.storage.v2.Storage and google.storage.control.v2.StorageControl", restOnly: []string{
		"storage.xml.", "storage.notifications.", "storage.projects.", "storage.channels.", "storage.objects.copy",
		"storage.bucketAccessControls.", "storage.defaultObjectAccessControls.", "storage.objectAccessControls.",
	}},
	"vertexai": {summary: "REST, gRPC", grpc: "google.cloud.aiplatform.v1.PredictionService and LlmUtilityService"},
}

// mixins are served per method; their other methods are not the service's.
var mixins = []string{"google.iam.v1.IAMPolicy", "google.longrunning.Operations", "google.cloud.location.Locations"}

// grpcOnlyAtGCP are protos Google serves on gRPC only, whatever their HTTP
// annotations say: Bigtable's data API publishes gRPC clients only.
var grpcOnlyAtGCP = []string{"google.bigtable.v2.Bigtable"}

// row is one service's line in the matrix.
type row struct {
	service, gcp               string
	rest, grpc                 int
	restOnly, grpcOnly, gcpOne []string
	allREST                    bool
}

// TestTransportMatrix derives each service's per-method transport gaps from
// the methods its provider registers and from Google's protos, and requires
// docs/architecture/transports.md to match. It needs every backend; run it
// through scripts/check/recordings.sh, or add -update-transports to rewrite.
func TestTransportMatrix(t *testing.T) {
	rows := matrix(t, served(assembleAll(t)))
	want := render(rows)
	document, err := os.ReadFile(transportsDoc)
	if err != nil {
		t.Fatal(err)
	}
	section := regexp.MustCompile(`(?s)<!-- matrix:begin -->\n.*<!-- matrix:end -->`)
	replaced := section.ReplaceAllLiteral(document, []byte("<!-- matrix:begin -->\n"+want+"<!-- matrix:end -->"))
	if *updateTransports {
		if err := os.WriteFile(transportsDoc, replaced, 0o600); err != nil { //nolint:gosec // a fixed repository path
			t.Fatal(err)
		}
		return
	}
	if string(replaced) != string(document) {
		t.Fatalf("docs/architecture/transports.md is stale; rerun with -update-transports. Want:\n%s", want)
	}
}

// assembleAll registers every provider with the backends of
// scripts/run/backend_env.sh, as the conformance server does.
func assembleAll(t *testing.T) *gateway.Gateway {
	t.Helper()
	env := map[string]string{}
	for _, name := range []string{"GCP_LOCAL_JAVA", "GCP_LOCAL_PUBSUB_JAR", "GCP_LOCAL_FIRESTORE_JAR", "GCP_LOCAL_SPANNER_EMULATOR",
		"GCP_LOCAL_SPANNER_LIBRARIES", "GCP_LOCAL_REDIS_SERVER", "GCP_LOCAL_POSTGRES_ROOT", "GCP_LOCAL_MYSQL_ROOT"} {
		env[name] = os.Getenv(name)
		if env[name] == "" || runtime.GOOS != "linux" {
			t.Skip("needs every backend on Linux: run scripts/check/recordings.sh")
		}
	}
	options := serveOptions{listen: "127.0.0.1:4566", pubsubJava: env["GCP_LOCAL_JAVA"], pubsubJAR: env["GCP_LOCAL_PUBSUB_JAR"],
		firestoreJava: env["GCP_LOCAL_JAVA"], firestoreJAR: env["GCP_LOCAL_FIRESTORE_JAR"],
		spannerEmulator: env["GCP_LOCAL_SPANNER_EMULATOR"], spannerLibraries: env["GCP_LOCAL_SPANNER_LIBRARIES"],
		redisServer: env["GCP_LOCAL_REDIS_SERVER"]}
	options.cloudsql = cloudSQLOptions{postgres: env["GCP_LOCAL_POSTGRES_ROOT"], mysql: env["GCP_LOCAL_MYSQL_ROOT"]}
	if uid := os.Getenv("GCP_LOCAL_POSTGRES_UID"); uid != "" {
		options.cloudsql.runAs = uid + ":" + os.Getenv("GCP_LOCAL_POSTGRES_GID")
	}
	selected := map[string]bool{}
	for _, service := range []string{"storage", "pubsub", "kms", "firestore", "bigquery", "spanner", "bigtable", "artifactregistry",
		"resourcemanager", "orgpolicy", "iam", "logging", "vertexai", "cloudtasks", "cloudscheduler", "secretmanager", "redis",
		"cloudsql", "compute", "run"} {
		selected[service] = true
	}
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	registry := gateway.NewRegistry()
	g := gateway.New(registry, nil)
	server := core.WithGateway("test", time.Second, g)
	if err := assembleServices(context.Background(), options, selected, registry, server, index, engine, &identity.Service{}); err != nil {
		t.Fatal(err)
	}
	return g
}

// served drops the routes a provider registers only to refuse: Storage's
// unserved JSON methods (400, docs/services/storage.md#gaps) and Compute's
// catch-all.
func served(g *gateway.Gateway) []gateway.ServedMethod {
	return slices.DeleteFunc(g.Served(), func(m gateway.ServedMethod) bool {
		return m.Method == "compute.unmodeled" || (m.Service == "storage" && !slices.Contains(storage.RegisteredMethods, m.Method))
	})
}

// binding is one google.api.http rule: a verb and its path template as a
// regular expression.
type binding struct {
	verb string
	path *regexp.Regexp
}

var variable = regexp.MustCompile(`\{([^}=]+)(=([^}]*))?\}`)

// template turns a proto path template into an anchored expression: * is one
// segment and ** one or more.
func template(path string) *regexp.Regexp {
	pattern := regexp.QuoteMeta(variable.ReplaceAllStringFunc(path, func(match string) string {
		inner := variable.FindStringSubmatch(match)[3]
		if inner == "" {
			inner = "*"
		}
		return inner
	}))
	pattern = strings.ReplaceAll(strings.ReplaceAll(pattern, `\*\*`, ".+"), `\*`, "[^/]+")
	return regexp.MustCompile("^" + pattern + "$")
}

// samples fill a route's variables with values: one segment each, and one to
// three for a multi-segment {name=**} variable.
func samples(path string) []string {
	filled := make([]string, 0, 3)
	for _, depth := range []string{"s", "s/s", "s/s/s"} {
		filled = append(filled, variable.ReplaceAllStringFunc(path, func(match string) string {
			if strings.Contains(match, "**") {
				return depth
			}
			return "s"
		}))
	}
	return filled
}

func bindings(method protoreflect.MethodDescriptor) []binding {
	rule, _ := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
	if rule == nil {
		return nil
	}
	var found []binding
	for _, r := range append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...) {
		for verb, path := range map[string]string{"GET": r.GetGet(), "POST": r.GetPost(), "PUT": r.GetPut(), "PATCH": r.GetPatch(), "DELETE": r.GetDelete()} {
			if path != "" {
				found = append(found, binding{verb, template(path)})
			}
		}
	}
	return found
}

// protoMethods returns every method of the protos whose methods the service
// serves over gRPC, by gRPC name; of a mixin, only the served methods.
func protoMethods(t *testing.T, grpc map[string]gateway.ServedMethod) map[string]protoreflect.MethodDescriptor {
	t.Helper()
	methods := map[string]protoreflect.MethodDescriptor{}
	for name := range grpc {
		service, rpc, _ := strings.Cut(strings.TrimPrefix(name, "/"), "/")
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
		owner, ok := descriptor.(protoreflect.ServiceDescriptor)
		if err != nil || !ok {
			t.Fatalf("%s: no service descriptor: %v", name, err)
		}
		if slices.Contains(mixins, service) {
			methods[name] = owner.Methods().ByName(protoreflect.Name(rpc))
			continue
		}
		for i := range owner.Methods().Len() {
			each := owner.Methods().Get(i)
			methods["/"+service+"/"+string(each.Name())] = each
		}
	}
	return methods
}

func matrix(t *testing.T, served []gateway.ServedMethod) []row {
	t.Helper()
	byService := map[string][]gateway.ServedMethod{}
	for _, m := range served {
		byService[m.Service] = append(byService[m.Service], m)
	}
	rows := make([]row, 0, len(byService))
	for service, methods := range byService {
		gcp, ok := gcpTransports[service]
		if !ok {
			t.Fatalf("%s: say what Google serves in gcpTransports", service)
		}
		rows = append(rows, serviceRow(t, service, gcp, methods))
	}
	slices.SortFunc(rows, func(a, b row) int { return strings.Compare(a.service, b.service) })
	return rows
}

// split separates a service's served gRPC methods from its REST routes, given
// as "VERB sample-path" strings, and marks the routes a transcoded gRPC method
// runs through as paired.
func split(methods []gateway.ServedMethod) (map[string]gateway.ServedMethod, map[string][]string, map[string]bool) {
	grpc, rest, paired := map[string]gateway.ServedMethod{}, map[string][]string{}, map[string]bool{}
	for _, m := range methods {
		if m.GRPC {
			grpc[m.Method] = m
			for _, id := range m.Routes {
				paired[id] = true
			}
			continue
		}
		for _, route := range m.Routes {
			verb, path, _ := strings.Cut(route, " ")
			for _, filled := range samples(path) {
				rest[m.Method] = append(rest[m.Method], verb+" "+filled)
			}
		}
	}
	return grpc, rest, paired
}

// routesFor returns the REST routes whose sample paths match one of the
// method's HTTP bindings. A service config may rebind a mixin method's verb
// (Pub/Sub serves GetIamPolicy on GET), so mixins match on the path alone.
func routesFor(descriptor protoreflect.MethodDescriptor, rest map[string][]string) []string {
	mixin := slices.Contains(mixins, string(descriptor.Parent().FullName()))
	var matched []string
	for id, routes := range rest {
		for _, b := range bindings(descriptor) {
			if slices.ContainsFunc(routes, func(route string) bool {
				verb, path, _ := strings.Cut(route, " ")
				return (mixin || verb == b.verb) && b.path.MatchString(path)
			}) && !slices.Contains(matched, id) {
				matched = append(matched, id)
			}
		}
	}
	return matched
}

// classify files one proto method under a gap or a Google one-transport method.
func (r *row) classify(descriptor protoreflect.MethodDescriptor, onGRPC, onREST bool) {
	owner := string(descriptor.Parent().FullName())
	restAtGCP := len(bindings(descriptor)) > 0 && !descriptor.IsStreamingClient() && !slices.Contains(grpcOnlyAtGCP, owner)
	short := owner[strings.LastIndex(owner, ".")+1:] + "." + string(descriptor.Name())
	switch {
	case onGRPC && !onREST && restAtGCP:
		r.grpcOnly = append(r.grpcOnly, short)
	case onGRPC && !onREST:
		r.gcpOne = append(r.gcpOne, short+" (gRPC)")
	case onREST && !onGRPC:
		r.restOnly = append(r.restOnly, short)
	}
}

// unpaired files a REST route no proto method pairs. Google serves the
// locations mixin (google.cloud.location.Locations) on gRPC wherever it serves
// the API on gRPC.
func (r *row) unpaired(id string, gcp gcpTransport) {
	locations := strings.Contains(id, ".locations.") && strings.Count(id, ".") == 2
	restOnly := gcp.grpc == "" || slices.ContainsFunc(gcp.restOnly, func(prefix string) bool { return strings.HasPrefix(id, prefix) })
	if restOnly && !locations {
		r.gcpOne = append(r.gcpOne, id+" (REST)")
		return
	}
	r.restOnly = append(r.restOnly, id)
}

func serviceRow(t *testing.T, service string, gcp gcpTransport, methods []gateway.ServedMethod) row {
	t.Helper()
	r := row{service: service, gcp: gcp.summary}
	grpc, rest, paired := split(methods)
	r.rest, r.grpc = len(rest), len(grpc)
	for name, descriptor := range protoMethods(t, grpc) {
		matched := routesFor(descriptor, rest)
		for _, id := range matched {
			paired[id] = true
		}
		served, onGRPC := grpc[name]
		r.classify(descriptor, onGRPC, len(matched) > 0 || len(served.Routes) > 0)
	}
	unpaired := 0
	for id := range rest {
		if !paired[id] {
			unpaired++
			r.unpaired(id, gcp)
		}
	}
	r.allREST = r.grpc == 0 && len(r.restOnly) == unpaired && unpaired > 0
	for _, list := range [][]string{r.restOnly, r.grpcOnly, r.gcpOne} {
		slices.Sort(list)
	}
	return r
}

func render(rows []row) string {
	var b strings.Builder
	b.WriteString("| Service | Google serves | REST routes | gRPC methods | Gap: served on REST only | Gap: served on gRPC only |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	for _, r := range rows {
		restOnly := strings.Join(r.restOnly, ", ")
		if r.allREST {
			restOnly = fmt.Sprintf("all %d", len(r.restOnly))
		}
		fmt.Fprintf(&b, "| `%s` | %s | %d | %d | %s | %s |\n", r.service, r.gcp, r.rest, r.grpc, orNone(restOnly), orNone(strings.Join(r.grpcOnly, ", ")))
	}
	b.WriteString("\nServed on one transport because Google serves only that one (not gaps):\n\n")
	for _, r := range rows {
		if len(r.gcpOne) > 0 {
			fmt.Fprintf(&b, "- `%s`: %s\n", r.service, strings.Join(r.gcpOne, ", "))
		}
	}
	return b.String()
}

func orNone(text string) string {
	if text == "" {
		return "none"
	}
	return text
}
