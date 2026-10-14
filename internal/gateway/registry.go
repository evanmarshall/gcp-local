// Package gateway separates transport contracts from provider behavior.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/evanmarshall/gcp-local/api/generated/artifactregistry"
	"github.com/evanmarshall/gcp-local/api/generated/compute"
	"github.com/evanmarshall/gcp-local/api/generated/contract"
	"github.com/evanmarshall/gcp-local/api/generated/databases/bigquery"
	"github.com/evanmarshall/gcp-local/api/generated/databases/bigtable"
	"github.com/evanmarshall/gcp-local/api/generated/databases/cloudsql"
	"github.com/evanmarshall/gcp-local/api/generated/databases/firestore"
	"github.com/evanmarshall/gcp-local/api/generated/databases/memorystore"
	"github.com/evanmarshall/gcp-local/api/generated/databases/spanner"
	"github.com/evanmarshall/gcp-local/api/generated/logging"
	"github.com/evanmarshall/gcp-local/api/generated/messaging/cloudscheduler"
	"github.com/evanmarshall/gcp-local/api/generated/messaging/cloudtasks"
	"github.com/evanmarshall/gcp-local/api/generated/messaging/pubsub"
	"github.com/evanmarshall/gcp-local/api/generated/mixin"
	"github.com/evanmarshall/gcp-local/api/generated/run"
	"github.com/evanmarshall/gcp-local/api/generated/security/cloudkms"
	"github.com/evanmarshall/gcp-local/api/generated/security/iam"
	"github.com/evanmarshall/gcp-local/api/generated/security/orgpolicy"
	"github.com/evanmarshall/gcp-local/api/generated/security/resourcemanager"
	"github.com/evanmarshall/gcp-local/api/generated/security/secretmanager"
	"github.com/evanmarshall/gcp-local/api/generated/storage"
	"github.com/evanmarshall/gcp-local/api/generated/vertexai"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Registration sentinels; every wrapper keeps the method name for diagnosis.
var (
	errUnaryRegistration  = errors.New("invalid unary registration")
	errStreamRegistration = errors.New("invalid stream registration")
	errDuplicateMethod    = errors.New("duplicate registration")
)

type PrincipalState string

const (
	servicePubsub                 = "pubsub"
	serviceStorage                = "storage"
	Anonymous      PrincipalState = "anonymous"
	Unknown        PrincipalState = "unknown"
	Authenticated  PrincipalState = "authenticated"
	Expired        PrincipalState = "expired"
)

type Principal struct {
	State   PrincipalState `json:"state"`
	Subject string         `json:"subject,omitempty"`
	Project string         `json:"project,omitempty"`
	Scopes  []string       `json:"scopes,omitempty"`
}

// Call is request-scoped. Project/location are resource scope, not caller scope.
// A missing project (for example a globally named bucket) awaits provider lookup.
type Call struct {
	Context                                      context.Context //nolint:containedctx // a Call carries one request context by design
	Method, Project, Location, Resource, TraceID string
	Principal                                    Principal
}
type Handler func(Call, any) (any, error)
type Stream interface {
	Context() context.Context
	Recv(proto.Message) error
	Send(proto.Message) error
}
type StreamHandler func(Call, Stream) error
type methodInfo struct {
	service   string
	streaming bool
}

// Registry is explicit and safe for concurrent dispatch. Registration grants no
// permissions: each provider must authorize its Call before performing effects.
type Registry struct {
	mu        sync.RWMutex
	contracts map[string]methodInfo
	unary     map[string]Handler
	stream    map[string]StreamHandler
}

func NewRegistry() *Registry {
	r := &Registry{contracts: map[string]methodInfo{}, unary: map[string]Handler{}, stream: map[string]StreamHandler{}}
	r.registerRoutes()
	r.registerPubSub()
	r.registerData("spanner", spanner.SpannerMethods)
	r.registerData("bigtable", bigtable.BigtableMethods)
	// Transcoded methods dispatch to their REST route's handler; the gRPC
	// contracts name the service for its untranscoded methods' gaps.
	r.registerData("secretmanager", secretmanager.SecretManagerMethods)
	r.registerData("cloudtasks", cloudtasks.CloudTasksMethods)
	r.registerData("cloudscheduler", cloudscheduler.CloudSchedulerMethods)
	r.registerData("cloudkms", cloudkms.CloudKMSMethods)
	r.registerData("run", run.RunServicesMethods)
	r.registerData("run", run.RunRevisionsMethods)
	r.registerData("spanner", spanner.SpannerInstanceAdminMethods)
	r.registerData("spanner", spanner.SpannerDatabaseAdminMethods)
	r.registerData("bigtable", bigtable.BigtableInstanceAdminMethods)
	r.registerData("bigtable", bigtable.BigtableTableAdminMethods)
	r.registerData("iam", iam.IAMAdminMethods)
	r.registerData("iam", iam.IAMCredentialsMethods)
	r.registerData("resourcemanager", resourcemanager.FoldersMethods)
	r.registerData("resourcemanager", resourcemanager.OrganizationsMethods)
	r.registerData("resourcemanager", resourcemanager.ProjectsMethods)
	r.registerData("orgpolicy", orgpolicy.OrgPolicyMethods)
	return r
}

// registerRoutes binds every generated REST route table to its service.
func (r *Registry) registerRoutes() {
	tables := []struct {
		routes  []contract.Route
		service string
	}{
		{storage.StorageRoutes, serviceStorage},
		{storage.StorageXMLRoutes, serviceStorage},
		{run.RunRoutes, "run"},
		{compute.ComputeRoutes, "compute"},
		{secretmanager.SecretManagerRoutes, "secretmanager"},
		{cloudkms.CloudKMSRoutes, "cloudkms"},
		{firestore.FirestoreRoutes, "firestore"},
		{bigquery.BigQueryRoutes, "bigquery"},
		{spanner.SpannerRoutes, "spanner"},
		{bigtable.BigtableRoutes, "bigtable"},
		{artifactregistry.ArtifactRegistryRoutes, "artifactregistry"},
		{resourcemanager.ResourceManagerRoutes, "resourcemanager"},
		{iam.IAMRoutes, "iam"},
		{orgpolicy.OrgPolicyRoutes, "orgpolicy"},
		{cloudtasks.CloudTasksRoutes, "cloudtasks"},
		{cloudscheduler.CloudSchedulerRoutes, "cloudscheduler"},
		{cloudsql.CloudSQLRoutes, "cloudsql"},
		{pubsub.PubSubRestRoutes, servicePubsub},
		{pubsub.PubSubSchemaRestRoutes, servicePubsub},
		{memorystore.RedisRoutes, "redis"},
		{vertexai.VertexAIRoutes, "vertexai"},
		{logging.LoggingRoutes, "logging"},
	}
	for _, table := range tables {
		for _, route := range table.routes {
			r.contracts[route.Method] = methodInfo{service: table.service}
		}
	}
}

// registerPubSub binds the streaming gRPC surfaces. Pub/Sub is the current
// implementation owner for IAMPolicy gRPC; its handler rejects other service
// resource names instead of forwarding them.
func (r *Registry) registerPubSub() {
	for _, rpc := range pubsub.PubSubMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub", streaming: rpc.Streaming}
	}
	for _, rpc := range mixin.IAMMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub"}
	}
	// Firestore server-streaming RPCs register as streams like bidirectional
	// ones; the gateway hands them the decoded request.
	for _, rpc := range firestore.FirestoreMethods {
		r.contracts[rpc.Method] = methodInfo{service: "firestore", streaming: rpc.Streaming}
	}
	for _, rpc := range logging.LoggingMethods {
		r.contracts[rpc.Method] = methodInfo{service: "logging", streaming: rpc.Streaming}
	}
}

// registerData binds one service's gRPC data-plane contracts.
func (r *Registry) registerData(service string, rpcs []contract.RPC) {
	for _, rpc := range rpcs {
		r.contracts[rpc.Method] = methodInfo{service: service, streaming: rpc.Streaming}
	}
}
func (r *Registry) Register(method string, handler Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.contracts[method]
	if !ok || info.streaming || handler == nil {
		return fmt.Errorf("%w: %s", errUnaryRegistration, method)
	}
	if r.unary[method] != nil {
		return fmt.Errorf("%w: %s", errDuplicateMethod, method)
	}
	r.unary[method] = handler
	return nil
}
func (r *Registry) RegisterStream(method string, handler StreamHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.contracts[method]
	if !ok || !info.streaming || handler == nil {
		return fmt.Errorf("%w: %s", errStreamRegistration, method)
	}
	if r.stream[method] != nil {
		return fmt.Errorf("%w: %s", errDuplicateMethod, method)
	}
	r.stream[method] = handler
	return nil
}
func (r *Registry) Invoke(call Call, request any) (any, error) {
	if err := call.Context.Err(); err != nil {
		return nil, normalizeError(err)
	}
	r.mu.RLock()
	handler := r.unary[call.Method]
	r.mu.RUnlock()
	if handler == nil {
		return nil, r.missing(call.Method)
	}
	result, err := handler(call, request)
	return result, normalizeError(err)
}
func (r *Registry) streamHandler(method string) (StreamHandler, error) {
	r.mu.RLock()
	handler := r.stream[method]
	r.mu.RUnlock()
	if handler == nil {
		return nil, r.missing(method)
	}
	return handler, nil
}
func (r *Registry) Providers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]bool{}
	for method := range r.unary {
		seen[r.contracts[method].service] = true
	}
	for method := range r.stream {
		seen[r.contracts[method].service] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ServedMethod is one method a provider serves on one transport: a REST route
// ID with its "VERB path" bindings, or a gRPC method name with the REST
// routes it is transcoded onto, if any.
type ServedMethod struct {
	Service, Method string
	GRPC            bool
	Routes          []string
}

// Served lists every method a provider serves, sorted by service and method:
// REST routes and gRPC methods with a handler, and gRPC methods transcoded
// onto a route with a handler, under that route's service. The transport
// matrix check reads it (docs/architecture/transports.md).
func (g *Gateway) Served() []ServedMethod {
	r := g.Registry
	r.mu.RLock()
	defer r.mu.RUnlock()
	served := append(r.handledMethods(g.routes), g.transcodedMethods()...)
	sort.Slice(served, func(i, j int) bool {
		if served[i].Service != served[j].Service {
			return served[i].Service < served[j].Service
		}
		return served[i].Method < served[j].Method
	})
	return served
}

// handled reports whether a method has a handler. The caller holds the lock.
func (r *Registry) handled(method string) bool {
	return r.unary[method] != nil || r.stream[method] != nil
}

// handledMethods lists the gRPC methods and REST routes with a handler. The
// caller holds the lock.
func (r *Registry) handledMethods(routes []compiledRoute) []ServedMethod {
	var served []ServedMethod
	for method, info := range r.contracts {
		if r.handled(method) && strings.HasPrefix(method, "/") {
			served = append(served, ServedMethod{Service: info.service, Method: method, GRPC: true})
		}
	}
	paths := map[string][]string{}
	for _, route := range routes {
		if r.handled(route.contract.Method) {
			paths[route.contract.Method] = append(paths[route.contract.Method], route.contract.Verb+" "+route.contract.Path)
		}
	}
	for method, bindings := range paths {
		served = append(served, ServedMethod{Service: r.contracts[method].service, Method: method, Routes: bindings})
	}
	return served
}

// transcodedMethods lists the gRPC methods transcoded onto a handled route,
// under each route's service. The caller holds the registry lock.
func (g *Gateway) transcodedMethods() []ServedMethod {
	served := make([]ServedMethod, 0, len(g.transcoded))
	for method, bindings := range g.transcoded {
		routes := map[string][]string{}
		for _, binding := range bindings {
			if g.Registry.handled(binding.REST) {
				service := g.Registry.contracts[binding.REST].service
				routes[service] = append(routes[service], binding.REST)
			}
		}
		for service, ids := range routes {
			served = append(served, ServedMethod{Service: service, Method: method, GRPC: true, Routes: ids})
		}
	}
	return served
}

// missing explains a method with no handler: a service this server was not
// started with, or a method its provider does not implement.
func (r *Registry) missing(method string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	service := r.contracts[method].service
	if service != "" && !r.serves(service) {
		flag := service
		if service == "cloudkms" {
			flag = "kms" // the -services spelling
		}
		return status.Errorf(codes.Unimplemented, "Service %s is not enabled on this server; add %s to -services", service, flag)
	}
	return gap(method)
}

// serves reports whether any handler of the service is registered. The
// caller holds the lock.
func (r *Registry) serves(service string) bool {
	for method := range r.unary {
		if r.contracts[method].service == service {
			return true
		}
	}
	for method := range r.stream {
		if r.contracts[method].service == service {
			return true
		}
	}
	return false
}

func gap(method string) error {
	return status.Error(codes.Unimplemented, "No provider implements "+method)
}
