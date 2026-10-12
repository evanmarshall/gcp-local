// Package gateway separates transport contracts from provider behavior.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/evanmarshall/gcp-local/api/generated"
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
	servicePubsub                = "pubsub"
	Anonymous     PrincipalState = "anonymous"
	Unknown       PrincipalState = "unknown"
	Authenticated PrincipalState = "authenticated"
	Expired       PrincipalState = "expired"
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
	r.registerData("spanner", generated.SpannerMethods)
	r.registerData("bigtable", generated.BigtableMethods)
	// Transcoded methods dispatch to their REST route's handler; the gRPC
	// contracts name the service for its untranscoded methods' gaps.
	r.registerData("secretmanager", generated.SecretManagerMethods)
	r.registerData("cloudtasks", generated.CloudTasksMethods)
	r.registerData("cloudkms", generated.CloudKMSMethods)
	return r
}

// registerRoutes binds every generated REST route table to its service.
func (r *Registry) registerRoutes() {
	tables := []struct {
		routes  []generated.Route
		service string
	}{
		{generated.StorageRoutes, "storage"},
		{generated.StorageXMLRoutes, "storage"},
		{generated.RunRoutes, "run"},
		{generated.ComputeRoutes, "compute"},
		{generated.SecretManagerRoutes, "secretmanager"},
		{generated.CloudKMSRoutes, "cloudkms"},
		{generated.FirestoreRoutes, "firestore"},
		{generated.BigQueryRoutes, "bigquery"},
		{generated.SpannerRoutes, "spanner"},
		{generated.BigtableRoutes, "bigtable"},
		{generated.ArtifactRegistryRoutes, "artifactregistry"},
		{generated.ResourceManagerRoutes, "resourcemanager"},
		{generated.IAMRoutes, "iam"},
		{generated.CloudTasksRoutes, "cloudtasks"},
		{generated.CloudSQLRoutes, "cloudsql"},
		{generated.PubSubRestRoutes, servicePubsub},
		{generated.RedisRoutes, "redis"},
		{generated.VertexAIRoutes, "vertexai"},
		{generated.LoggingRoutes, "logging"},
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
	for _, rpc := range generated.PubSubMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub", streaming: rpc.Streaming}
	}
	for _, rpc := range generated.IAMMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub"}
	}
	// Firestore server-streaming RPCs register as streams like bidirectional
	// ones; the gateway hands them the decoded request.
	for _, rpc := range generated.FirestoreMethods {
		r.contracts[rpc.Method] = methodInfo{service: "firestore", streaming: rpc.Streaming}
	}
	for _, rpc := range generated.LoggingMethods {
		r.contracts[rpc.Method] = methodInfo{service: "logging", streaming: rpc.Streaming}
	}
}

// registerData binds one service's gRPC data-plane contracts.
func (r *Registry) registerData(service string, rpcs []generated.RPC) {
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
