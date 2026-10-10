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
	for _, route := range generated.StorageRoutes {
		r.contracts[route.Method] = methodInfo{service: "storage"}
	}
	for _, route := range generated.RunRoutes {
		r.contracts[route.Method] = methodInfo{service: "run"}
	}
	for _, route := range generated.ComputeRoutes {
		r.contracts[route.Method] = methodInfo{service: "compute"}
	}
	for _, rpc := range generated.PubSubMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub", streaming: rpc.Streaming}
	}
	// Pub/Sub is the current implementation owner for IAMPolicy gRPC. Its
	// handler rejects other service resource names instead of forwarding them.
	for _, rpc := range generated.IAMMethods {
		r.contracts[rpc.Method] = methodInfo{service: "pubsub"}
	}
	return r
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
		return nil, gap(call.Method)
	}
	result, err := handler(call, request)
	return result, normalizeError(err)
}
func (r *Registry) streamHandler(method string) (StreamHandler, error) {
	r.mu.RLock()
	handler := r.stream[method]
	r.mu.RUnlock()
	if handler == nil {
		return nil, gap(method)
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
func gap(method string) error {
	return status.Error(codes.Unimplemented, "No provider implements "+method)
}
