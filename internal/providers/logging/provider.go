// Package logging implements a Cloud Logging v2 slice: entries write and
// list, logs list and delete, over REST and gRPC. Entries stay in provider
// state, scoped per project, and reset with the scope. No cloud recording
// covers these methods.
package logging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// serviceName is the provider and backend registry name.
const serviceName = "logging"

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Logging")

// Method identifiers: REST discovery IDs and gRPC full method names.
const (
	restWrite      = "logging.entries.write"
	restList       = "logging.entries.list"
	restLogsList   = "logging.projects.logs.list"
	restLogsDelete = "logging.projects.logs.delete"
	rpcWrite       = "/google.logging.v2.LoggingServiceV2/WriteLogEntries"
	rpcList        = "/google.logging.v2.LoggingServiceV2/ListLogEntries"
	rpcLogsList    = "/google.logging.v2.LoggingServiceV2/ListLogs"
	rpcLogsDelete  = "/google.logging.v2.LoggingServiceV2/DeleteLog"
)

// Permissions follow Google's Logging access-control table: every method is
// checked on the project that owns the entries.
const (
	permissionWrite      = "logging.logEntries.create"
	permissionList       = "logging.logEntries.list"
	permissionLogsList   = "logging.logs.list"
	permissionLogsDelete = "logging.logs.delete"
)

// permissions binds each served method to its single project permission.
var permissions = map[string]string{
	restWrite: permissionWrite, rpcWrite: permissionWrite,
	restList: permissionList, rpcList: permissionList,
	restLogsList: permissionLogsList, rpcLogsList: permissionLogsList,
	restLogsDelete: permissionLogsDelete, rpcLogsDelete: permissionLogsDelete,
}

// Limits of the local model.
const (
	// maxEntriesPerProject bounds memory; the oldest entries are dropped first.
	maxEntriesPerProject = 10000
	defaultEntryPage     = 50
	maxEntryPage         = 1000
	defaultLogPage       = 1000
)

// Provider owns the per-project log entry store.
type Provider struct {
	index  *state.Index
	engine *iam.Engine
	scopes *state.Store[scopeState]
	clock  func() time.Time
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("logging requires registry, manager, index, and iam")

// New registers the Cloud Logging provider for its REST and gRPC methods.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	p := &Provider{index: index, engine: engine, scopes: state.NewStore(newScopeState, state.GlobalScope), clock: time.Now}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      provider.NoBackend,
		AfterReset: func(scope state.Scope) error {
			if err := p.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the Logging backend: %w", err)
	}
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// Reset clears one project's entries.
func (p *Provider) Reset(scope state.Scope) error {
	if err := p.scopes.With(scope, func(s *scopeState) error {
		*s = newScopeState()
		return nil
	}); err != nil {
		return fmt.Errorf("clear the logging scope: %w", err)
	}
	return nil
}

// storedEntry is one written entry. The entry's LogName is cleared; the
// project and unescaped log ID own the name.
type storedEntry struct {
	logID string
	seq   uint64
	entry *loggingpb.LogEntry
}

// scopeState is one project's entries in write order.
type scopeState struct {
	entries []storedEntry
	seq     uint64
}

func newScopeState() scopeState { return scopeState{} }

// handle is the gateway entry point for REST and gRPC calls.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	switch request := raw.(type) {
	case *gateway.RESTRequest:
		return p.handleREST(call, request)
	case proto.Message:
		return p.handleRPC(call, request)
	default:
		return nil, status.Error(codes.Internal, "Invalid Logging request")
	}
}

// handleRPC dispatches one gRPC request.
func (p *Provider) handleRPC(call gateway.Call, raw proto.Message) (proto.Message, error) {
	switch request := raw.(type) {
	case *loggingpb.WriteLogEntriesRequest:
		return p.write(call, request)
	case *loggingpb.ListLogEntriesRequest:
		return p.list(call, request)
	case *loggingpb.ListLogsRequest:
		return p.listLogs(call, request)
	case *loggingpb.DeleteLogRequest:
		if err := p.deleteLog(call, request); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	default:
		return nil, status.Error(codes.Internal, "Invalid Logging request type")
	}
}

// restMessage builds the protobuf request of one REST call.
func restMessage(call gateway.Call, request *gateway.RESTRequest) (proto.Message, error) {
	switch call.Method {
	case restWrite:
		return &loggingpb.WriteLogEntriesRequest{}, nil
	case restList:
		return &loggingpb.ListLogEntriesRequest{}, nil
	case restLogsList:
		return logsRequest(request)
	case restLogsDelete:
		return &loggingpb.DeleteLogRequest{LogName: "projects/" + request.Path["project"] + "/logs/" + escapeLogID(request.Path["log"])}, nil
	default:
		return nil, gap("this method is not implemented")
	}
}

// handleREST decodes one REST request into its protobuf form, runs it, and
// renders the protobuf JSON response.
func (p *Provider) handleREST(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	message, err := restMessage(call, request)
	if err != nil {
		return nil, err
	}
	if request.JSON != nil {
		if err := protojson.Unmarshal(request.JSON, message); err != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid JSON payload received: "+err.Error())
		}
	}
	response, err := p.handleRPC(call, message)
	if err != nil {
		return nil, err
	}
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return nil, status.Error(codes.Internal, "Response encoding failed")
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded), Headers: http.Header{}}, nil
}

// logsRequest reads the logs.list query parameters.
func logsRequest(request *gateway.RESTRequest) (*loggingpb.ListLogsRequest, error) {
	listing := &loggingpb.ListLogsRequest{Parent: "projects/" + request.Path["project"], PageToken: request.Query.Get("pageToken"), ResourceNames: request.Query["resourceNames"]}
	if size := request.Query.Get("pageSize"); size != "" {
		value, err := strconv.ParseInt(size, 10, 32)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid pageSize")
		}
		listing.PageSize = int32(value)
	}
	return listing, nil
}

// projectPattern pins the project identifier shape.
var projectPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// project resolves a "projects/{id}" resource name to a project ID, mapping a
// project number the index knows.
func (p *Provider) project(name string) (string, error) {
	id, ok := strings.CutPrefix(name, "projects/")
	if !ok || strings.Contains(id, "/") {
		if strings.HasPrefix(name, "organizations/") || strings.HasPrefix(name, "folders/") || strings.HasPrefix(name, "billingAccounts/") {
			return "", gap("only project resource names are supported")
		}
		return "", status.Errorf(codes.InvalidArgument, "Invalid resource name %q", name)
	}
	if !projectPattern.MatchString(id) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid project %q", id)
	}
	if resource, found := p.index.ProjectByNumber(id); found {
		return resource.Scope.Project, nil
	}
	return id, nil
}

// authorize checks the method's permission on every named project before any
// state is read or written.
func (p *Provider) authorize(call gateway.Call, projects []string) error {
	for _, project := range projects {
		scoped := call
		scoped.Project = project
		scoped.Resource = provider.ProjectResource(project)
		_, err := p.engine.Authorize(scoped, iam.Target{Name: provider.ProjectResource(project)}, nil)
		if err == nil {
			continue
		}
		switch status.Code(err) {
		case codes.PermissionDenied:
			return status.Errorf(codes.PermissionDenied, "Permission '%s' denied on resource 'projects/%s' (or it may not exist).", permissions[call.Method], project)
		case codes.Unauthenticated:
			return status.Error(codes.Unauthenticated, "Request is missing required authentication credential. Expected OAuth 2 access token, login cookie or other valid authentication credential.")
		default:
			return fmt.Errorf("authorize %s: %w", call.Method, err)
		}
	}
	return nil
}
