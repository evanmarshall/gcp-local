package bigtable

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/evanmarshall/gcp-local/api/generated"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Bigtable data-plane gRPC methods (google.bigtable.v2.Bigtable).
const (
	dataService           = "/google.bigtable.v2.Bigtable/"
	rpcReadRows           = dataService + "ReadRows"
	rpcSampleRowKeys      = dataService + "SampleRowKeys"
	rpcMutateRow          = dataService + "MutateRow"
	rpcMutateRows         = dataService + "MutateRows"
	rpcCheckAndMutateRow  = dataService + "CheckAndMutateRow"
	rpcReadModifyWriteRow = dataService + "ReadModifyWriteRow"
	rpcPingAndWarm        = dataService + "PingAndWarm"
)

// Data-plane permissions.
const (
	permReadRows   = "bigtable.tables.readRows"
	permMutateRows = "bigtable.tables.mutateRows"
)

// dataPermissions follows Bigtable's access-control permission table:
// conditional and read-modify-write operations need readRows besides
// mutateRows, and PingAndWarm needs bigtable.instances.ping on the instance.
var dataPermissions = map[string][]string{
	rpcReadRows:           {permReadRows},
	rpcSampleRowKeys:      {"bigtable.tables.sampleRowKeys"},
	rpcMutateRow:          {permMutateRows},
	rpcMutateRows:         {permMutateRows},
	rpcCheckAndMutateRow:  {permMutateRows, permReadRows},
	rpcReadModifyWriteRow: {permMutateRows, permReadRows},
	rpcPingAndWarm:        {"bigtable.instances.ping"},
}

// errDataPlan reports a generated data-plane method without a permission plan.
var errDataPlan = errors.New("bigtable data-plane method has no permission plan")

// dataRPCs indexes the generated data-plane contracts by method.
var dataRPCs = func() map[string]generated.RPC {
	rpcs := map[string]generated.RPC{}
	for _, rpc := range generated.BigtableMethods {
		rpcs[rpc.Method] = rpc
	}
	return rpcs
}()

// registerData binds the data-plane permission plans and gRPC handlers.
func (p *Provider) registerData(registry *gateway.Registry, engine *iam.Engine) error {
	for _, rpc := range generated.BigtableMethods {
		permissions, ok := dataPermissions[rpc.Method]
		if !ok {
			return fmt.Errorf("%w: %s", errDataPlan, rpc.Method)
		}
		if err := engine.Register(rpc.Method, iam.Rule{Baseline: permissions}); err != nil {
			return fmt.Errorf("register %s: %w", rpc.Method, err)
		}
		var err error
		if rpc.Streaming {
			err = registry.RegisterStream(rpc.Method, p.dataStream)
		} else {
			err = registry.Register(rpc.Method, p.dataUnary)
		}
		if err != nil {
			return fmt.Errorf("register %s: %w", rpc.Method, err)
		}
	}
	return nil
}

// dataUnary authorizes one unary data call, then forwards it to the project's emulator.
func (p *Provider) dataUnary(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	route, known := dataRPCs[call.Method]
	if !ok || !known {
		return nil, status.Error(codes.Internal, "Invalid Bigtable request")
	}
	scope, err := p.authorizeData(call, request)
	if err != nil {
		return nil, err
	}
	response := route.NewResponse()
	err = p.manager.Session(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error {
		b, err := emulatorOf(active)
		if err != nil {
			return err
		}
		return backendStatus(b.conn.Invoke(ctx, call.Method, request, response))
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // emulator gRPC statuses carry the client-visible code and message
	}
	return response, nil
}

// dataStream authorizes one server-streaming call once, before any backend
// work, then relays the emulator's stream. Client cancellation and scope reset
// cancel the backend call.
func (p *Provider) dataStream(call gateway.Call, downstream gateway.Stream) error {
	route, known := dataRPCs[call.Method]
	if !known {
		return status.Error(codes.Internal, "Invalid Bigtable request")
	}
	request := route.NewRequest()
	if err := downstream.Recv(request); err != nil {
		return fmt.Errorf("read the stream request: %w", err)
	}
	scope, err := p.authorizeData(call, request)
	if err != nil {
		return err
	}
	return p.manager.Session(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error { //nolint:wrapcheck // emulator gRPC statuses carry the client-visible code and message
		b, err := emulatorOf(active)
		if err != nil {
			return err
		}
		return backendStatus(backend.RelayServerStream(ctx, b.conn, call.Method, request, route.NewResponse, downstream.Send))
	})
}

// viewed is a request that can address an authorized or materialized view.
type viewed interface {
	GetAuthorizedViewName() string
	GetMaterializedViewName() string
}

// authorizeData authorizes one data call against its table when published,
// else its instance, else the project, so absence is reported only to callers
// that hold the permission. It returns the project scope to forward to.
func (p *Provider) authorizeData(call gateway.Call, request proto.Message) (state.Scope, error) {
	if view, ok := request.(viewed); ok && (view.GetAuthorizedViewName() != "" || view.GetMaterializedViewName() != "") {
		return state.Scope{}, gap("authorized and materialized views")
	}
	parts := strings.Split(call.Resource, "/")
	if len(parts) < 4 {
		return state.Scope{}, status.Error(codes.InvalidArgument, "Malformed resource name")
	}
	scope := provider.ProjectScope(parts[1])
	target, exists := p.dataTarget(scope, parts)
	decision, err := p.engine.Authorize(call, target, nil)
	if decision.ID != "" {
		_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-decision-id", decision.ID))
	}
	if err != nil {
		code := status.Code(err)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return scope, fmt.Errorf("authorize %s: %w", call.Method, err)
		}
		return scope, status.Errorf(code, "Request had insufficient permissions: %s on %s.", provider.DeniedPermission(decision),
			strings.TrimPrefix(target.Name, "//bigtableadmin.googleapis.com/"))
	}
	if !exists {
		return scope, provider.NotFound(kindInstance, parts[3])
	}
	return scope, nil
}

// dataTarget resolves the IAM target of a table or instance name split on
// "/", and whether its instance exists, from provider state and the index.
func (p *Provider) dataTarget(scope state.Scope, parts []string) (iam.Target, bool) {
	project, instance := parts[1], parts[3]
	if !uid.MatchString(instance) || p.instanceLocked(scope, instance) == nil {
		return iam.Target{Name: provider.ProjectResource(project)}, false
	}
	target := iam.Target{Name: instanceResource(project, instance)}
	if len(parts) == 6 {
		if resource, found := p.index.Get(tableResource(project, instance, parts[5])); found && resource.Scope == scope {
			target.Name = resource.Name
		}
	}
	return target, true
}

// gap reports an unsupported request shape explicitly.
func gap(feature string) error {
	return status.Error(codes.Unimplemented, "gcp-local Bigtable coverage gap: "+feature)
}
