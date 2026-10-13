package bigtable

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/evanmarshall/gcp-local/api/generated/contract"
	bigtableapi "github.com/evanmarshall/gcp-local/api/generated/databases/bigtable"
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
var dataRPCs = func() map[string]contract.RPC {
	rpcs := map[string]contract.RPC{}
	for _, rpc := range bigtableapi.BigtableMethods {
		rpcs[rpc.Method] = rpc
	}
	return rpcs
}()

// registerData binds the data-plane permission plans and gRPC handlers.
func (p *Provider) registerData(registry *gateway.Registry, engine *iam.Engine) error {
	for _, rpc := range bigtableapi.BigtableMethods {
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

// profiled is a request that names an app profile; every data RPC does.
type profiled interface{ GetAppProfileId() string }

// dataDenied is the data plane's 403 message, the permission then the
// resource; it is not recorded.
const dataDenied = "Request had insufficient permissions: %s on %s."

// authorizeData authorizes one data call against its table when published,
// else its instance, else the project, so absence is reported only to callers
// that hold the permission. It returns the project scope to forward to. Only
// the default app profile is modeled.
func (p *Provider) authorizeData(call gateway.Call, request proto.Message) (state.Scope, error) {
	if err := unservedData(request); err != nil {
		return state.Scope{}, err
	}
	parts := strings.Split(call.Resource, "/")
	if len(parts) < 4 || !uid.MatchString(parts[3]) {
		return state.Scope{}, status.Error(codes.InvalidArgument, "Malformed resource name")
	}
	project, instance := parts[1], parts[3]
	scope := provider.ProjectScope(project)
	candidates := []string{instanceResource(project, instance), provider.ProjectResource(project)}
	if len(parts) == 6 {
		candidates = append([]string{tableResource(project, instance, parts[5])}, candidates...)
	}
	gate := p.gate
	gate.Message = dataDenied
	decision, target, err := provider.Nearest(gate, p.scopes, scope, call, "", candidates...)
	if decision.ID != "" {
		_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-decision-id", decision.ID))
	}
	if err != nil {
		return scope, dataRefusal(call, err, decision, target)
	}
	if target.Name == provider.ProjectResource(project) {
		return scope, status.Errorf(codes.NotFound, "Instance %s not found.", provider.InstanceName(project, instance))
	}
	return scope, nil
}

// unservedData refuses authorized and materialized views and app profiles
// other than the default one.
func unservedData(request proto.Message) error {
	if view, ok := request.(viewed); ok && (view.GetAuthorizedViewName() != "" || view.GetMaterializedViewName() != "") {
		return gap("authorized and materialized views")
	}
	if named, ok := request.(profiled); ok && named.GetAppProfileId() != "" && named.GetAppProfileId() != "default" {
		return gap("app profile " + named.GetAppProfileId())
	}
	return nil
}

// dataRefusal renders a data-plane authorization refusal: a denial naming
// the permission and the target, or the 401 for a call without credentials.
func dataRefusal(call gateway.Call, err error, decision iam.Decision, target iam.Target) error {
	if status.Code(err) == codes.PermissionDenied {
		return status.Errorf(codes.PermissionDenied, dataDenied, provider.DeniedPermission(decision),
			strings.TrimPrefix(target.Name, "//"+adminHost+"/"))
	}
	return credentials(call, err, "bigtable.googleapis.com", strings.ReplaceAll(strings.TrimPrefix(call.Method, "/"), "/", "."))
}

// gap reports an unsupported request shape explicitly.
var gap = provider.CoverageGap("Bigtable")
