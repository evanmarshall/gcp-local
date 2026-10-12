package spanner

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Spanner data-plane gRPC methods (google.spanner.v1.Spanner).
const (
	dataService            = "/google.spanner.v1.Spanner/"
	rpcCreateSession       = dataService + "CreateSession"
	rpcBatchCreateSessions = dataService + "BatchCreateSessions"
	rpcGetSession          = dataService + "GetSession"
	rpcListSessions        = dataService + "ListSessions"
	rpcDeleteSession       = dataService + "DeleteSession"
	rpcExecuteSQL          = dataService + "ExecuteSql"
	rpcExecuteStreamingSQL = dataService + "ExecuteStreamingSql"
	rpcExecuteBatchDml     = dataService + "ExecuteBatchDml"
	rpcRead                = dataService + "Read"
	rpcStreamingRead       = dataService + "StreamingRead"
	rpcBeginTransaction    = dataService + "BeginTransaction"
	rpcCommit              = dataService + "Commit"
	rpcRollback            = dataService + "Rollback"
	rpcPartitionQuery      = dataService + "PartitionQuery"
	rpcPartitionRead       = dataService + "PartitionRead"
	// planBeginReadOnly is the permission plan of a read-only BeginTransaction;
	// the RPC itself is registered under rpcBeginTransaction.
	planBeginReadOnly = "spanner.data.BeginTransaction.readOnly"
)

// Request features resolved for methods whose transaction selector can begin
// a transaction inline.
const (
	featureBeginReadOnly  = "begin_read_only"
	featureBeginReadWrite = "begin_read_write"
)

// Data-plane permissions.
const (
	permBeginReadOnly  = "spanner.databases.beginReadOnlyTransaction"
	permBeginReadWrite = "spanner.databases.beginOrRollbackReadWriteTransaction"
	permWrite          = "spanner.databases.write"
	permSessionsCreate = "spanner.sessions.create"
)

// dataPlan is one method's documented permission on the session's database
// and whether its request can begin a transaction inline.
type dataPlan struct {
	permission string
	begins     bool
}

// dataPlans follows the per-method "Authorization requires" rows of the
// Spanner v1 REST reference (projects.instances.databases.sessions.*). The
// begin features add the BeginTransaction permission for an inline begin; that
// addition is modeled locally, not documented per method.
var dataPlans = map[string]dataPlan{
	rpcCreateSession:       {permSessionsCreate, false},
	rpcBatchCreateSessions: {permSessionsCreate, false},
	rpcGetSession:          {"spanner.sessions.get", false},
	rpcListSessions:        {"spanner.sessions.list", false},
	rpcDeleteSession:       {"spanner.sessions.delete", false},
	rpcExecuteSQL:          {"spanner.databases.select", true},
	rpcExecuteStreamingSQL: {"spanner.databases.select", true},
	rpcExecuteBatchDml:     {permWrite, true},
	rpcRead:                {"spanner.databases.read", true},
	rpcStreamingRead:       {"spanner.databases.read", true},
	rpcBeginTransaction:    {permBeginReadWrite, false},
	planBeginReadOnly:      {permBeginReadOnly, false},
	rpcCommit:              {permWrite, true},
	rpcRollback:            {permBeginReadWrite, false},
	rpcPartitionQuery:      {"spanner.databases.partitionQuery", true},
	rpcPartitionRead:       {"spanner.databases.partitionRead", true},
}

// errDataPlan reports a generated data-plane method without a permission plan.
var errDataPlan = errors.New("spanner data-plane method has no permission plan")

// dataRPCs indexes the generated data-plane contracts by method.
var dataRPCs = func() map[string]generated.RPC {
	rpcs := map[string]generated.RPC{}
	for _, rpc := range generated.SpannerMethods {
		rpcs[rpc.Method] = rpc
	}
	return rpcs
}()

// registerDataPermissions binds every data-plane plan.
func registerDataPermissions(engine *iam.Engine) error {
	for method, plan := range dataPlans {
		rule := iam.Rule{Baseline: []string{plan.permission}}
		if plan.begins {
			rule.When = map[string][]string{featureBeginReadOnly: {permBeginReadOnly}, featureBeginReadWrite: {permBeginReadWrite}}
		}
		if err := engine.Register(method, rule); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// sessionPath binds a session name from the URL path.
func sessionPath(path map[string]string) string {
	return databasePath(path) + "/sessions/" + path["session"]
}

// restRoute maps one unary REST session method onto its gRPC method: field
// receives the resource name bound from the URL path, and the JSON body (if
// any) fills the rest of the request, as in the v1 google.api.http rules.
type restRoute struct {
	grpc, field string
	bind        binding
}

var restData = map[string]restRoute{
	"spanner.sessions.create":           {rpcCreateSession, pathDatabase, databasePath},
	"spanner.sessions.batchCreate":      {rpcBatchCreateSessions, pathDatabase, databasePath},
	"spanner.sessions.list":             {rpcListSessions, pathDatabase, databasePath},
	"spanner.sessions.get":              {rpcGetSession, fieldName, sessionPath},
	"spanner.sessions.delete":           {rpcDeleteSession, fieldName, sessionPath},
	"spanner.sessions.executeSql":       {rpcExecuteSQL, fieldSession, sessionPath},
	"spanner.sessions.executeBatchDml":  {rpcExecuteBatchDml, fieldSession, sessionPath},
	"spanner.sessions.read":             {rpcRead, fieldSession, sessionPath},
	"spanner.sessions.beginTransaction": {rpcBeginTransaction, fieldSession, sessionPath},
	"spanner.sessions.commit":           {rpcCommit, fieldSession, sessionPath},
	"spanner.sessions.rollback":         {rpcRollback, fieldSession, sessionPath},
	"spanner.sessions.partitionQuery":   {rpcPartitionQuery, fieldSession, sessionPath},
	"spanner.sessions.partitionRead":    {rpcPartitionRead, fieldSession, sessionPath},
}

// fieldSession is the request field naming the session of a data call.
const fieldSession = "session"

// restSession transcodes one REST session request to its gRPC request, then
// authorizes and forwards it exactly as the gRPC path does.
func (p *Provider) restSession(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	route, known := restData[call.Method]
	if !ok || !known {
		return nil, status.Error(codes.Internal, "Invalid Spanner request")
	}
	contract := dataRPCs[route.grpc]
	message, err := rpc{method: route.grpc, request: contract.NewRequest(), response: contract.NewResponse(),
		body: "*", field: route.field, bind: route.bind}.build(request)
	if err != nil {
		return nil, err
	}
	call.Method, call.Project, call.Resource = route.grpc, request.Path[pathProject], route.bind(request.Path)
	response, err := p.dataUnary(call, message)
	if err != nil {
		return nil, err
	}
	reply, ok := response.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Spanner response")
	}
	document, err := render(reply)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// registerData binds the gRPC and REST data-plane handlers.
func (p *Provider) registerData(registry *gateway.Registry) error {
	for method := range restData {
		if err := registry.Register(method, p.restSession); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	for _, rpc := range generated.SpannerMethods {
		if _, ok := dataPlans[rpc.Method]; !ok {
			return fmt.Errorf("%w: %s", errDataPlan, rpc.Method)
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

// dataUnary authorizes one unary data-plane call against the session's
// database, then forwards it to the project's emulator.
func (p *Provider) dataUnary(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	route, known := dataRPCs[call.Method]
	if !ok || !known {
		return nil, status.Error(codes.Internal, "Invalid Spanner request")
	}
	scope, err := p.authorizeData(call, request)
	if err != nil {
		return nil, err
	}
	response := route.NewResponse()
	err = p.manager.Session(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, active backend.Backend) error {
		b, err := driverOf(active)
		if err != nil {
			return err
		}
		return b.client().Invoke(ctx, call.Method, request, response)
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
		return status.Error(codes.Internal, "Invalid Spanner request")
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
		b, err := driverOf(active)
		if err != nil {
			return err
		}
		return backend.RelayServerStream(ctx, b.client(), call.Method, request, route.NewResponse, downstream.Send)
	})
}

// authorizeData validates the request shape, resolves its plan and features,
// and authorizes it against the addressed database (or its nearest indexed
// ancestor, so a missing database is reported by the emulator after
// authorization). It returns the project scope to forward to.
func (p *Provider) authorizeData(call gateway.Call, request proto.Message) (state.Scope, error) {
	parts := strings.Split(call.Resource, "/")
	if len(parts) < 6 {
		return state.Scope{}, status.Error(codes.InvalidArgument, "Malformed resource name")
	}
	path := map[string]string{pathProject: parts[1], pathInstance: parts[3], pathDatabase: parts[5]}
	scope := scopeOf(parts[1])
	if _, failed := p.poisoned.Load(scope); failed {
		return scope, status.Error(codes.FailedPrecondition, "A Spanner mutation outcome is unresolved; reset this scope")
	}
	plan, features, err := dataFeatures(call.Method, request)
	if err != nil {
		return scope, err
	}
	target := p.target(path)
	authorization := call
	authorization.Method = plan
	decision, err := p.engine.Authorize(authorization, target, features)
	if decision.ID != "" {
		_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-decision-id", decision.ID))
	}
	if err != nil {
		code := status.Code(err)
		if code != codes.PermissionDenied && code != codes.Unauthenticated {
			return scope, fmt.Errorf("authorize %s: %w", call.Method, err)
		}
		return scope, status.Errorf(code, "Caller is missing IAM permission %s on resource %s.",
			deniedPermission(decision), strings.TrimPrefix(target.Name, "//spanner.googleapis.com/"))
	}
	return scope, nil
}

// deniedPermission names the first failed check of a decision.
func deniedPermission(decision iam.Decision) string {
	for _, check := range decision.Checks {
		if !check.Allowed {
			return check.Permission
		}
	}
	return "unspecified"
}

// selected is a request whose transaction selector can begin a transaction.
type selected interface {
	GetTransaction() *spannerpb.TransactionSelector
}

// dataFeatures validates request fields outside the local slice and resolves
// the permission plan and begin features of one request.
func dataFeatures(method string, request proto.Message) (string, map[string]bool, error) {
	if err := supportedData(request); err != nil {
		return "", nil, err
	}
	if !dataPlans[method].begins {
		plan, err := beginPlan(method, request)
		return plan, nil, err
	}
	features := map[string]bool{featureBeginReadOnly: false, featureBeginReadWrite: false}
	if commit, ok := request.(*spannerpb.CommitRequest); ok {
		if single := commit.GetSingleUseTransaction(); single != nil {
			if single.GetReadWrite() == nil {
				return "", nil, gap("single-use commit transactions other than read-write")
			}
			features[featureBeginReadWrite] = true
		}
		return method, features, nil
	}
	carrier, ok := request.(selected)
	if !ok {
		return "", nil, status.Error(codes.Internal, "Spanner request has no transaction selector")
	}
	if err := selectorFeatures(carrier.GetTransaction(), features); err != nil {
		return "", nil, err
	}
	return method, features, nil
}

// beginPlan selects the BeginTransaction plan by transaction mode; every other
// method without begin features keeps its own plan.
func beginPlan(method string, request proto.Message) (string, error) {
	begin, ok := request.(*spannerpb.BeginTransactionRequest)
	switch {
	case !ok:
		return method, nil
	case begin.GetOptions().GetPartitionedDml() != nil:
		return "", gap("partitioned DML transactions")
	case begin.GetOptions().GetReadOnly() != nil:
		return planBeginReadOnly, nil
	default:
		return method, nil
	}
}

// selectorFeatures marks the transaction a selector begins inline. Single-use
// read-write and inline partitioned DML are refused: the plan has no write
// permission for them.
func selectorFeatures(selector *spannerpb.TransactionSelector, features map[string]bool) error {
	if single := selector.GetSingleUse(); single != nil && single.GetReadOnly() == nil {
		return gap("single-use transactions other than read-only")
	}
	begin := selector.GetBegin()
	switch {
	case begin == nil:
	case begin.GetReadOnly() != nil:
		features[featureBeginReadOnly] = true
	case begin.GetReadWrite() != nil:
		features[featureBeginReadWrite] = true
	default:
		return gap("inline partitioned DML transactions")
	}
	return nil
}

// supportedData rejects request features the local slice does not authorize:
// fine-grained access control roles and Data Boost.
func supportedData(request proto.Message) error {
	switch request := request.(type) {
	case *spannerpb.CreateSessionRequest:
		if request.GetSession().GetCreatorRole() != "" {
			return gap("fine-grained access control (creator_role)")
		}
	case *spannerpb.BatchCreateSessionsRequest:
		if request.GetSessionTemplate().GetCreatorRole() != "" {
			return gap("fine-grained access control (creator_role)")
		}
	case *spannerpb.ExecuteSqlRequest:
		if request.GetDataBoostEnabled() {
			return gap("Data Boost")
		}
	case *spannerpb.ReadRequest:
		if request.GetDataBoostEnabled() {
			return gap("Data Boost")
		}
	default:
	}
	return nil
}
