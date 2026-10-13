package firestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ownerBearer is the emulator's documented administrative credential, which
// bypasses security rules. The provider sends it only after its own IAM
// decision; the caller's token and metadata are never forwarded.
const ownerBearer = "Bearer owner"

// grpcUnary and grpcStreams are the Firestore v1 gRPC methods the provider
// serves, with the emulator response each returns. Write, PartitionQuery, and
// ExecutePipeline stay unregistered and return UNIMPLEMENTED.
var (
	grpcUnary = map[string]func() proto.Message{
		"GetDocument":       func() proto.Message { return &pb.Document{} },
		"ListDocuments":     func() proto.Message { return &pb.ListDocumentsResponse{} },
		"CreateDocument":    func() proto.Message { return &pb.Document{} },
		"UpdateDocument":    func() proto.Message { return &pb.Document{} },
		"DeleteDocument":    func() proto.Message { return &emptypb.Empty{} },
		"BeginTransaction":  func() proto.Message { return &pb.BeginTransactionResponse{} },
		"Commit":            func() proto.Message { return &pb.CommitResponse{} },
		"Rollback":          func() proto.Message { return &emptypb.Empty{} },
		"ListCollectionIds": func() proto.Message { return &pb.ListCollectionIdsResponse{} },
		"BatchWrite":        func() proto.Message { return &pb.BatchWriteResponse{} },
	}
	grpcStreams = map[string]struct{ request, response func() proto.Message }{
		"BatchGetDocuments": {
			func() proto.Message { return &pb.BatchGetDocumentsRequest{} },
			func() proto.Message { return &pb.BatchGetDocumentsResponse{} },
		},
		"RunQuery": {
			func() proto.Message { return &pb.RunQueryRequest{} },
			func() proto.Message { return &pb.RunQueryResponse{} },
		},
		"RunAggregationQuery": {
			func() proto.Message { return &pb.RunAggregationQueryRequest{} },
			func() proto.Message { return &pb.RunAggregationQueryResponse{} },
		},
	}
)

// registerGRPC binds the served Firestore gRPC methods.
func (p *Provider) registerGRPC(registry *gateway.Registry) error {
	for method := range grpcUnary {
		if err := registry.Register(grpcMethodPrefix+method, p.unary); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	for method := range grpcStreams {
		if err := registry.RegisterStream(grpcMethodPrefix+method, p.serverStream); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	if err := registry.RegisterStream(grpcMethodPrefix+"Listen", p.listen); err != nil {
		return fmt.Errorf("register Listen: %w", err)
	}
	return nil
}

// ownerContext carries the owner credential and the validated database as
// the resource-prefix header Google's clients send; the emulator's Listen
// requires it.
func ownerContext(ctx context.Context, project string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", ownerBearer, "google-cloud-resource-prefix", databaseName(project)))
}

// planned validates a gRPC request, registers the project's database anchor,
// and authorizes every plan before any backend work.
func (p *Provider) planned(call gateway.Call, request proto.Message) (backend.Key, error) {
	scope := provider.ProjectScope(call.Project)
	if err := scope.Validate(); err != nil {
		return backend.Key{}, status.Errorf(codes.InvalidArgument, "Invalid project %q", call.Project)
	}
	plans, err := requestPlans(call.Project, request)
	if err != nil {
		return backend.Key{}, err
	}
	if _, err := p.authorize(call, call.Project, plans); err != nil {
		return backend.Key{}, err
	}
	return backend.Key{Service: serviceName, Scope: scope}, nil
}

// unary serves every unary Firestore gRPC method: authorize, then forward the
// unchanged request to the scope's emulator inside Manager.Use.
func (p *Provider) unary(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	newResponse, served := grpcUnary[strings.TrimPrefix(call.Method, grpcMethodPrefix)]
	if !ok || !served {
		return nil, status.Error(codes.Internal, "Invalid Firestore request")
	}
	key, err := p.planned(call, request)
	if err != nil {
		return nil, err
	}
	response := newResponse()
	err = p.manager.Use(call.Context, key, func(ctx context.Context, active backend.Backend) error {
		b, ok := active.(driver)
		if !ok {
			return status.Error(codes.Internal, "Invalid Firestore backend")
		}
		if err := p.expire(ctx, b, key.Scope); err != nil {
			return err
		}
		return b.client().Invoke(ownerContext(ctx, call.Project), call.Method, request, response)
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // keep the emulator or manager status unchanged on the wire
	}
	return response, nil
}

// serverStream serves BatchGetDocuments, RunQuery, and RunAggregationQuery.
// The single request is authorized before the backend session opens; the
// session lets reset cancel a slow reader.
func (p *Provider) serverStream(call gateway.Call, downstream gateway.Stream) error {
	shape, ok := grpcStreams[strings.TrimPrefix(call.Method, grpcMethodPrefix)]
	if !ok {
		return status.Error(codes.Unimplemented, "Firestore coverage gap: this method is not served")
	}
	request := shape.request()
	if err := downstream.Recv(request); err != nil {
		return fmt.Errorf("read the stream request: %w", err)
	}
	key, err := p.planned(call, request)
	if err != nil {
		return err
	}
	return p.manager.Session(call.Context, key, func(ctx context.Context, active backend.Backend) error { //nolint:wrapcheck // keep the emulator status on the wire
		b, ok := active.(driver)
		if !ok {
			return status.Error(codes.Internal, "Invalid Firestore backend")
		}
		if err := p.expire(ctx, b, key.Scope); err != nil {
			return err
		}
		upstream, err := b.client().NewStream(ownerContext(ctx, call.Project), &grpc.StreamDesc{ServerStreams: true}, call.Method)
		if err != nil {
			return err //nolint:wrapcheck // emulator status
		}
		if err := upstream.SendMsg(request); err != nil {
			return err //nolint:wrapcheck // emulator status
		}
		if err := upstream.CloseSend(); err != nil {
			return err //nolint:wrapcheck // emulator status
		}
		return relayResponses(upstream, downstream, shape.response)
	})
}

// relayResponses copies an upstream server stream to the caller until EOF;
// emulator statuses pass through unchanged.
func relayResponses(upstream grpc.ClientStream, downstream gateway.Stream, newResponse func() proto.Message) error {
	for {
		response := newResponse()
		err := upstream.RecvMsg(response)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err //nolint:wrapcheck // emulator status
		}
		if err := downstream.Send(response); err != nil {
			return fmt.Errorf("send a stream response: %w", err)
		}
	}
}

// listen relays a Listen stream. The opening request and every later request
// are validated and authorized before they reach the emulator, and each
// response is re-authorized, so a revoked grant stops delivery.
func (p *Provider) listen(call gateway.Call, downstream gateway.Stream) error {
	first := &pb.ListenRequest{}
	if err := downstream.Recv(first); err != nil {
		return fmt.Errorf("read the opening Listen request: %w", err)
	}
	key, err := p.planned(call, first)
	if err != nil {
		return err
	}
	return p.manager.Session(call.Context, key, func(session context.Context, active backend.Backend) error { //nolint:wrapcheck // keep the IAM or emulator status on the wire
		b, ok := active.(driver)
		if !ok {
			return status.Error(codes.Internal, "Invalid Firestore backend")
		}
		if err := p.expire(session, b, key.Scope); err != nil {
			return err
		}
		upstreamCtx, cancel := context.WithCancel(ownerContext(session, call.Project))
		defer cancel()
		upstream, err := pb.NewFirestoreClient(b.client()).Listen(upstreamCtx)
		if err != nil {
			return err //nolint:wrapcheck // emulator status
		}
		if err := upstream.Send(first); err != nil {
			return fmt.Errorf("forward the opening Listen request: %w", err)
		}
		_ = grpc.SendHeader(call.Context, nil) //nolint:contextcheck // the call context is this request's context
		failures := make(chan error, 2)
		go p.listenRequests(call, downstream, upstream, failures)
		go p.listenResponses(call, upstream, downstream, failures)
		// Returning releases gRPC's blocked downstream Recv/Send calls; the
		// cancelled upstream context ends the emulator stream.
		select {
		case err := <-failures:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-session.Done():
			return session.Err()
		}
	})
}

// listenRequests forwards client requests upstream until the client half-closes.
func (p *Provider) listenRequests(call gateway.Call, downstream gateway.Stream, upstream pb.Firestore_ListenClient, failures chan<- error) {
	for {
		request := &pb.ListenRequest{}
		if err := downstream.Recv(request); err != nil {
			if !errors.Is(err, io.EOF) {
				failures <- err
				return
			}
			if closeErr := upstream.CloseSend(); closeErr != nil {
				failures <- closeErr
			}
			return
		}
		plans, err := requestPlans(call.Project, request)
		if err == nil {
			_, err = p.authorize(call, call.Project, plans)
		}
		if err == nil {
			err = upstream.Send(request)
		}
		if err != nil {
			failures <- err
			return
		}
	}
}

// listenResponses forwards emulator responses after re-authorizing the stream.
func (p *Provider) listenResponses(call gateway.Call, upstream pb.Firestore_ListenClient, downstream gateway.Stream, failures chan<- error) {
	for {
		response, err := upstream.Recv()
		if err == nil {
			_, err = p.authorize(call, call.Project, []string{planListen})
		}
		if err == nil {
			err = downstream.Send(response)
		}
		if err != nil {
			failures <- err
			return
		}
	}
}
