package gateway

import (
	"context"
	"io"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func (g *Gateway) NewGRPCServer() *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2 << 20))
	pb.RegisterPublisherServer(server, &generated.PublisherServer{Dispatch: g})
	pb.RegisterSubscriberServer(server, &generated.SubscriberServer{Dispatch: g})
	return server
}
func (g *Gateway) rpcCall(ctx context.Context, method string, request proto.Message) (Call, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	call, err := g.newCall(ctx, method, md.Get("authorization"), md.Get("traceparent"))
	if err != nil {
		return call, err
	}
	info, ok := g.rpcs[method]
	if !ok {
		return call, gap(method)
	}
	resource, err := resourceField(request, info.ResourceField)
	if err != nil {
		return call, err
	}
	parts, pattern := strings.Split(resource, "/"), strings.Split(info.ResourcePattern, "/")
	if len(parts) != len(pattern) {
		return call, status.Error(codes.InvalidArgument, "Malformed resource name")
	}
	for i, part := range parts {
		if part == "" || part == "." || part == ".." || (pattern[i] != "*" && part != pattern[i]) {
			return call, status.Error(codes.InvalidArgument, "Malformed resource name")
		}
		if i > 0 && parts[i-1] == "projects" {
			call.Project = part
		}
		if i > 0 && parts[i-1] == "locations" {
			call.Location = part
		}
	}
	call.Resource = resource
	return call, nil
}
func resourceField(message proto.Message, path string) (string, error) {
	current := message.ProtoReflect()
	fields := strings.Split(path, ".")
	for i, name := range fields {
		field := current.Descriptor().Fields().ByName(protoreflect.Name(name))
		if field == nil {
			return "", status.Error(codes.Internal, "Contract resource field missing")
		}
		if i == len(fields)-1 && field.Kind() == protoreflect.StringKind {
			return current.Get(field).String(), nil
		}
		if field.Kind() != protoreflect.MessageKind || !current.Has(field) {
			return "", status.Error(codes.InvalidArgument, "Missing resource name")
		}
		current = current.Get(field).Message()
	}
	return "", status.Error(codes.Internal, "Contract resource field is not a string")
}
func (g *Gateway) Unary(ctx context.Context, method string, request proto.Message) (proto.Message, error) {
	call, err := g.rpcCall(ctx, method, request)
	if err != nil {
		return nil, err
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-gcp-local-trace-id", call.TraceID))
	result, err := g.Registry.Invoke(call, request)
	if err != nil {
		return nil, err
	}
	response, ok := result.(proto.Message)
	if !ok || response == nil {
		return nil, status.Error(codes.Internal, "Provider returned an invalid response type")
	}
	return response, nil
}
func (g *Gateway) Stream(method string, transport grpc.ServerStream) error {
	handler, err := g.Registry.streamHandler(method)
	if err != nil {
		return err
	}
	info := g.rpcs[method]
	first := info.NewRequest()
	if err := transport.RecvMsg(first); err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "Missing initial stream request")
		}
		return normalizeError(err)
	}
	call, err := g.rpcCall(transport.Context(), method, first)
	if err != nil {
		return err
	}
	transport.SetHeader(metadata.Pairs("x-gcp-local-trace-id", call.TraceID))
	stream := &rpcStream{ServerStream: transport, first: first, requestType: first.ProtoReflect().Descriptor().FullName(), responseType: info.NewResponse().ProtoReflect().Descriptor().FullName()}
	return normalizeError(handler(call, stream))
}

type rpcStream struct {
	grpc.ServerStream
	first                     proto.Message
	requestType, responseType protoreflect.FullName
}

func (s *rpcStream) Recv(message proto.Message) error {
	if message.ProtoReflect().Descriptor().FullName() != s.requestType {
		return status.Error(codes.Internal, "Provider received into an invalid stream type")
	}
	if s.first != nil {
		proto.Reset(message)
		proto.Merge(message, s.first)
		s.first = nil
		return nil
	}
	return s.RecvMsg(message)
}
func (s *rpcStream) Send(message proto.Message) error {
	if message.ProtoReflect().Descriptor().FullName() != s.responseType {
		return status.Error(codes.Internal, "Provider sent an invalid stream type")
	}
	return s.SendMsg(message)
}
