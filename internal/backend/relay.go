package backend

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// RelayServerStream forwards one authorized server-streaming call to a backend
// connection: it sends the single request, then passes each response to send
// until the backend ends the stream. Cancelling ctx (client cancellation,
// reset, or shutdown) cancels the backend call.
func RelayServerStream(ctx context.Context, conn grpc.ClientConnInterface, method string, request proto.Message,
	newResponse func() proto.Message, send func(proto.Message) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	if err != nil {
		return err //nolint:wrapcheck // backend gRPC statuses carry the client-visible code and message
	}
	// io.EOF from SendMsg means the backend already ended the stream; RecvMsg
	// reports its status.
	if err := stream.SendMsg(request); err != nil && !errors.Is(err, io.EOF) {
		return err //nolint:wrapcheck // backend gRPC statuses carry the client-visible code and message
	}
	if err := stream.CloseSend(); err != nil {
		return err //nolint:wrapcheck // backend gRPC statuses carry the client-visible code and message
	}
	for {
		response := newResponse()
		if err := stream.RecvMsg(response); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err //nolint:wrapcheck // backend gRPC statuses carry the client-visible code and message
		}
		if err := send(response); err != nil {
			return err
		}
	}
}
