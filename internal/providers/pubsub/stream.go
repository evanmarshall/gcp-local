package pubsub

import (
	"context"
	"io"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func validateStream(request *pb.StreamingPullRequest, first bool) error {
	if err := fields(request, "subscription", "ack_ids", "modify_deadline_seconds", "modify_deadline_ack_ids", "stream_ack_deadline_seconds", "client_id", "max_outstanding_messages", "max_outstanding_bytes"); err != nil {
		return err
	}
	if first {
		if _, err := resourceName(request.Subscription, "subscriptions"); err != nil {
			return err
		}
		if request.StreamAckDeadlineSeconds < 10 || request.StreamAckDeadlineSeconds > 600 {
			return invalid("Stream ack deadline must be 10 through 600 seconds")
		}
	} else if request.Subscription != "" || request.ClientId != "" || request.MaxOutstandingMessages != 0 || request.MaxOutstandingBytes != 0 {
		return invalid("Subscription, client ID, and flow control belong only in the first request")
	}
	if request.MaxOutstandingMessages < 0 || request.MaxOutstandingBytes < 0 || len(request.ModifyDeadlineSeconds) != len(request.ModifyDeadlineAckIds) {
		return invalid("Invalid streaming flow control or ack deadlines")
	}
	for _, seconds := range request.ModifyDeadlineSeconds {
		if seconds < 0 || seconds > 600 {
			return invalid("Ack deadline must be zero through 600 seconds")
		}
	}
	if request.StreamAckDeadlineSeconds != 0 && (request.StreamAckDeadlineSeconds < 10 || request.StreamAckDeadlineSeconds > 600) {
		return invalid("Stream ack deadline must be 10 through 600 seconds")
	}
	return nil
}

func (p *Provider) stream(call gateway.Call, downstream gateway.Stream) error {
	first := &pb.StreamingPullRequest{}
	if err := downstream.Recv(first); err != nil {
		return err
	}
	if err := validateStream(first, true); err != nil {
		return err
	}
	if first.Subscription != call.Resource {
		return invalid("Stream subscription disagrees with request scope")
	}
	if err := p.authorize(call, first); err != nil {
		return err
	}
	return p.manager.Session(call.Context, keyOf(call), func(ctx context.Context, active backend.Backend) error {
		call.Context = ctx
		if err := p.authorize(call, first); err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(ctx, metadata.MD{}))
		defer cancel()
		upstream, err := pb.NewSubscriberClient(active.(driver).client()).StreamingPull(ctx)
		if err != nil {
			return err
		}
		if err := upstream.Send(first); err != nil {
			return err
		}
		_ = grpc.SendHeader(call.Context, nil)
		errors := make(chan error, 2)
		go func() {
			for {
				request := &pb.StreamingPullRequest{}
				if err := downstream.Recv(request); err != nil {
					if err == io.EOF {
						if err = upstream.CloseSend(); err != nil {
							errors <- err
						}
						return // Drain responses until upstream EOF after half-close.
					}
					errors <- err
					return
				}
				if err := validateStream(request, false); err != nil {
					errors <- err
					return
				}
				if err := p.authorize(call, request); err != nil {
					errors <- err
					return
				}
				if err := upstream.Send(request); err != nil {
					errors <- err
					return
				}
			}
		}()
		go func() {
			for {
				response, err := upstream.Recv()
				if err != nil {
					errors <- err
					return
				}
				if err := p.authorize(call, first); err != nil {
					errors <- err
					return
				}
				if err := downstream.Send(response); err != nil {
					errors <- err
					return
				}
			}
		}()
		// Returning releases gRPC's blocked downstream Recv/Send calls. Neither
		// relay mutates provider state; the cancelled upstream context terminates
		// pending backend work before its owned connection is closed on reset.
		select {
		case err := <-errors:
			if err == io.EOF {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}
