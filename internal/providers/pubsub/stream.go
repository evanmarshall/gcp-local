package pubsub

import (
	"context"
	"errors"
	"fmt"
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
		if _, err := resourceName(request.GetSubscription(), "subscriptions"); err != nil {
			return err
		}
		return validateStreamFlow(request)
	}
	if request.GetSubscription() != "" || request.GetClientId() != "" || request.GetMaxOutstandingMessages() != 0 || request.GetMaxOutstandingBytes() != 0 {
		return invalid("Subscription, client ID, and flow control belong only in the first request")
	}
	return validateStreamFlow(request)
}

// validateStreamFlow rejects negative flow-control values, mismatched
// ack-deadline arrays, and out-of-range deadlines.
func validateStreamFlow(request *pb.StreamingPullRequest) error {
	if request.GetMaxOutstandingMessages() < 0 || request.GetMaxOutstandingBytes() < 0 || len(request.GetModifyDeadlineSeconds()) != len(request.GetModifyDeadlineAckIds()) {
		return invalid("Invalid streaming flow control or ack deadlines")
	}
	for _, seconds := range request.GetModifyDeadlineSeconds() {
		if seconds < 0 || seconds > 600 {
			return invalid("Ack deadline must be zero through 600 seconds")
		}
	}
	if deadline := request.GetStreamAckDeadlineSeconds(); deadline != 0 && (deadline < 10 || deadline > 600) {
		return invalid("Stream ack deadline must be 10 through 600 seconds")
	}
	return nil
}

func (p *Provider) stream(call gateway.Call, downstream gateway.Stream) error {
	first, err := openStream(call, downstream)
	if err != nil {
		return err
	}
	if err := p.authorize(call, first); err != nil {
		return err
	}
	if err := p.manager.Session(call.Context, keyOf(call), func(session context.Context, active backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		if err := p.authorize(call, first); err != nil {
			return err
		}
		return p.relayStream(session, call, downstream, active, first)
	}); err != nil {
		return fmt.Errorf("open the backend stream session: %w", err)
	}
	return nil
}

// openStream reads and validates the opening StreamingPullRequest, which carries
// the subscription the whole stream acts on.
func openStream(call gateway.Call, downstream gateway.Stream) (*pb.StreamingPullRequest, error) {
	first := &pb.StreamingPullRequest{}
	if err := downstream.Recv(first); err != nil {
		return nil, fmt.Errorf("read the opening stream request: %w", err)
	}
	if err := validateStream(first, true); err != nil {
		return nil, err
	}
	if first.GetSubscription() != call.Resource {
		return nil, invalid("Stream subscription disagrees with request scope")
	}
	return first, nil
}

// relayStream forwards both directions between the downstream gRPC stream and
// the backend's StreamingPull stream. Each relay goroutine reports the first
// failure it hits; the session context ends pending backend work on reset.
func (p *Provider) relayStream(session context.Context, call gateway.Call, downstream gateway.Stream, active backend.Backend, first *pb.StreamingPullRequest) error {
	upstreamCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(session, metadata.MD{}))
	defer cancel()
	b, err := driverOf(active)
	if err != nil {
		return err
	}
	upstream, err := pb.NewSubscriberClient(b.client()).StreamingPull(upstreamCtx)
	if err != nil {
		return fmt.Errorf("open the upstream stream: %w", err)
	}
	if err := upstream.Send(first); err != nil {
		return fmt.Errorf("forward the opening request upstream: %w", err)
	}
	_ = grpc.SendHeader(call.Context, nil) //nolint:contextcheck // the session context is this request's context by design
	failures := make(chan error, 2)
	go p.relayDownstream(call, downstream, upstream, failures)
	go p.relayUpstream(call, upstream, downstream, failures, first)
	// Returning releases gRPC's blocked downstream Recv/Send calls. Neither
	// relay mutates provider state; the cancelled upstream context terminates
	// pending backend work before its owned connection is closed on reset.
	select {
	case err := <-failures:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	case <-session.Done():
		return session.Err()
	}
}

// relayDownstream forwards client requests upstream until the client half-closes.
func (p *Provider) relayDownstream(call gateway.Call, downstream gateway.Stream, upstream pb.Subscriber_StreamingPullClient, failures chan<- error) {
	for {
		request := &pb.StreamingPullRequest{}
		if err := downstream.Recv(request); err != nil {
			// A client half-close drains responses until upstream EOF.
			finishDownstream(err, upstream, failures)
			return
		}
		if err := validateStream(request, false); err != nil {
			failures <- err
			return
		}
		if err := p.authorize(call, request); err != nil {
			failures <- err
			return
		}
		if err := upstream.Send(request); err != nil {
			failures <- err
			return
		}
	}
}

// finishDownstream handles a downstream receive error: a client half-close
// becomes an upstream CloseSend, and anything else is the relay's failure.
func finishDownstream(err error, upstream pb.Subscriber_StreamingPullClient, failures chan<- error) {
	if !errors.Is(err, io.EOF) {
		failures <- err
		return
	}
	if closeErr := upstream.CloseSend(); closeErr != nil {
		failures <- closeErr
	}
}

// relayUpstream forwards backend responses downstream; every response batch is
// re-authorized against the subscription that opened the stream.
func (p *Provider) relayUpstream(call gateway.Call, upstream pb.Subscriber_StreamingPullClient, downstream gateway.Stream, failures chan<- error, first *pb.StreamingPullRequest) {
	for {
		response, err := upstream.Recv()
		if err != nil {
			failures <- err
			return
		}
		if err := p.authorize(call, first); err != nil {
			failures <- err
			return
		}
		if err := downstream.Send(response); err != nil {
			failures <- err
			return
		}
	}
}
