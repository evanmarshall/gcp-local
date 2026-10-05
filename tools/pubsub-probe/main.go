//go:build backendprobe

// Command pubsub-probe checks the upstream backend, not gcp-local fidelity.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const topic = "projects/backend-probe/topics/topic"
const subscription = "projects/backend-probe/subscriptions/subscription"
const peer = "projects/backend-peer/topics/topic"

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:8085", "isolated loopback backend")
	empty := flag.Bool("expect-empty", false, "check state loss after restart")
	forwarded := flag.Bool("moo-forwarded", false, "explicit host loopback probe of a disposable Moo backend; not isolated conformance")
	flag.Parse()
	host, _, err := net.SplitHostPort(*endpoint)
	must(err)
	interfaces, err := net.Interfaces()
	must(err)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		panic("Backend probes require an explicit numeric loopback target")
	}
	if !*forwarded && (len(interfaces) != 1 || interfaces[0].Name != "lo" || os.Getenv("GCP_LOCAL_ISOLATED") != "1") {
		panic("Run this backend-only probe in the isolated local runner")
	}
	connection, err := grpc.NewClient(*endpoint, grpc.WithNoProxy(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(err)
	defer connection.Close()
	publisher, subscriber := pb.NewPublisherClient(connection), pb.NewSubscriberClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if *empty {
		for _, name := range []string{topic, peer} {
			_, err := publisher.GetTopic(ctx, &pb.GetTopicRequest{Topic: name})
			check(status.Code(err) == codes.NotFound, "restart retained a topic")
		}
		_, err := subscriber.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: subscription})
		check(status.Code(err) == codes.NotFound, "restart retained a subscription")
		fmt.Println(`{"restart_empty":true}`)
		return
	}
	_, err = publisher.CreateTopic(ctx, &pb.Topic{Name: topic})
	must(err)
	_, err = publisher.CreateTopic(ctx, &pb.Topic{Name: peer})
	must(err)
	_, err = subscriber.CreateSubscription(ctx, &pb.Subscription{Name: subscription, Topic: topic, AckDeadlineSeconds: 10})
	must(err)
	// A deliberately invalid credential is accepted upstream: the provider must
	// enforce identity/IAM before invoking this backend.
	unauthorized := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer not-a-real-token")
	published, err := publisher.Publish(unauthorized, &pb.PublishRequest{Topic: topic, Messages: []*pb.PubsubMessage{{Data: []byte("single")}}})
	must(err)
	check(len(published.MessageIds) == 1 && published.MessageIds[0] != "", "missing message ID")
	pulled, err := subscriber.Pull(ctx, &pb.PullRequest{Subscription: subscription, MaxMessages: 1})
	must(err)
	check(len(pulled.ReceivedMessages) == 1, "missing pulled message")
	message := pulled.ReceivedMessages[0]
	check(message.Message.MessageId == published.MessageIds[0] && string(message.Message.Data) == "single", "pull changed message")
	_, err = subscriber.Acknowledge(ctx, &pb.AcknowledgeRequest{Subscription: subscription, AckIds: []string{message.AckId}})
	must(err)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	stream, err := subscriber.StreamingPull(streamCtx)
	must(err)
	must(stream.Send(&pb.StreamingPullRequest{Subscription: subscription, StreamAckDeadlineSeconds: 10, MaxOutstandingMessages: 10, MaxOutstandingBytes: 10240}))
	published, err = publisher.Publish(ctx, &pb.PublishRequest{Topic: topic, Messages: []*pb.PubsubMessage{{Data: []byte("stream")}}})
	must(err)
	received, err := stream.Recv()
	must(err)
	check(len(received.ReceivedMessages) == 1 && received.ReceivedMessages[0].Message.MessageId == published.MessageIds[0], "stream changed message")
	must(stream.Send(&pb.StreamingPullRequest{AckIds: []string{received.ReceivedMessages[0].AckId}}))
	must(stream.CloseSend())
	_, halfClose := stream.Recv()
	check(halfClose == io.EOF, "stream half-close did not terminate cleanly")
	cancelCtx, stop := context.WithCancel(ctx)
	canceled, err := subscriber.StreamingPull(cancelCtx)
	must(err)
	must(canceled.Send(&pb.StreamingPullRequest{Subscription: subscription, StreamAckDeadlineSeconds: 10}))
	stop()
	_, err = canceled.Recv()
	check(status.Code(err) == codes.Canceled, "stream cancellation failed")
	_, iamErr := iampb.NewIAMPolicyClient(connection).GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	check(status.Code(iamErr) == codes.Unimplemented, "unexpected upstream IAM behavior")
	// Retain one owned topic/subscription for the external restart check.
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"kind": "backend-only", "create_publish_pull_ack": true,
		"streaming": true, "half_close": true, "cancellation": true, "iam": "not-enforced", "cloud_fidelity": "unverified", "moo_forwarded": *forwarded})
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
