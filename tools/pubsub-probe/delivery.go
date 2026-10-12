//go:build backendprobe

package main

import (
	"context"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// The provider builds on these emulator behaviors (docs/services/messaging/pubsub.md):
// dead-lettering it can switch with UpdateSubscription, an ack-deadline expiry
// that keeps key order, an unenforced retry backoff, and UNKNOWN for an
// unparsable filter.
const deliveryTopic = "projects/backend-probe/topics/delivery"

type deliveryProbe struct {
	ctx        context.Context
	publisher  pb.PublisherClient
	subscriber pb.SubscriberClient
}

func (d deliveryProbe) subscription(name string, edit func(*pb.Subscription)) string {
	s := &pb.Subscription{Name: "projects/backend-probe/subscriptions/" + name, Topic: deliveryTopic, AckDeadlineSeconds: 10}
	edit(s)
	_, err := d.subscriber.CreateSubscription(d.ctx, s)
	must(err)
	return s.GetName()
}

func (d deliveryProbe) publish(key, value string) {
	_, err := d.publisher.Publish(d.ctx, &pb.PublishRequest{Topic: deliveryTopic, Messages: []*pb.PubsubMessage{{Data: []byte(value), OrderingKey: key}}})
	must(err)
}

func (d deliveryProbe) pull(subscription string, wait time.Duration) []*pb.ReceivedMessage {
	deadline := time.Now().Add(wait)
	for {
		response, err := d.subscriber.Pull(d.ctx, &pb.PullRequest{Subscription: subscription, MaxMessages: 1, ReturnImmediately: true})
		must(err)
		if len(response.GetReceivedMessages()) > 0 || time.Now().After(deadline) {
			return response.GetReceivedMessages()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (d deliveryProbe) deadline(subscription, ackID string, seconds int32) {
	_, err := d.subscriber.ModifyAckDeadline(d.ctx, &pb.ModifyAckDeadlineRequest{Subscription: subscription, AckIds: []string{ackID}, AckDeadlineSeconds: seconds})
	must(err)
}

func first(received []*pb.ReceivedMessage) string {
	if len(received) == 0 {
		return ""
	}
	return string(received[0].GetMessage().GetData())
}

// probeDelivery checks each emulator behavior and returns what it observed.
func probeDelivery(ctx context.Context, publisher pb.PublisherClient, subscriber pb.SubscriberClient) map[string]any {
	d := deliveryProbe{ctx: ctx, publisher: publisher, subscriber: subscriber}
	_, err := publisher.CreateTopic(ctx, &pb.Topic{Name: deliveryTopic})
	must(err)
	_, err = publisher.CreateTopic(ctx, &pb.Topic{Name: deliveryTopic + "-dead"})
	must(err)
	// Dead-lettering is off until UpdateSubscription sets the policy.
	dead := d.subscription("dead-lettered", func(*pb.Subscription) {})
	policy := &pb.DeadLetterPolicy{DeadLetterTopic: deliveryTopic + "-dead", MaxDeliveryAttempts: 5}
	_, err = subscriber.UpdateSubscription(ctx, &pb.UpdateSubscriptionRequest{Subscription: &pb.Subscription{Name: dead, DeadLetterPolicy: policy}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dead_letter_policy"}}})
	must(err)
	d.publish("", "poison")
	attempts := []int32{}
	for received := d.pull(dead, time.Second); len(received) > 0; received = d.pull(dead, 500*time.Millisecond) {
		attempts = append(attempts, received[0].GetDeliveryAttempt())
		d.deadline(dead, received[0].GetAckId(), 0)
	}
	check(len(attempts) == 5 && attempts[4] == 5, "emulator dead-lettering changed")
	// An explicit nack re-queues an ordered message behind later ones; an
	// expired deadline redelivers in key order.
	ordered := d.subscription("ordered", func(s *pb.Subscription) { s.EnableMessageOrdering = true })
	d.publish("key", "1")
	d.publish("key", "2")
	received := d.pull(ordered, time.Second)
	d.deadline(ordered, received[0].GetAckId(), 0)
	afterNack := first(d.pull(ordered, time.Second))
	check(afterNack == "2", "emulator ordered nack behavior changed; revisit the provider's nack rewrite")
	expiring := d.subscription("expiring", func(s *pb.Subscription) { s.EnableMessageOrdering = true })
	d.publish("other", "a")
	d.publish("other", "b")
	received = d.pull(expiring, time.Second)
	d.deadline(expiring, received[0].GetAckId(), 1)
	afterExpiry := first(d.pull(expiring, 5*time.Second))
	check(afterExpiry == "a", "emulator no longer redelivers an expired ordered message first")
	// Retry backoff is accepted but not applied.
	retried := d.subscription("retried", func(s *pb.Subscription) {
		s.RetryPolicy = &pb.RetryPolicy{MinimumBackoff: durationpb.New(5 * time.Second)}
	})
	d.publish("", "retry")
	received = d.pull(retried, time.Second)
	d.deadline(retried, received[0].GetAckId(), 0)
	check(len(d.pull(retried, 500*time.Millisecond)) == 1, "emulator now applies retry backoff; the provider would double it")
	_, err = subscriber.CreateSubscription(ctx, &pb.Subscription{Name: "projects/backend-probe/subscriptions/bad-filter", Topic: deliveryTopic, Filter: "attributes.k = ="})
	check(status.Code(err) == codes.Unknown, "emulator filter error changed")
	return map[string]any{"dead_letter_attempts": attempts, "ordered_after_nack": afterNack, "ordered_after_expiry": afterExpiry, "retry_backoff_applied": false, "invalid_filter": status.Code(err).String()}
}
