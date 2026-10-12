package pubsub

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Service-agent permission keys for dead-letter forwarding. GCP's
// DeadLetterPolicy reference requires the subscription project's Pub/Sub
// service agent to publish to the dead-letter topic; the dead-letter guide adds
// subscriber on the source subscription so the agent can acknowledge.
const (
	deadLetterPublish     = "pubsub.deadLetter.publish"
	deadLetterAcknowledge = "pubsub.deadLetter.acknowledge"
)

// maxTrackedDeliveries bounds the per-subscription ack-ID map that counts
// consecutive nacks for retry backoff. Untracked nacks use the minimum backoff.
const maxTrackedDeliveries = 10000

// delivery is the provider-owned delivery configuration of one subscription.
//
// The emulator implements dead-lettering but not IAM, so the provider owns the
// requested dead-letter policy and forwards it to the emulator only while the
// service agent holds both grants ("Pub/Sub only counts delivery attempts when
// a dead-letter topic is configured correctly", dead-letter topics guide). The
// emulator ignores retry backoff and re-queues a nacked ordered message behind
// later ones, so the provider rewrites nacks: an ordered nack becomes a
// one-second deadline (the emulator's expiry path redelivers in key order), and
// a retry policy turns it into the backoff delay.
type delivery struct {
	ordered    bool
	retry      *pb.RetryPolicy
	deadLetter *pb.DeadLetterPolicy

	mu       sync.Mutex
	active   bool              // dead-letter policy currently forwarded to the emulator
	messages map[string]string // ack ID -> message ID, retry policies only
	failures map[string]int    // message ID -> consecutive nacks
}

// newDelivery captures the delivery settings a create request carries, or nil
// when the subscription needs no provider delivery state.
func newDelivery(subscription *pb.Subscription) *delivery {
	if !subscription.GetEnableMessageOrdering() && subscription.GetRetryPolicy() == nil && subscription.GetDeadLetterPolicy() == nil {
		return nil
	}
	d := &delivery{ordered: subscription.GetEnableMessageOrdering(), messages: map[string]string{}, failures: map[string]int{}}
	if retry := subscription.GetRetryPolicy(); retry != nil {
		d.retry = proto.CloneOf(retry)
	}
	if policy := subscription.GetDeadLetterPolicy(); policy != nil {
		d.deadLetter = proto.CloneOf(policy)
		if d.deadLetter.GetMaxDeliveryAttempts() == 0 {
			d.deadLetter.MaxDeliveryAttempts = 5 // Reference default; the emulator echoes 5 as well.
		}
	}
	return d
}

func (p *Provider) deliveryOf(subscription string) *delivery {
	value, ok := p.deliveries.Load(canonical(subscription))
	if !ok {
		return nil
	}
	d, _ := value.(*delivery)
	return d
}

// serviceAgent is the per-project Pub/Sub service agent principal.
func (p *Provider) serviceAgent(ctx context.Context, call gateway.Call) (gateway.Call, error) {
	project, ok := p.index.Get(provider.ProjectResource(call.Project))
	if !ok || project.Number == "" {
		return call, gap("the Pub/Sub service agent needs the numeric resource project number")
	}
	call.Context = ctx
	call.Principal = gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:service-" + project.Number + "@gcp-sa-pubsub.iam.gserviceaccount.com", Project: call.Project, Scopes: []string{identity.CloudPlatform}}
	return call, nil
}

// deadLetterPermitted reports whether the service agent may publish to the
// dead-letter topic and acknowledge on the source subscription. Any denial,
// gap, or missing resource means the policy is not configured correctly.
func (p *Provider) deadLetterPermitted(ctx context.Context, call gateway.Call, subscription string, policy *pb.DeadLetterPolicy) bool {
	agent, err := p.serviceAgent(ctx, call)
	if err != nil {
		return false
	}
	checks := []struct{ method, resource string }{{deadLetterPublish, policy.GetDeadLetterTopic()}, {deadLetterAcknowledge, subscription}}
	for _, check := range checks {
		if _, found := p.index.Get(canonical(check.resource)); !found {
			return false
		}
		agent.Method, agent.Resource = check.method, check.resource
		if _, err := p.iam.Authorize(agent, iam.Target{Name: canonical(check.resource)}, nil); err != nil {
			return false
		}
	}
	return true
}

// syncDeadLetter re-evaluates the service agent's grants and forwards or clears
// the emulator's dead-letter policy when the outcome changed. It runs before
// every pull, ack, nack, stream request, and push delivery, so a grant change
// takes effect at the next delivery decision.
func (p *Provider) syncDeadLetter(ctx context.Context, b driver, call gateway.Call, subscription string) error {
	d := p.deliveryOf(subscription)
	if d == nil || d.deadLetter == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	want := p.deadLetterPermitted(ctx, call, subscription, d.deadLetter)
	if want == d.active {
		return nil
	}
	update := &pb.Subscription{Name: subscription}
	if want {
		update.DeadLetterPolicy = d.deadLetter
	}
	request := &pb.UpdateSubscriptionRequest{Subscription: update, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"dead_letter_policy"}}}
	if _, err := invoke(ctx, b, subscriber+"UpdateSubscription", request); err != nil {
		return fmt.Errorf("synchronize the dead-letter policy: %w", err)
	}
	d.active = want
	return nil
}

// track remembers which message each ack ID delivered, for retry backoff.
func (p *Provider) track(subscription string, received []*pb.ReceivedMessage) {
	d := p.deliveryOf(subscription)
	if d == nil || d.retry == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, message := range received {
		if len(d.messages) >= maxTrackedDeliveries {
			return
		}
		d.messages[message.GetAckId()] = message.GetMessage().GetMessageId()
	}
}

// acknowledged forgets acknowledged deliveries and their failure counts.
func (p *Provider) acknowledged(subscription string, ackIDs []string) {
	d := p.deliveryOf(subscription)
	if d == nil || d.retry == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ackIDs {
		if message, ok := d.messages[id]; ok {
			delete(d.failures, message)
			delete(d.messages, id)
		}
	}
}

// nackDeadline returns the ack deadline a nack of ackID is forwarded with.
func (p *Provider) nackDeadline(subscription, ackID string) int32 {
	d := p.deliveryOf(subscription)
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var seconds int32
	if d.retry != nil {
		failures := 1
		if message, ok := d.messages[ackID]; ok {
			d.failures[message]++
			failures = d.failures[message]
			delete(d.messages, ackID)
		}
		seconds = backoff(d.retry, failures)
	}
	if d.ordered && seconds < 1 {
		seconds = 1
	}
	return seconds
}

// backoff doubles the minimum backoff for each consecutive failure, capped at
// the maximum. Google's retry-policy guide gives no formula ("for each
// consecutive message failure, more time is added to the delay, up to a
// maximum"); the defaults are the RetryPolicy reference's 10 and 600 seconds.
func backoff(policy *pb.RetryPolicy, failures int) int32 {
	minimum, maximum := 10*time.Second, 600*time.Second
	if policy.GetMinimumBackoff() != nil {
		minimum = policy.GetMinimumBackoff().AsDuration()
	}
	if policy.GetMaximumBackoff() != nil {
		maximum = policy.GetMaximumBackoff().AsDuration()
	}
	delay := float64(minimum) * math.Pow(2, float64(min(failures-1, 16)))
	delay = math.Min(delay, float64(maximum))
	return int32(math.Ceil(time.Duration(delay).Seconds()))
}

// rewriteNacks returns the deadline list for a ModifyAckDeadline-shaped batch:
// nacks (zero deadlines) take the subscription's ordered or backoff deadline.
func (p *Provider) rewriteNacks(subscription string, ackIDs []string, seconds []int32) []int32 {
	rewritten := append([]int32(nil), seconds...)
	for i, value := range rewritten {
		if value == 0 {
			rewritten[i] = p.nackDeadline(subscription, ackIDs[i])
		}
	}
	return rewritten
}

// forwardModifyAckDeadline forwards a unary ModifyAckDeadline. Nacks on a
// subscription with provider delivery state are rewritten, and the request is
// split by deadline because the RPC carries one deadline for all its IDs.
func (p *Provider) forwardModifyAckDeadline(ctx context.Context, b driver, request *pb.ModifyAckDeadlineRequest) (proto.Message, error) {
	if request.GetAckDeadlineSeconds() != 0 || p.deliveryOf(request.GetSubscription()) == nil {
		return invoke(ctx, b, subscriber+"ModifyAckDeadline", request)
	}
	seconds := p.rewriteNacks(request.GetSubscription(), request.GetAckIds(), make([]int32, len(request.GetAckIds())))
	groups := map[int32][]string{}
	for i, id := range request.GetAckIds() {
		groups[seconds[i]] = append(groups[seconds[i]], id)
	}
	for _, deadline := range slices.Sorted(maps.Keys(groups)) {
		forwarded := &pb.ModifyAckDeadlineRequest{Subscription: request.GetSubscription(), AckIds: groups[deadline], AckDeadlineSeconds: deadline}
		if _, err := invoke(ctx, b, subscriber+"ModifyAckDeadline", forwarded); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

// validateDeadLetter checks a dead-letter policy before any backend call: the
// provider may withhold the policy from the emulator, so it cannot rely on the
// emulator's own checks. Ranges and messages follow the emulator, which matches
// the DeadLetterPolicy reference (0 means the default 5; otherwise 5 to 100).
func (p *Provider) validateDeadLetter(call gateway.Call, policy *pb.DeadLetterPolicy) error {
	if policy == nil {
		return nil
	}
	project, err := resourceName(policy.GetDeadLetterTopic(), "topics")
	if err != nil {
		return err
	}
	if project != call.Project {
		return gap("cross-project dead-letter topics")
	}
	attempts := policy.GetMaxDeliveryAttempts()
	if attempts != 0 && attempts < 5 {
		return status.Errorf(codes.OutOfRange, "The value for max_delivery_attempts is too small. You passed %d in the request, but the minimum value is 5.", attempts)
	}
	if attempts > 100 {
		return status.Errorf(codes.OutOfRange, "The value for max_delivery_attempts is too large. You passed %d in the request, but the maximum value is 100.", attempts)
	}
	if topic, found := p.index.Get(canonical(policy.GetDeadLetterTopic())); !found || topic.Kind != "topic" {
		return status.Error(codes.NotFound, "Dead letter topic not found")
	}
	return nil
}
