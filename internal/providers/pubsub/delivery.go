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

// maxTrackedDeliveries bounds each per-subscription tracking map. Past the
// bound, an untracked nack uses the minimum backoff, an untracked message
// reports delivery attempt 1, and a repeated exactly-once ack reaches the
// emulator.
const maxTrackedDeliveries = 10000

// delivery is the provider-owned delivery configuration of one subscription.
//
// The emulator implements dead-lettering but not IAM, so the provider owns the
// requested dead-letter policy and forwards it to the emulator only while the
// service agent holds both grants. Without them GCP still counts and reports
// delivery attempts and redelivers past max_delivery_attempts, but never
// forwards (recording pubsub-delivery-features). The emulator counts attempts
// only while it holds the policy, so the provider counts deliveries itself
// while the policy is withheld.
//
// The emulator ignores retry backoff and re-queues a nacked ordered message
// behind later ones, so the provider rewrites nacks: an ordered nack becomes a
// one-second deadline (the emulator's expiry path redelivers in key order), and
// a retry policy turns it into the backoff delay.
//
// On an exactly-once subscription GCP answers OK to a unary Acknowledge of an
// ack ID it already acknowledged, where the emulator fails it, so the provider
// remembers successful acknowledgements and does not forward them again.
type delivery struct {
	ordered     bool
	exactlyOnce bool
	retry       *pb.RetryPolicy
	deadLetter  *pb.DeadLetterPolicy

	mu       sync.Mutex
	active   bool                // dead-letter policy currently forwarded to the emulator
	messages map[string]string   // ack ID -> message ID, retry and dead-letter policies
	failures map[string]int      // message ID -> consecutive nacks
	attempts map[string]int32    // message ID -> deliveries counted while the policy is withheld
	acked    map[string]struct{} // ack IDs acknowledged on an exactly-once subscription
}

// newDelivery captures the delivery settings a create request carries, or nil
// when the subscription needs no provider delivery state.
func newDelivery(subscription *pb.Subscription) *delivery {
	if !subscription.GetEnableMessageOrdering() && !subscription.GetEnableExactlyOnceDelivery() && subscription.GetRetryPolicy() == nil && subscription.GetDeadLetterPolicy() == nil {
		return nil
	}
	d := &delivery{ordered: subscription.GetEnableMessageOrdering(), exactlyOnce: subscription.GetEnableExactlyOnceDelivery(),
		messages: map[string]string{}, failures: map[string]int{}, attempts: map[string]int32{}, acked: map[string]struct{}{}}
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

// tracks reports whether a subscription follows individual messages, for
// retry backoff or dead-letter attempt counts.
func (d *delivery) tracks() bool {
	return d != nil && (d.retry != nil || d.deadLetter != nil)
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

// track remembers which message each ack ID delivered and, on a dead-letter
// subscription, stamps the provider's attempt count on deliveries the emulator
// did not count. A nonzero emulator count means the emulator held the policy
// for that delivery, so it is kept.
func (p *Provider) track(subscription string, received []*pb.ReceivedMessage) {
	d := p.deliveryOf(subscription)
	if !d.tracks() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, message := range received {
		id := message.GetMessage().GetMessageId()
		if d.deadLetter != nil && message.GetDeliveryAttempt() == 0 {
			message.DeliveryAttempt = d.count(id)
		}
		if len(d.messages) < maxTrackedDeliveries {
			d.messages[message.GetAckId()] = id
		}
	}
}

// count records one more delivery of a message and returns its attempt number.
func (d *delivery) count(id string) int32 {
	attempts, found := d.attempts[id]
	if !found && len(d.attempts) >= maxTrackedDeliveries {
		return 1
	}
	d.attempts[id] = attempts + 1
	return attempts + 1
}

// acknowledged forgets acknowledged deliveries with their failure and attempt
// counts.
func (p *Provider) acknowledged(subscription string, ackIDs []string) {
	d := p.deliveryOf(subscription)
	if !d.tracks() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ackIDs {
		if message, ok := d.messages[id]; ok {
			delete(d.failures, message)
			delete(d.attempts, message)
			delete(d.messages, id)
		}
	}
}

// forwardAcknowledge forwards a unary Acknowledge. On an exactly-once
// subscription, ack IDs the provider already acknowledged succeed without
// reaching the emulator, as GCP answers OK for them. Unknown and expired ack
// IDs still fail with the emulator's EXACTLY_ONCE_ACKID_FAILURE details, as
// the exactly-once guide describes for invalid ack IDs. A successful ack
// forgets the messages' tracking state.
func (p *Provider) forwardAcknowledge(ctx context.Context, b driver, request *pb.AcknowledgeRequest) (proto.Message, error) {
	d := p.deliveryOf(request.GetSubscription())
	once := d != nil && d.exactlyOnce
	forwarded := request
	if once {
		forwarded = &pb.AcknowledgeRequest{Subscription: request.GetSubscription(), AckIds: d.unacknowledged(request.GetAckIds())}
		if len(forwarded.GetAckIds()) == 0 {
			return &emptypb.Empty{}, nil
		}
	}
	response, err := invoke(ctx, b, subscriber+"Acknowledge", forwarded)
	if err != nil {
		return response, err
	}
	if once {
		d.remember(forwarded.GetAckIds())
	}
	p.acknowledged(request.GetSubscription(), request.GetAckIds())
	return response, nil
}

// unacknowledged returns the ack IDs this exactly-once subscription has not
// yet acknowledged.
func (d *delivery) unacknowledged(ackIDs []string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	pending := []string{}
	for _, id := range ackIDs {
		if _, done := d.acked[id]; !done {
			pending = append(pending, id)
		}
	}
	return pending
}

// remember records successfully acknowledged ack IDs, up to the tracking bound.
func (d *delivery) remember(ackIDs []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ackIDs {
		if len(d.acked) < maxTrackedDeliveries {
			d.acked[id] = struct{}{}
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
	message, tracked := d.messages[ackID]
	delete(d.messages, ackID)
	if d.retry != nil {
		failures := 1
		if tracked {
			d.failures[message]++
			failures = d.failures[message]
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
