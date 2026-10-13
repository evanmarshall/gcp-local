package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sync"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/messaging/pubsub/validation"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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
	project, err := validation.ResourceName(policy.GetDeadLetterTopic(), "topics")
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

type pushSubscription struct {
	scope  state.Scope
	config *pb.PushConfig
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *Provider) authorizePush(call gateway.Call, config *pb.PushConfig) error {
	if p.tokens == nil {
		return gap("local ID-token issuer is not configured")
	}
	account := iam.AccountResource(call.Project, config.GetOidcToken().GetServiceAccountEmail())
	resource, found := p.index.Get(account)
	if !found || resource.Service != "iam" || resource.Kind != "serviceAccount" || resource.Scope != scopeOf(call) {
		return gap("configure a same-project push service account")
	}
	project, _ := p.index.Get(provider.ProjectResource(call.Project))
	if !regexp.MustCompile(`^[1-9][0-9]{0,19}$`).MatchString(project.Number) {
		return gap("push requires the numeric resource project number")
	}
	call.Method, call.Resource = "pubsub.push.actAs", account
	_, err := p.iam.Authorize(call, iam.Target{Name: account}, nil)
	if err != nil {
		return fmt.Errorf("authorize push identity: %w", err)
	}
	return nil
}

func (p *Provider) pushCount(scope state.Scope) int {
	count := 0
	p.pushes.Range(func(_, value any) bool {
		if worker, ok := pushWorker(value); ok && worker.scope == scope {
			count++
		}
		return true
	})
	return count
}
func (p *Provider) startPush(parent context.Context, call gateway.Call, config *pb.PushConfig) error {
	cloned, err := cloneMessage(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	worker := &pushSubscription{scope: scopeOf(call), config: cloned, cancel: cancel, done: make(chan struct{})}
	p.pushes.Store(canonical(call.Resource), worker)
	go func() {
		defer close(worker.done)
		defer cancel()
		// The subscription was authorized before creation. The worker owns its
		// queue; it mints under the service agent, never the creator or an admin.
		// A session failure ends this worker, so the error is not actionable here.
		_ = p.manager.Session(ctx, keyOf(call), func(ctx context.Context, active backend.Backend) error {
			b, err := driverOf(active)
			if err != nil {
				return err
			}
			p.deliver(ctx, b, call, worker.config)
			return ctx.Err()
		})
	}()
	return nil
}
func (p *Provider) stopPush(name string) error {
	value, ok := p.pushes.Load(name)
	if !ok {
		return nil
	}
	worker, ok := pushWorker(value)
	if !ok {
		p.pushes.Delete(name)
		return nil
	}
	worker.cancel()
	select {
	case <-worker.done:
		p.pushes.Delete(name)
		return nil
	case <-time.After(3 * time.Second):
		return status.Error(codes.FailedPrecondition, "Push worker did not stop; reset this scope")
	}
}
func (p *Provider) resetPush(scope state.Scope) error {
	var result error
	p.pushes.Range(func(key, value any) bool {
		if worker, ok := pushWorker(value); ok && worker.scope == scope {
			name, ok := key.(string)
			if !ok {
				result = errMessageType
				return false
			}
			result = p.stopPush(name)
		}
		return result == nil
	})
	return result
}

// restorePush puts provider-owned configuration back into backend responses:
// push config, labels, and the requested dead-letter policy, which the emulator
// holds only while the service agent's grants allow forwarding.
func (p *Provider) restorePush(response proto.Message) {
	switch response := response.(type) {
	case *pb.Topic:
		p.decorateTopic(response)
	case *pb.ListTopicsResponse:
		for _, topic := range response.GetTopics() {
			p.decorateTopic(topic)
		}
	case *pb.Subscription:
		p.decorateSubscription(response)
	case *pb.ListSubscriptionsResponse:
		for _, subscription := range response.GetSubscriptions() {
			p.decorateSubscription(subscription)
		}
	case *pb.Snapshot:
		response.Labels = p.storedLabels(response.GetName(), response.GetLabels())
	case *pb.ListSnapshotsResponse:
		for _, snapshot := range response.GetSnapshots() {
			snapshot.Labels = p.storedLabels(snapshot.GetName(), snapshot.GetLabels())
		}
	}
}

// decorateSubscription restores one subscription's provider-owned fields.
func (p *Provider) decorateSubscription(subscription *pb.Subscription) {
	if subscription == nil {
		return
	}
	if value, ok := p.pushes.Load(canonical(subscription.GetName())); ok {
		if worker, ok := pushWorker(value); ok {
			subscription.PushConfig, _ = cloneMessage(worker.config)
		}
	}
	subscription.Labels = p.storedLabels(subscription.GetName(), subscription.GetLabels())
	if d := p.deliveryOf(subscription.GetName()); d != nil && d.deadLetter != nil {
		subscription.DeadLetterPolicy = proto.CloneOf(d.deadLetter)
	}
}

// storedLabels returns the provider-owned labels of a resource, or fallback.
func (p *Provider) storedLabels(name string, fallback map[string]string) map[string]string {
	if labels, ok := p.labels.Load(canonical(name)); ok {
		if stored, ok := labels.(map[string]string); ok {
			return maps.Clone(stored)
		}
	}
	return fallback
}

func (p *Provider) pushToken(ctx context.Context, call gateway.Call, config *pb.PushConfig) (string, error) {
	if _, failed := p.poisoned.Load(scopeOf(call)); failed {
		return "", status.Error(codes.FailedPrecondition, "Pub/Sub scope needs reset")
	}
	project, ok := p.index.Get(provider.ProjectResource(call.Project))
	if !ok {
		return "", gap("push project disappeared")
	}
	call.Context = ctx
	call.Principal = gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:service-" + project.Number + "@gcp-sa-pubsub.iam.gserviceaccount.com", Project: call.Project, Scopes: []string{identity.CloudPlatform}}
	audience := config.GetOidcToken().GetAudience()
	if audience == "" {
		audience = config.GetPushEndpoint()
	}
	token, err := p.tokens.Mint(call, iam.AccountResource(call.Project, config.GetOidcToken().GetServiceAccountEmail()), audience)
	if err != nil {
		return "", fmt.Errorf("mint push identity token: %w", err)
	}
	return token, nil
}

func (p *Provider) deliver(ctx context.Context, b driver, call gateway.Call, config *pb.PushConfig) {
	client := pushClient(config.GetPushEndpoint())
	for ctx.Err() == nil {
		if p.syncDeadLetter(ctx, b, call, call.Resource) != nil && !pauseFor(ctx, time.Second) {
			return
		}
		messages, retry := p.pullForDelivery(ctx, b, call, config)
		p.track(call.Resource, messages)
		if retry && !pauseFor(ctx, time.Second) {
			return
		}
		for _, message := range messages {
			if ctx.Err() != nil {
				return
			}
			accepted := p.pushMessage(ctx, call, config, client, message)
			if !p.settleDelivery(ctx, b, call, message.GetAckId(), accepted) {
				return
			}
		}
	}
}

// pushClient builds a client that dials only the configured endpoint and never
// follows redirects, so a push target cannot pivot the worker elsewhere.
func pushClient(endpoint string) *http.Client {
	parsed, _ := url.Parse(endpoint)
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
		if address != parsed.Host {
			return nil, gap("push dial target changed")
		}
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", parsed.Host)
	}}
	return &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// pauseFor waits one interval or until the context ends, and reports whether the
// loop may continue.
func pauseFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pullForDelivery draws one message batch, or reports whether the failure is
// worth retrying after a pause. An empty subscription just idles the worker.
func (p *Provider) pullForDelivery(ctx context.Context, b driver, call gateway.Call, config *pb.PushConfig) ([]*pb.ReceivedMessage, bool) {
	if _, err := p.pushToken(ctx, call, config); err != nil {
		return nil, true
	}
	pullCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	result, err := invoke(pullCtx, b, subscriber+"Pull", &pb.PullRequest{Subscription: call.Resource, MaxMessages: 1})
	cancel()
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, false
		}
		return nil, true
	}
	pulled, ok := result.(*pb.PullResponse)
	if !ok {
		return nil, true
	}
	return p.expire(ctx, b, call.Resource, pulled.GetReceivedMessages()), true
}

// defaultRetention is a subscription's message retention when it sets none
// (Subscription reference).
const defaultRetention = 7 * 24 * time.Hour

// expire drops pulled messages published longer ago than the subscription's
// message retention, by the provider clock, and acknowledges them in the
// emulator, which keeps every message until acknowledged; GCP deletes them.
// A failed acknowledgment leaves them for the next pull, which drops them
// again.
func (p *Provider) expire(ctx context.Context, b driver, subscription string, received []*pb.ReceivedMessage) []*pb.ReceivedMessage {
	retention := defaultRetention
	if value, ok := p.retentions.Load(canonical(subscription)); ok {
		retention, _ = value.(time.Duration)
	}
	cutoff := p.clock().Add(-retention)
	kept := received[:0:0]
	var expired []string
	for _, message := range received {
		if published := message.GetMessage().GetPublishTime(); published != nil && published.AsTime().Before(cutoff) {
			expired = append(expired, message.GetAckId())
			continue
		}
		kept = append(kept, message)
	}
	if len(expired) > 0 {
		_, _ = invoke(context.WithoutCancel(ctx), b, subscriber+"Acknowledge", &pb.AcknowledgeRequest{Subscription: subscription, AckIds: expired})
		p.acknowledged(subscription, expired)
	}
	return kept
}

// pushMessage posts one message to the endpoint and reports whether the
// endpoint accepted it. Failures are retried by nacking, not logged.
func (p *Provider) pushMessage(ctx context.Context, call gateway.Call, config *pb.PushConfig, client *http.Client, message *pb.ReceivedMessage) bool {
	token, err := p.pushToken(ctx, call, config)
	if err != nil {
		return false
	}
	body, ok := pushBody(call.Resource, message)
	if !ok {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.GetPushEndpoint(), bytes.NewReader(body))
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	// Cloud Run counts the subscription's own project's pushes as internal.
	provider.MarkInternal(request, call.Project)
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	_ = response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		return true
	default:
		return false
	}
}

// settleDelivery acknowledges an accepted delivery, or resets the ack deadline
// so the message is redelivered. A failed settle without a pause ends the worker.
func (p *Provider) settleDelivery(ctx context.Context, b driver, call gateway.Call, ackID string, accepted bool) bool {
	ackCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if accepted {
		_, err := invoke(ackCtx, b, subscriber+"Acknowledge", &pb.AcknowledgeRequest{Subscription: call.Resource, AckIds: []string{ackID}})
		if err == nil {
			p.acknowledged(call.Resource, []string{ackID})
			return true
		}
	} else {
		deadline := p.nackDeadline(call.Resource, ackID)
		_, err := invoke(ackCtx, b, subscriber+"ModifyAckDeadline", &pb.ModifyAckDeadlineRequest{Subscription: call.Resource, AckIds: []string{ackID}, AckDeadlineSeconds: deadline})
		if err == nil {
			return true
		}
	}
	return pauseFor(ctx, time.Second)
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
	if err := validation.Stream(first, true); err != nil {
		return nil, err
	}
	if first.GetSubscription() != call.Resource {
		return nil, provider.Invalid("Stream subscription disagrees with request scope")
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
	if err := p.syncDeadLetter(session, b, call, first.GetSubscription()); err != nil {
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
	go p.relayDownstream(call, b, first.GetSubscription(), downstream, upstream, failures)
	go p.relayUpstream(call, b, upstream, downstream, failures, first)
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
// Stream nacks take the provider's delivery deadline, as unary nacks do.
func (p *Provider) relayDownstream(call gateway.Call, b driver, subscription string, downstream gateway.Stream, upstream pb.Subscriber_StreamingPullClient, failures chan<- error) {
	for {
		request := &pb.StreamingPullRequest{}
		if err := downstream.Recv(request); err != nil {
			// A client half-close drains responses until upstream EOF.
			finishDownstream(err, upstream, failures)
			return
		}
		if err := validation.Stream(request, false); err != nil {
			failures <- err
			return
		}
		if err := p.authorize(call, request); err != nil {
			failures <- err
			return
		}
		if len(request.GetModifyDeadlineAckIds()) > 0 {
			if err := p.syncDeadLetter(call.Context, b, call, subscription); err != nil {
				failures <- err
				return
			}
			request.ModifyDeadlineSeconds = p.rewriteNacks(subscription, request.GetModifyDeadlineAckIds(), request.GetModifyDeadlineSeconds())
		}
		p.acknowledged(subscription, request.GetAckIds())
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
func (p *Provider) relayUpstream(call gateway.Call, b driver, upstream pb.Subscriber_StreamingPullClient, downstream gateway.Stream, failures chan<- error, first *pb.StreamingPullRequest) {
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
		pulled := len(response.GetReceivedMessages())
		response.ReceivedMessages = p.expire(call.Context, b, first.GetSubscription(), response.GetReceivedMessages())
		p.track(first.GetSubscription(), response.GetReceivedMessages())
		if pulled > 0 && len(response.GetReceivedMessages()) == 0 {
			continue // every message expired; keepalive answers still go out
		}
		if err := downstream.Send(response); err != nil {
			failures <- err
			return
		}
	}
}

// pushBody encodes the Pub/Sub push envelope or reports why it cannot. The
// envelope carries deliveryAttempt only when a dead-letter policy counts
// attempts (dead-letter topics guide).
func pushBody(subscription string, received *pb.ReceivedMessage) ([]byte, bool) {
	encoded, err := protojson.Marshal(received.GetMessage())
	if err != nil {
		return nil, false
	}
	envelope := map[string]any{"message": json.RawMessage(encoded), "subscription": subscription}
	if attempt := received.GetDeliveryAttempt(); attempt > 0 {
		envelope["deliveryAttempt"] = attempt
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, false
	}
	return body, true
}
