// Package pubsub wraps the official emulator with scoped lifecycle and IAM.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("pub/sub requires registry, backend manager, index, and iam")

// serviceName is the registered service key for Pub/Sub backends and index entries.
const serviceName = "pubsub"

type Provider struct {
	manager  *backend.Manager
	index    *state.Index
	iam      *iam.Engine
	poisoned sync.Map // unresolved control-plane mutations, cleared only on reset
	tokens   *identity.IDTokens
	pushes   sync.Map // canonical subscription -> provider-owned immutable config and worker
	labels   sync.Map // canonical subscription or snapshot -> provider-owned labels map
	// canonical subscription -> provider-owned delivery policy (delivery.go)
	deliveries sync.Map
}

func New(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	p, err := newProvider(registry, manager, index, engine, func(scope state.Scope) (backend.Backend, error) {
		return &emulator{config: config, scope: scope}, nil
	})
	if err == nil {
		p.tokens = config.IDTokens
	}
	return p, err
}
func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	p := &Provider{manager: manager, index: index, iam: engine}
	if err := provider.RegisterPermissions(engine, permissions); err != nil {
		return nil, fmt.Errorf("pubsub permissions: %w", err)
	}
	if err := manager.Register(serviceName, backend.Definition{Validate: state.GlobalScope, New: factory, AfterReset: p.afterReset}); err != nil {
		return nil, fmt.Errorf("register the pub/sub backend: %w", err)
	}
	if err := p.registerMethods(registry); err != nil {
		return nil, err
	}
	return p, nil
}

// afterReset clears provider-owned state once the backend reset has succeeded.
func (p *Provider) afterReset(scope state.Scope) error {
	if err := p.resetPush(scope); err != nil {
		return err
	}
	prefix := canonical("projects/" + scope.Project + "/")
	for _, owned := range []*sync.Map{&p.labels, &p.deliveries} {
		owned.Range(func(key, _ any) bool {
			if name, ok := key.(string); ok && strings.HasPrefix(name, prefix) {
				owned.Delete(key)
			}
			return true
		})
	}
	p.iam.ClearScope(serviceName, scope)
	p.index.RemoveScope(serviceName, scope)
	p.poisoned.Delete(scope)
	return nil
}

// registerMethods binds the gRPC, REST, IAM policy, and streaming surfaces.
func (p *Provider) registerMethods(registry *gateway.Registry) error {
	for method := range permissions {
		if !unaryMethod(method) {
			continue
		}
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	for method := range restTwins {
		if err := registry.Register(method, p.handleRest); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	if err := p.registerRestPermissions(); err != nil {
		return err
	}
	return p.registerPolicyAndStream(registry)
}

// unaryMethod reports whether a permission key is a unary gRPC method served by
// handle. Permission-only keys, streaming pull, and the IAM policy surface are
// registered separately.
func unaryMethod(method string) bool {
	return strings.HasPrefix(method, "/") && method != subscriber+"StreamingPull" && !strings.HasPrefix(method, policyRPC)
}

// registerPolicyAndStream binds the IAM policy RPCs and streaming pull.
func (p *Provider) registerPolicyAndStream(registry *gateway.Registry) error {
	for _, method := range []string{"GetIamPolicy", "SetIamPolicy"} {
		if err := registry.Register(policyRPC+method, p.policy); err != nil {
			return fmt.Errorf("register policy %s: %w", method, err)
		}
	}
	if err := registry.RegisterStream(subscriber+"StreamingPull", p.stream); err != nil {
		return fmt.Errorf("register the streaming pull method: %w", err)
	}
	return nil
}

func scopeOf(call gateway.Call) state.Scope {
	return state.Scope{Project: call.Project, Location: state.Global}
}
func keyOf(call gateway.Call) backend.Key {
	return backend.Key{Service: serviceName, Scope: scopeOf(call)}
}
func projectMethod(method string) bool {
	switch method {
	case publisher + "CreateTopic", subscriber + "CreateSubscription", subscriber + "CreateSnapshot",
		publisher + "ListTopics", subscriber + "ListSubscriptions", subscriber + "ListSnapshots":
		return true
	}
	return false
}

// authorize runs every check that gates a Pub/Sub call, in the order that
// decides which error a caller sees first. It never touches the backend.
func (p *Provider) authorize(call gateway.Call, request proto.Message) error {
	if call.Principal.State == gateway.Anonymous {
		return status.Error(codes.Unauthenticated, "Pub/Sub requires a named local principal")
	}
	if _, failed := p.poisoned.Load(scopeOf(call)); failed {
		return status.Error(codes.FailedPrecondition, "Pub/Sub mutation outcome is unresolved; reset this scope")
	}
	if err := p.authorizeCall(call); err != nil {
		return err
	}
	if err := p.authorizeRelated(call, request); err != nil {
		return err
	}
	return p.checkTarget(call)
}

// authorizeRelated checks the second resource a request names: the topic a
// subscription attaches to, a snapshot's source subscription, or a seek's
// snapshot (access-control table).
func (p *Provider) authorizeRelated(call gateway.Call, request proto.Message) error {
	switch r := request.(type) {
	case *pb.Subscription:
		if err := p.authorizeAttachment(call, r); err != nil {
			return err
		}
		return p.validateDeadLetter(call, r.GetDeadLetterPolicy())
	case *pb.CreateSnapshotRequest:
		return p.authorizeOn(call, r.GetSubscription(), "subscription", snapshotSource)
	case *pb.SeekRequest:
		if r.GetTime() != nil {
			return nil
		}
		return p.authorizeOn(call, r.GetSnapshot(), "snapshot", snapshotSeek)
	}
	return nil
}

// authorizeOn evaluates one permission key on a same-project resource the
// provider index holds.
func (p *Provider) authorizeOn(call gateway.Call, name, kind, method string) error {
	project, err := resourceName(name, kind+"s")
	if err != nil {
		return err
	}
	if project != call.Project {
		return gap("cross-project " + kind + "s")
	}
	resource, found := p.index.Get(canonical(name))
	if !found {
		return status.Error(codes.NotFound, strings.ToUpper(kind[:1])+kind[1:]+" does not exist")
	}
	if resource.Service != serviceName || resource.Kind != kind || resource.Scope != scopeOf(call) {
		return gap(kind + " ownership disagrees with the index")
	}
	related := call
	related.Method, related.Resource = method, name
	decision, err := p.iam.Authorize(related, iam.Target{Name: resource.Name}, nil)
	_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-"+kind+"-decision-id", decision.ID))
	if err != nil {
		return fmt.Errorf("authorize %s for %s: %w", kind, call.Method, err)
	}
	return nil
}

// checkTarget rejects pulls from push subscriptions and calls on resources the
// index does not hold.
func (p *Provider) checkTarget(call gateway.Call) error {
	if call.Method == subscriber+"Pull" || call.Method == subscriber+"StreamingPull" {
		if _, push := p.pushes.Load(canonical(call.Resource)); push {
			return status.Error(codes.FailedPrecondition, "Cannot pull from a push subscription")
		}
	}
	if projectMethod(call.Method) {
		return nil
	}
	if _, found := p.index.Get(canonical(call.Resource)); !found {
		return status.Error(codes.NotFound, "Pub/Sub resource does not exist")
	}
	return nil
}

// authorizeCall checks the method permission against the indexed resource, or
// against its project for project-level methods and not-yet-indexed resources.
func (p *Provider) authorizeCall(call gateway.Call) error {
	project, found := p.index.Get(provider.ProjectResource(call.Project))
	if !found || project.Service != "resourcemanager" || project.Kind != "project" {
		return gap("configure the resource project hierarchy")
	}
	target := iam.Target{Name: project.Name}
	if !projectMethod(call.Method) {
		if resource, found := p.index.Get(canonical(call.Resource)); found {
			if resource.Service != serviceName || resource.Scope != scopeOf(call) || resource.Parent != project.Name {
				return gap("resource ownership disagrees with the index")
			}
			target.Name = resource.Name
		}
	}
	decision, err := p.iam.Authorize(call, target, nil)
	_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-decision-id", decision.ID))
	if err != nil {
		return fmt.Errorf("authorize %s: %w", call.Method, err)
	}
	return nil
}

// authorizeAttachment checks that a subscription may attach to its topic and,
// for push subscriptions, act as the push identity.
func (p *Provider) authorizeAttachment(call gateway.Call, subscription *pb.Subscription) error {
	project, err := resourceName(subscription.GetTopic(), "topics")
	if err != nil {
		return err
	}
	if project != call.Project {
		return gap("cross-project subscriptions")
	}
	if err := p.authorizeOn(call, subscription.GetTopic(), "topic", attach); err != nil {
		return err
	}
	if subscription.GetPushConfig() == nil {
		return nil
	}
	return p.authorizePush(call, subscription.GetPushConfig())
}

func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub request")
	}
	if err := p.preflight(call, request); err != nil {
		return nil, err
	}
	// Pull blocks, so it runs as a session beside other operations.
	use := p.manager.Use
	if call.Method == subscriber+"Pull" {
		use = p.manager.Session
	}
	var response proto.Message
	err := use(call.Context, keyOf(call), func(session context.Context, active backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		var err error
		response, err = p.execute(call, request, active)
		return err
	})
	return response, err
}

// execute re-authorizes under the backend gate, forwards the call, and settles
// any index-owned mutation it caused.
func (p *Provider) execute(call gateway.Call, request proto.Message, active backend.Backend) (proto.Message, error) {
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	b, err := driverOf(active)
	if err != nil {
		return nil, err
	}
	creating, deleting := mutationKind(call.Method)
	if creating {
		if _, found := p.index.Get(canonical(call.Resource)); found {
			return nil, status.Error(codes.AlreadyExists, "Pub/Sub resource already exists")
		}
	}
	forwarded, push, err := p.splitPushConfig(call, request)
	if err != nil {
		return nil, err
	}
	forwarded, err = withoutDeadLetter(forwarded)
	if err != nil {
		return nil, err
	}
	response, err := p.forward(call, b, request, forwarded)
	if creating || deleting {
		return response, p.settleMutation(call.Context, b, call, response, request, push, creating, err)
	}
	if err == nil {
		p.restorePush(response)
	}
	return response, err
}

// forward invokes the backend. Delivery calls first synchronize the
// dead-letter policy; nacks take the provider's delivery deadline, and
// exactly-once acks skip IDs already acknowledged. A pull re-authorizes
// afterwards so a grant revoked while the pull blocked does not release
// messages.
func (p *Provider) forward(call gateway.Call, b driver, request, forwarded proto.Message) (proto.Message, error) {
	switch r := forwarded.(type) {
	case *pb.PullRequest:
		if err := p.syncDeadLetter(call.Context, b, call, r.GetSubscription()); err != nil {
			return nil, err
		}
	case *pb.ModifyAckDeadlineRequest:
		if err := p.syncDeadLetter(call.Context, b, call, r.GetSubscription()); err != nil {
			return nil, err
		}
		return p.forwardModifyAckDeadline(call.Context, b, r)
	case *pb.AcknowledgeRequest:
		return p.forwardAcknowledge(call.Context, b, r)
	}
	response, err := invoke(call.Context, b, call.Method, forwarded)
	if subscription, ok := request.(*pb.Subscription); ok && subscription.GetFilter() != "" && status.Code(err) == codes.Unknown {
		// The emulator answers an unparsable filter with UNKNOWN; GCP rejects it as invalid.
		return nil, provider.Invalid("Invalid filter expression")
	}
	if err != nil {
		return response, err
	}
	return response, p.afterDelivery(call, request, response)
}

// afterDelivery re-authorizes a pull that may have blocked while a grant was
// revoked, then tracks the delivered messages for retry backoff and
// dead-letter attempt counts.
func (p *Provider) afterDelivery(call gateway.Call, request, response proto.Message) error {
	r, ok := request.(*pb.PullRequest)
	if !ok {
		return nil
	}
	if err := p.authorize(call, request); err != nil {
		return err
	}
	if pulled, ok := response.(*pb.PullResponse); ok {
		p.track(r.GetSubscription(), pulled.GetReceivedMessages())
	}
	return nil
}

// withoutDeadLetter strips the dead-letter policy from a forwarded create; the
// provider forwards it later, only while the service agent holds its grants.
func withoutDeadLetter(request proto.Message) (proto.Message, error) {
	subscription, ok := request.(*pb.Subscription)
	if !ok || subscription.GetDeadLetterPolicy() == nil {
		return request, nil
	}
	forwarded, err := cloneMessage(subscription)
	if err != nil {
		return nil, err
	}
	forwarded.DeadLetterPolicy = nil
	return forwarded, nil
}

// preflight applies the checks that must run before any backend session opens.
func (p *Provider) preflight(call gateway.Call, request proto.Message) error {
	if call.Method != publisher+"ListTopics" && call.Method != subscriber+"ListSubscriptions" && call.Method != subscriber+"ListSnapshots" {
		project, err := resourceName(call.Resource, "")
		if err != nil {
			return err
		}
		if project != call.Project {
			return provider.Invalid("Resource project mismatch")
		}
	}
	if err := validate(request); err != nil {
		return err
	}
	return p.authorize(call, request)
}

// mutationKind classifies a method by whether it changes index-owned state.
func mutationKind(method string) (bool, bool) {
	creating := method == publisher+"CreateTopic" || method == subscriber+"CreateSubscription" || method == subscriber+"CreateSnapshot"
	deleting := method == publisher+"DeleteTopic" || method == subscriber+"DeleteSubscription" || method == subscriber+"DeleteSnapshot"
	return creating, deleting
}

// splitPushConfig strips the push configuration from the forwarded request: the
// unauthenticated emulator must never make HTTP deliveries itself. The provider
// owns the push worker and mints its identity.
func (p *Provider) splitPushConfig(call gateway.Call, request proto.Message) (proto.Message, *pb.PushConfig, error) {
	subscription, ok := request.(*pb.Subscription)
	if !ok || subscription.GetPushConfig() == nil {
		return request, nil, nil
	}
	if p.pushCount(scopeOf(call)) >= maxPushSubscriptions {
		return nil, nil, status.Error(codes.ResourceExhausted, "Local limit is 32 push subscriptions per project")
	}
	push, err := cloneMessage(subscription.GetPushConfig())
	if err != nil {
		return nil, nil, err
	}
	forwarded, err := cloneMessage(subscription)
	if err != nil {
		return nil, nil, err
	}
	forwarded.PushConfig = nil
	return forwarded, push, nil
}

// settleMutation reconciles the provider index and push worker after a create or
// delete. An ambiguous backend outcome is resolved by reading the resource back
// rather than assuming success.
func (p *Provider) settleMutation(session context.Context, b driver, call gateway.Call, response proto.Message, request proto.Message, push *pb.PushConfig, creating bool, invokeErr error) error {
	if invokeErr == nil {
		return p.commitMutation(session, call, response, request, push, creating)
	}
	if !ambiguous(invokeErr) {
		return invokeErr
	}
	return p.resolveMutation(session, b, call, push, creating, invokeErr)
}

// commitMutation mirrors a successful create or delete into the provider index,
// records provider-owned labels and delivery policy, and starts the push worker
// for a created subscription.
func (p *Provider) commitMutation(session context.Context, call gateway.Call, response proto.Message, request proto.Message, push *pb.PushConfig, creating bool) error {
	if !creating {
		return p.forget(call)
	}
	if err := p.publish(call); err != nil {
		return err
	}
	if push != nil {
		if err := p.startPush(session, call, push); err != nil {
			return err
		}
	}
	var labels map[string]string
	switch r := request.(type) {
	case *pb.Subscription:
		labels = r.GetLabels()
		if d := newDelivery(r); d != nil {
			p.deliveries.Store(canonical(call.Resource), d)
		}
	case *pb.CreateSnapshotRequest:
		labels = r.GetLabels() // The emulator drops snapshot labels.
	}
	if len(labels) > 0 {
		p.labels.Store(canonical(call.Resource), labels)
	}
	p.restorePush(response)
	return nil
}

// resolveMutation re-reads an ambiguous mutation's resource to decide whether
// the backend landed it before the error, and starts the push worker only then.
func (p *Provider) resolveMutation(session context.Context, b driver, call gateway.Call, push *pb.PushConfig, creating bool, invokeErr error) error {
	p.reconcile(session, b, call)
	if creating && push != nil && p.mutationLanded(call) {
		if err := p.startPush(session, call, push); err != nil {
			return err
		}
	}
	return invokeErr
}

// mutationLanded reports whether an ambiguous create is confirmed in the index
// and this scope has not been marked poisoned by a previous failure.
func (p *Provider) mutationLanded(call gateway.Call) bool {
	if _, failed := p.poisoned.Load(scopeOf(call)); failed {
		return false
	}
	_, found := p.index.Get(canonical(call.Resource))
	return found
}

// maxPushSubscriptions bounds the push workers one project may hold.
const maxPushSubscriptions = 32

func invoke(ctx context.Context, b driver, method string, request proto.Message) (proto.Message, error) {
	for _, descriptor := range generated.PubSubMethods {
		if descriptor.Method == method {
			response := descriptor.NewResponse()
			// Incoming auth/trace/quota metadata is never forwarded to upstream.
			err := b.client().Invoke(metadata.NewOutgoingContext(ctx, metadata.MD{}), method, request, response)
			return response, err
		}
	}
	return nil, status.Error(codes.Internal, "Missing generated Pub/Sub descriptor")
}
func (p *Provider) publish(call gateway.Call) error {
	kind := "topic"
	if strings.Contains(call.Resource, "/subscriptions/") {
		kind = "subscription"
	} else if strings.Contains(call.Resource, "/snapshots/") {
		kind = "snapshot"
	}
	if err := p.index.Put(serviceName, state.Resource{Name: canonical(call.Resource), Service: serviceName, Kind: kind, Parent: provider.ProjectResource(call.Project), Scope: scopeOf(call)}); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return status.Error(codes.FailedPrecondition, "Backend mutation could not be indexed; reset this scope")
	}
	return nil
}
func (p *Provider) forget(call gateway.Call) error {
	p.labels.Delete(canonical(call.Resource))
	p.deliveries.Delete(canonical(call.Resource))
	if err := p.stopPush(canonical(call.Resource)); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return err
	}
	if err := p.index.Remove(serviceName, scopeOf(call), canonical(call.Resource)); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return status.Error(codes.FailedPrecondition, "Backend deletion could not be indexed; reset this scope")
	}
	p.iam.Forget(canonical(call.Resource))
	return nil
}
func ambiguous(err error) bool {
	return status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Resolve uncertain control-plane outcomes under the same mutation gate. A
// read failure retains the last confirmed index and blocks access until reset.
func (p *Provider) reconcile(parent context.Context, b driver, call gateway.Call) {
	// A cancelled request must not cancel reconciliation, and the Python JVM
	// backend outlives it, so the deadline is derived rather than inherited.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	method, request := publisher+"GetTopic", proto.Message(&pb.GetTopicRequest{Topic: call.Resource})
	if strings.Contains(call.Resource, "/subscriptions/") {
		method, request = subscriber+"GetSubscription", &pb.GetSubscriptionRequest{Subscription: call.Resource}
	} else if strings.Contains(call.Resource, "/snapshots/") {
		method, request = subscriber+"GetSnapshot", &pb.GetSnapshotRequest{Snapshot: call.Resource}
	}
	_, err := invoke(ctx, b, method, request)
	if err == nil {
		_ = p.publish(call)
		return
	}
	if status.Code(err) == codes.NotFound {
		_ = p.forget(call)
		return
	}
	p.poisoned.Store(scopeOf(call), true)
}
