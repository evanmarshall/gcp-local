// Package pubsub wraps the official emulator with scoped lifecycle and IAM.
package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	pubsubapi "github.com/evanmarshall/gcp-local/api/generated/messaging/pubsub"
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
	"google.golang.org/protobuf/proto"
)

var gap = provider.CoverageGap("Pub/Sub")

// canonical is the full resource name of a relative Pub/Sub name.
func canonical(name string) string { return "//pubsub.googleapis.com/" + name }

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
	// canonical schema -> provider-owned Protocol Buffer schema (schema_proto.go)
	schemas sync.Map
	// canonical topic -> its binding to a provider-owned schema
	topicSchemas sync.Map
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
	for _, owned := range []*sync.Map{&p.labels, &p.deliveries, &p.schemas, &p.topicSchemas} {
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
		handler := p.handle
		if strings.HasPrefix(method, schemaService) {
			handler = p.handleSchema
		}
		if err := registry.Register(method, handler); err != nil {
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
	if err := p.registerRestSchemas(registry); err != nil {
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
		publisher + "ListTopics", subscriber + "ListSubscriptions", subscriber + "ListSnapshots",
		schemaService + "CreateSchema", schemaService + "ListSchemas", schemaService + "ValidateSchema", schemaService + "ValidateMessage":
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
// subscription attaches to, a snapshot's source subscription, a seek's
// snapshot (access-control table), or the schema a created topic attaches.
func (p *Provider) authorizeRelated(call gateway.Call, request proto.Message) error {
	switch r := request.(type) {
	case *pb.Topic:
		if r.GetSchemaSettings() == nil {
			return nil
		}
		return p.authorizeOn(call, r.GetSchemaSettings().GetSchema(), "schema", schemaAttach)
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
	project, err := validation.ResourceName(name, kind+"s")
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
	if _, still := p.index.Get(resource.Name); provider.Unverified(err) && !still {
		// A concurrent delete unindexed it between the lookup and Authorize.
		return status.Error(codes.NotFound, strings.ToUpper(kind[:1])+kind[1:]+" does not exist")
	}
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
	target, err := p.callTarget(call, project)
	if err != nil {
		return err
	}
	decision, err := p.iam.Authorize(call, target, nil)
	if provider.Unverified(err) && target.Name != project.Name {
		// A concurrent delete can unindex the resource between callTarget and
		// Authorize; authorize against what the index holds now.
		if target, err = p.callTarget(call, project); err != nil {
			return err
		}
		decision, err = p.iam.Authorize(call, target, nil)
	}
	_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-decision-id", decision.ID))
	if err != nil {
		return fmt.Errorf("authorize %s: %w", call.Method, err)
	}
	return nil
}

// callTarget is the indexed resource a call names, or its project for
// project-level methods and not-yet-indexed resources.
func (p *Provider) callTarget(call gateway.Call, project state.Resource) (iam.Target, error) {
	target := iam.Target{Name: project.Name}
	if !projectMethod(call.Method) {
		if resource, found := p.index.Get(canonical(call.Resource)); found {
			if resource.Service != serviceName || resource.Scope != scopeOf(call) || resource.Parent != project.Name {
				return target, gap("resource ownership disagrees with the index")
			}
			target.Name = resource.Name
		}
	}
	return target, nil
}

// authorizeAttachment checks that a subscription may attach to its topic and,
// for push subscriptions, act as the push identity.
func (p *Provider) authorizeAttachment(call gateway.Call, subscription *pb.Subscription) error {
	project, err := validation.ResourceName(subscription.GetTopic(), "topics")
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
	return p.run(call, request)
}

// run opens the backend session and runs the authorize, invoke, and settle
// flow that the gRPC and REST surfaces share.
func (p *Provider) run(call gateway.Call, request proto.Message) (proto.Message, error) {
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
	forwarded, err = p.providerFields(forwarded)
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

// providerFields applies the provider-owned parts of a request before it is
// forwarded: the dead-letter policy and Protocol Buffer schema settings are
// withheld, and a publish to a Protocol Buffer topic is validated.
func (p *Provider) providerFields(request proto.Message) (proto.Message, error) {
	forwarded, err := withoutDeadLetter(request)
	if err != nil {
		return nil, err
	}
	if forwarded, err = p.withoutProtoSchema(forwarded); err != nil {
		return nil, err
	}
	if publish, ok := forwarded.(*pb.PublishRequest); ok {
		return p.validateSchemaPublish(publish)
	}
	return forwarded, nil
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
		project, err := validation.ResourceName(call.Resource, "")
		if err != nil {
			return err
		}
		if project != call.Project {
			return provider.Invalid("Resource project mismatch")
		}
	}
	if err := validation.Request(request); err != nil {
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
	return p.resolveMutation(session, b, call, request, push, creating, invokeErr)
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
	case *pb.Topic:
		p.storeTopicSchema(r)
	}
	if len(labels) > 0 {
		p.labels.Store(canonical(call.Resource), labels)
	}
	p.restorePush(response)
	return nil
}

// resolveMutation re-reads an ambiguous mutation's resource to decide whether
// the backend landed it before the error, and only then binds a created topic
// to its provider-owned schema or starts a created subscription's push worker.
func (p *Provider) resolveMutation(session context.Context, b driver, call gateway.Call, request proto.Message, push *pb.PushConfig, creating bool, invokeErr error) error {
	if indexErr := p.reconcile(session, b, call); indexErr != nil {
		return errors.Join(invokeErr, indexErr)
	}
	if topic, ok := request.(*pb.Topic); ok && creating && p.mutationLanded(call) {
		p.storeTopicSchema(topic)
	}
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
	for _, descriptor := range pubsubapi.PubSubMethods {
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
	switch {
	case strings.Contains(call.Resource, "/subscriptions/"):
		kind = "subscription"
	case strings.Contains(call.Resource, "/snapshots/"):
		kind = "snapshot"
	case strings.Contains(call.Resource, "/schemas/"):
		kind = "schema"
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
	p.topicSchemas.Delete(canonical(call.Resource))
	if err := p.stopPush(canonical(call.Resource)); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return err
	}
	if err := p.index.Remove(serviceName, scopeOf(call), canonical(call.Resource)); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return status.Error(codes.FailedPrecondition, "Backend deletion could not be indexed; reset this scope")
	}
	// The index removal also dropped the resource's policy.
	return nil
}
func ambiguous(err error) bool {
	return status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Resolve uncertain control-plane outcomes under the same mutation gate. A
// read failure retains the last confirmed index and blocks access until reset.
// It returns the index error of a confirmed outcome that could not be recorded.
func (p *Provider) reconcile(parent context.Context, b driver, call gateway.Call) error {
	// A cancelled request must not cancel reconciliation, and the Python JVM
	// backend outlives it, so the deadline is derived rather than inherited.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	method, request := publisher+"GetTopic", proto.Message(&pb.GetTopicRequest{Topic: call.Resource})
	switch {
	case strings.Contains(call.Resource, "/subscriptions/"):
		method, request = subscriber+"GetSubscription", &pb.GetSubscriptionRequest{Subscription: call.Resource}
	case strings.Contains(call.Resource, "/snapshots/"):
		method, request = subscriber+"GetSnapshot", &pb.GetSnapshotRequest{Snapshot: call.Resource}
	case strings.Contains(call.Resource, "/schemas/"):
		method, request = schemaService+"GetSchema", &pb.GetSchemaRequest{Name: call.Resource}
	}
	_, err := invoke(ctx, b, method, request)
	if err == nil {
		return p.publish(call)
	}
	if status.Code(err) == codes.NotFound {
		return p.forget(call)
	}
	p.poisoned.Store(scopeOf(call), true)
	return nil
}

const publisher = "/google.pubsub.v1.Publisher/"
const subscriber = "/google.pubsub.v1.Subscriber/"
const schemaService = "/google.pubsub.v1.SchemaService/"

// schemaAttach is checked on the schema a created topic names; the Admin and
// Editor roles carry pubsub.schemas.attach (access-control table).
const schemaAttach = "pubsub.schemas.attachTarget"
const attach = "pubsub.attachSubscription"

// CreateSnapshot also needs consume on the source subscription, and Seek to a
// snapshot needs snapshots.seek on it (access-control table).
const (
	snapshotSource = "pubsub.snapshots.source"
	snapshotSeek   = "pubsub.snapshots.seekTarget"
)
const policyRPC = "/google.iam.v1.IAMPolicy/"

// IAM policy permissions shared by the gRPC policy surface and the REST twins.
const (
	permTopicsGetIam        = "pubsub.topics.getIamPolicy"
	permTopicsSetIam        = "pubsub.topics.setIamPolicy"
	permSubscriptionsGetIam = "pubsub.subscriptions.getIamPolicy"
	permSubscriptionsSetIam = "pubsub.subscriptions.setIamPolicy"
	permSchemasGetIam       = "pubsub.schemas.getIamPolicy"
	permSchemasSetIam       = "pubsub.schemas.setIamPolicy"
)

// Permission targets follow Google's documented access-control table. Live
// Pub/Sub permission probes remain unrecorded; these are not fidelity claims.
var permissions = map[string]string{
	publisher + "CreateTopic":         "pubsub.topics.create",
	publisher + "GetTopic":            "pubsub.topics.get",
	publisher + "DeleteTopic":         "pubsub.topics.delete",
	publisher + "ListTopics":          "pubsub.topics.list",
	publisher + "Publish":             "pubsub.topics.publish",
	subscriber + "CreateSubscription": "pubsub.subscriptions.create",
	subscriber + "GetSubscription":    "pubsub.subscriptions.get",
	subscriber + "DeleteSubscription": "pubsub.subscriptions.delete",
	subscriber + "ListSubscriptions":  "pubsub.subscriptions.list",
	subscriber + "Pull":               permissionSubscriptionsConsume,
	subscriber + "Acknowledge":        permissionSubscriptionsConsume,
	subscriber + "ModifyAckDeadline":  permissionSubscriptionsConsume,
	subscriber + "StreamingPull":      permissionSubscriptionsConsume,
	subscriber + "Seek":               permissionSubscriptionsConsume,
	subscriber + "CreateSnapshot":     "pubsub.snapshots.create",
	subscriber + "ListSnapshots":      "pubsub.snapshots.list",
	subscriber + "DeleteSnapshot":     "pubsub.snapshots.delete",
	// The access-control table has no GetSnapshot row; this follows the
	// snapshots.get permission that the Viewer, Editor, and Admin roles carry.
	subscriber + "GetSnapshot": "pubsub.snapshots.get",
	snapshotSource:             permissionSubscriptionsConsume,
	snapshotSeek:               "pubsub.snapshots.seek",
	deadLetterPublish:          "pubsub.topics.publish",
	deadLetterAcknowledge:      permissionSubscriptionsConsume,
	attach:                     "pubsub.topics.attachSubscription",
	permTopicsGetIam:           permTopicsGetIam,
	permTopicsSetIam:           permTopicsSetIam,
	permSubscriptionsGetIam:    permSubscriptionsGetIam,
	permSubscriptionsSetIam:    permSubscriptionsSetIam,
	"pubsub.push.actAs":        "iam.serviceAccounts.actAs",
	// Schema rows of the access-control table. DeleteSchemaRevision needs
	// pubsub.schemas.delete; ValidateMessage needs pubsub.schemas.validate on
	// the project even when it names a schema.
	schemaService + "CreateSchema":         "pubsub.schemas.create",
	schemaService + "GetSchema":            "pubsub.schemas.get",
	schemaService + "ListSchemas":          "pubsub.schemas.list",
	schemaService + "ListSchemaRevisions":  "pubsub.schemas.listRevisions",
	schemaService + "CommitSchema":         "pubsub.schemas.commit",
	schemaService + "RollbackSchema":       "pubsub.schemas.rollback",
	schemaService + "DeleteSchemaRevision": "pubsub.schemas.delete",
	schemaService + "DeleteSchema":         "pubsub.schemas.delete",
	schemaService + "ValidateSchema":       "pubsub.schemas.validate",
	schemaService + "ValidateMessage":      "pubsub.schemas.validate",
	schemaAttach:                           "pubsub.schemas.attach",
	permSchemasGetIam:                      permSchemasGetIam,
	permSchemasSetIam:                      permSchemasSetIam,
	policyRPC + "GetIamPolicy":             permTopicsGetIam,
	policyRPC + "SetIamPolicy":             permTopicsSetIam,
}

// permissionSubscriptionsConsume is the single permission that governs every
// subscription message operation.
const permissionSubscriptionsConsume = "pubsub.subscriptions.consume"

// errMessageType reports a protobuf clone whose concrete type changed, which
// would mean the descriptor and the Go type disagree.
var errMessageType = errors.New("cloned message has an unexpected type")

// errBackendType reports a backend that this provider did not register.
var errBackendType = errors.New("backend is not the pub/sub emulator")

// cloneMessage clones a protobuf message and preserves its concrete type. The
// assertion is checked so a descriptor/type mismatch is an error, not a panic.
func cloneMessage[T proto.Message](message T) (T, error) {
	var zero T
	clone, ok := proto.Clone(message).(T)
	if !ok {
		return zero, errMessageType
	}
	return clone, nil
}

// driverOf returns the emulator driver behind a manager-owned backend.
func driverOf(active backend.Backend) (driver, error) {
	value, ok := active.(driver)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// pushWorker reads a stored push worker, reporting a foreign value in the map.
func pushWorker(value any) (*pushSubscription, bool) {
	worker, ok := value.(*pushSubscription)
	return worker, ok
}

func (p *Provider) policy(call gateway.Call, raw any) (any, error) {
	project, err := validation.ResourceName(call.Resource, "")
	if err != nil {
		return nil, err
	}
	if project != call.Project {
		return nil, provider.Invalid("Policy project mismatch")
	}
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, provider.Invalid("Invalid IAM request")
	}
	update, err := p.policyUpdate(&call, request)
	if err != nil {
		return nil, err
	}
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	result, err := p.applyPolicy(call, request, update)
	if err != nil {
		return nil, err
	}
	return wirePolicy(result)
}

// policyUpdate validates the IAM request and turns a SetIamPolicy into the
// engine policy. The call method is renamed to the resolved IAM operation.
func (p *Provider) policyUpdate(call *gateway.Call, request proto.Message) (*iam.Policy, error) {
	kind := strings.Split(call.Resource, "/")[2]
	if kind == "snapshots" {
		return nil, gap("snapshot IAM policies")
	}
	switch r := request.(type) {
	case *iampb.GetIamPolicyRequest:
		if err := validation.Fields(r, "resource", "options"); err != nil {
			return nil, err
		}
		if err := validatePolicyOptions(r.GetOptions()); err != nil {
			return nil, err
		}
		call.Method = "pubsub." + kind + ".getIamPolicy"
		return nil, nil //nolint:nilnil // a get request carries no policy update
	case *iampb.SetIamPolicyRequest:
		policy, err := decodeSetPolicy(r)
		if err != nil {
			return nil, err
		}
		call.Method = "pubsub." + kind + ".setIamPolicy"
		return policy, nil
	default:
		return nil, gap("IAM method")
	}
}

// validatePolicyOptions accepts only policy version 1, which is what the
// emulator's conditional-policy gap already excludes.
func validatePolicyOptions(options *iampb.GetPolicyOptions) error {
	if options == nil {
		return nil
	}
	if err := validation.Fields(options, "requested_policy_version"); err != nil {
		return err
	}
	if version := options.GetRequestedPolicyVersion(); version != 0 && version != 1 && version != 3 {
		return gap("requested IAM policy version")
	}
	return nil
}

// decodeSetPolicy converts the wire policy into the engine representation.
func decodeSetPolicy(r *iampb.SetIamPolicyRequest) (*iam.Policy, error) {
	if r.GetUpdateMask() != nil {
		return nil, gap("partial IAM policy updates")
	}
	if r.GetPolicy() == nil {
		return nil, provider.Invalid("Missing IAM policy")
	}
	if err := validation.Fields(r.GetPolicy(), "version", "bindings", "etag"); err != nil {
		return nil, err
	}
	policy := iam.Policy{Version: int(r.GetPolicy().GetVersion()), ETag: base64.StdEncoding.EncodeToString(r.GetPolicy().GetEtag())}
	for _, binding := range r.GetPolicy().GetBindings() {
		if binding == nil {
			return nil, provider.Invalid("Missing IAM binding")
		}
		if err := validation.Fields(binding, "role", "members", "condition"); err != nil {
			return nil, err
		}
		bindingShape := iam.Binding{Role: binding.GetRole(), Members: append([]string(nil), binding.GetMembers()...)}
		if condition := binding.GetCondition(); condition != nil {
			// The expr proto carries the CEL source text verbatim.
			bindingShape.Condition = json.RawMessage(`{"expression":` + strconv.Quote(condition.GetExpression()) + `}`)
		}
		policy.Bindings = append(policy.Bindings, bindingShape)
	}
	return &policy, nil
}

// applyPolicy runs the get or set inside one backend session.
func (p *Provider) applyPolicy(call gateway.Call, request proto.Message, update *iam.Policy) (iam.Policy, error) {
	var result iam.Policy
	err := p.manager.Use(call.Context, keyOf(call), func(session context.Context, _ backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		if err := p.authorize(call, request); err != nil {
			return err
		}
		var policyErr error
		if update == nil {
			result, policyErr = p.iam.GetPolicy(canonical(call.Resource))
		} else {
			result, policyErr = p.iam.SetPolicy(canonical(call.Resource), *update)
		}
		if policyErr != nil {
			return fmt.Errorf("set iam policy: %w", policyErr)
		}
		return nil
	})
	if err != nil {
		return iam.Policy{}, fmt.Errorf("apply policy in the backend session: %w", err)
	}
	return result, nil
}

// wirePolicy converts the engine policy back to the wire representation.
func wirePolicy(result iam.Policy) (*iampb.Policy, error) {
	etag, err := base64.StdEncoding.DecodeString(result.ETag)
	if err != nil {
		return nil, fmt.Errorf("decode policy etag: %w", err)
	}
	if result.Version < 0 || result.Version > math.MaxInt32 {
		return nil, status.Error(codes.Internal, "Policy version is outside the wire range")
	}
	response := &iampb.Policy{Version: int32(result.Version), Etag: etag}
	for _, binding := range result.Bindings {
		response.Bindings = append(response.Bindings, &iampb.Binding{Role: binding.Role, Members: binding.Members})
	}
	return response, nil
}
