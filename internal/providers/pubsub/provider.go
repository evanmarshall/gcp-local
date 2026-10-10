// Package pubsub wraps the official emulator with scoped lifecycle and IAM.
package pubsub

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/api/generated"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
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
	if err := registerPermissions(engine); err != nil {
		return nil, err
	}
	if err := manager.Register(serviceName, backend.Definition{Validate: state.GlobalScope, New: factory, AfterReset: func(scope state.Scope) error {
		if err := p.resetPush(scope); err != nil {
			return err
		}
		engine.ClearScope(serviceName, scope)
		index.RemoveScope(serviceName, scope)
		p.poisoned.Delete(scope)
		return nil
	}}); err != nil {
		return nil, err
	}
	for method := range permissions {
		if !strings.HasPrefix(method, "/") || method == subscriber+"StreamingPull" {
			continue
		}
		if err := registry.Register(method, p.handle); err != nil {
			return nil, err
		}
	}
	for _, method := range []string{"GetIamPolicy", "SetIamPolicy"} {
		if err := registry.Register(policyRPC+method, p.policy); err != nil {
			return nil, err
		}
	}
	if err := registry.RegisterStream(subscriber+"StreamingPull", p.stream); err != nil {
		return nil, err
	}
	return p, nil
}

func scopeOf(call gateway.Call) state.Scope {
	return state.Scope{Project: call.Project, Location: state.Global}
}
func keyOf(call gateway.Call) backend.Key {
	return backend.Key{Service: serviceName, Scope: scopeOf(call)}
}
func projectMethod(method string) bool {
	return method == publisher+"CreateTopic" || method == subscriber+"CreateSubscription" || method == publisher+"ListTopics" || method == subscriber+"ListSubscriptions"
}

func (p *Provider) authorize(call gateway.Call, request proto.Message) error {
	if call.Principal.State == gateway.Anonymous {
		return status.Error(codes.Unauthenticated, "Pub/Sub requires a named local principal")
	}
	if _, failed := p.poisoned.Load(scopeOf(call)); failed {
		return status.Error(codes.FailedPrecondition, "Pub/Sub mutation outcome is unresolved; reset this scope")
	}
	project, found := p.index.Get(projectResource(call.Project))
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
		return err
	}
	if subscription, ok := request.(*pb.Subscription); ok {
		project, err := resourceName(subscription.GetTopic(), "topics")
		if err != nil {
			return err
		}
		if project != call.Project {
			return gap("cross-project subscriptions")
		}
		topic, found := p.index.Get(canonical(subscription.GetTopic()))
		if !found {
			return status.Error(codes.NotFound, "Topic does not exist")
		}
		if topic.Service != serviceName || topic.Kind != "topic" || topic.Scope != scopeOf(call) {
			return gap("topic ownership disagrees with the index")
		}
		attachment := call
		attachment.Method, attachment.Resource = attach, subscription.GetTopic()
		decision, err := p.iam.Authorize(attachment, iam.Target{Name: topic.Name}, nil)
		_ = grpc.SetHeader(call.Context, metadata.Pairs("x-gcp-local-attachment-decision-id", decision.ID))
		if err != nil {
			return err
		}
		if subscription.GetPushConfig() != nil {
			if err := p.authorizePush(call, subscription.GetPushConfig()); err != nil {
				return err
			}
		}
	}
	if call.Method == subscriber+"Pull" || call.Method == subscriber+"StreamingPull" {
		if _, push := p.pushes.Load(canonical(call.Resource)); push {
			return status.Error(codes.FailedPrecondition, "Cannot pull from a push subscription")
		}
	}
	if !projectMethod(call.Method) {
		if _, found := p.index.Get(canonical(call.Resource)); !found {
			return status.Error(codes.NotFound, "Pub/Sub resource does not exist")
		}
	}
	return nil
}

func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub request")
	}
	if call.Method != publisher+"ListTopics" && call.Method != subscriber+"ListSubscriptions" {
		project, err := resourceName(call.Resource, "")
		if err != nil {
			return nil, err
		}
		if project != call.Project {
			return nil, invalid("Resource project mismatch")
		}
	}
	if err := validate(request); err != nil {
		return nil, err
	}
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	var response proto.Message
	use := p.manager.Use
	if call.Method == subscriber+"Pull" {
		use = p.manager.Session
	}
	err := use(call.Context, keyOf(call), func(ctx context.Context, active backend.Backend) error {
		call.Context = ctx
		if err := p.authorize(call, request); err != nil {
			return err
		}
		b := active.(driver)
		creating := call.Method == publisher+"CreateTopic" || call.Method == subscriber+"CreateSubscription"
		deleting := call.Method == publisher+"DeleteTopic" || call.Method == subscriber+"DeleteSubscription"
		if creating {
			if _, found := p.index.Get(canonical(call.Resource)); found {
				return status.Error(codes.AlreadyExists, "Pub/Sub resource already exists")
			}
		}
		var err error
		forwarded := request
		var push *pb.PushConfig
		if subscription, ok := request.(*pb.Subscription); ok && subscription.GetPushConfig() != nil {
			if p.pushCount(scopeOf(call)) >= 32 {
				return status.Error(codes.ResourceExhausted, "Local limit is 32 push subscriptions per project")
			}
			push = proto.Clone(subscription.GetPushConfig()).(*pb.PushConfig)
			copy := proto.Clone(subscription).(*pb.Subscription)
			copy.PushConfig = nil // The unauthenticated emulator must never make HTTP deliveries.
			forwarded = copy
		}
		response, err = invoke(ctx, b, call.Method, forwarded)
		if err == nil && call.Method == subscriber+"Pull" {
			err = p.authorize(call, request)
		}
		if creating || deleting {
			if err == nil {
				if creating {
					if err := p.publish(call); err != nil {
						return err
					}
					if push != nil {
						p.startPush(call, push)
					}
					p.restorePush(response)
					return nil
				}
				return p.forget(call)
			}
			if ambiguous(err) {
				p.reconcile(b, call)
				if creating && push != nil {
					_, failed := p.poisoned.Load(scopeOf(call))
					if _, found := p.index.Get(canonical(call.Resource)); found && !failed {
						p.startPush(call, push)
					}
				}
			}
		}
		if err == nil {
			p.restorePush(response)
		}
		return err
	})
	return response, err
}

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
	}
	if err := p.index.Put(serviceName, state.Resource{Name: canonical(call.Resource), Service: serviceName, Kind: kind, Parent: projectResource(call.Project), Scope: scopeOf(call)}); err != nil {
		p.poisoned.Store(scopeOf(call), true)
		return status.Error(codes.FailedPrecondition, "Backend mutation could not be indexed; reset this scope")
	}
	return nil
}
func (p *Provider) forget(call gateway.Call) error {
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
func (p *Provider) reconcile(b driver, call gateway.Call) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	method, request := publisher+"GetTopic", proto.Message(&pb.GetTopicRequest{Topic: call.Resource})
	if strings.Contains(call.Resource, "/subscriptions/") {
		method, request = subscriber+"GetSubscription", &pb.GetSubscriptionRequest{Subscription: call.Resource}
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
