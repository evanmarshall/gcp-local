package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Shared fixture values for the memory-backed provider tests.
const (
	secondProject  = "project-b"
	kindProject    = "project"
	hierarchy      = "resourcemanager"
	roleAdmin      = "roles/pubsub.admin"
	roleSubscriber = "roles/pubsub.subscriber"
	roleViewer     = "roles/pubsub.viewer"
	pushSubName    = "projects/project-a/subscriptions/push"
	shortTopicName = "projects/project-a/topics/x"
	foreignTopic   = "projects/project-b/topics/topic"
	absentTopic    = "projects/project-a/topics/absent"
	getIAM         = policyRPC + "GetIamPolicy"
	setIAM         = policyRPC + "SetIamPolicy"
)

// memoryPubSub is a small in-process stand-in for the official emulator. It
// keeps topics, subscriptions, and per-subscription queues so provider flows
// observe realistic create/get/delete/publish/pull outcomes without Java.
type memoryPubSub struct {
	mu            sync.Mutex
	calls         []string
	topics        map[string]*pb.Topic
	subscriptions map[string]*pb.Subscription
	queues        map[string][]*pb.PubsubMessage
	outstanding   map[string]*pb.PubsubMessage
	acked         []string
	snapshots     map[string]*pb.Snapshot
	schemas       map[string]*pb.Schema // Avro schemas, unvalidated
	modacks       []*pb.ModifyAckDeadlineRequest
	updates       []*pb.UpdateSubscriptionRequest
	nextID        int
	starts        int
	fault         func(method string) error
	stream        func(ctx context.Context) (grpc.ClientStream, error)
}

func newMemoryPubSub() *memoryPubSub {
	return &memoryPubSub{topics: map[string]*pb.Topic{}, subscriptions: map[string]*pb.Subscription{}, queues: map[string][]*pb.PubsubMessage{}, outstanding: map[string]*pb.PubsubMessage{}, snapshots: map[string]*pb.Snapshot{}}
}

func (m *memoryPubSub) Start(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts++
	return nil
}
func (*memoryPubSub) Ready(context.Context) error { return nil }
func (*memoryPubSub) Stop(context.Context) error  { return nil }
func (m *memoryPubSub) Reset(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics, m.subscriptions = map[string]*pb.Topic{}, map[string]*pb.Subscription{}
	m.queues, m.outstanding = map[string][]*pb.PubsubMessage{}, map[string]*pb.PubsubMessage{}
	m.snapshots = map[string]*pb.Snapshot{}
	return nil
}
func (m *memoryPubSub) client() grpc.ClientConnInterface { return m }

func (m *memoryPubSub) NewStream(ctx context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	m.mu.Lock()
	m.calls = append(m.calls, method)
	open := m.stream
	m.mu.Unlock()
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		return nil, status.Error(codes.Internal, "caller metadata reached backend stream")
	}
	if open == nil {
		return nil, status.Error(codes.Unimplemented, "no scripted stream")
	}
	return open(ctx)
}

// called reports how many times the backend saw one method.
func (m *memoryPubSub) called(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, seen := range m.calls {
		if seen == method {
			count++
		}
	}
	return count
}

// total reports every backend call so far.
func (m *memoryPubSub) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *memoryPubSub) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if md, _ := metadata.FromOutgoingContext(ctx); len(md) != 0 {
		return status.Error(codes.Internal, "caller metadata reached backend")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, method)
	if m.fault != nil {
		if err := m.fault(method); err != nil {
			return err
		}
	}
	in, inOK := request.(proto.Message)
	out, outOK := response.(proto.Message)
	if !inOK || !outOK {
		return status.Error(codes.Internal, "non-proto backend call")
	}
	result, err := m.apply(method, in)
	if err != nil {
		return err
	}
	proto.Merge(out, result)
	return nil
}

// apply executes one unary method against the in-memory state.
func (m *memoryPubSub) apply(method string, in proto.Message) (proto.Message, error) {
	if result, handled, err := m.applyTopic(in); handled {
		return result, err
	}
	if result, handled, err := m.applySubscription(in); handled {
		return result, err
	}
	if result, handled, err := m.applyMessages(in); handled {
		return result, err
	}
	if result, handled, err := m.applySnapshot(in); handled {
		return result, err
	}
	if result, handled, err := m.applySchema(in); handled {
		return result, err
	}
	return nil, status.Error(codes.Unimplemented, "memory backend does not serve "+method)
}

// applyTopic serves the topic control plane.
func (m *memoryPubSub) applyTopic(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.Topic:
		if _, found := m.topics[r.GetName()]; found {
			return nil, true, status.Error(codes.AlreadyExists, "Topic already exists")
		}
		stored, err := cloneMessage(r)
		m.topics[r.GetName()] = stored
		return stored, true, err
	case *pb.GetTopicRequest:
		if topic, found := m.topics[r.GetTopic()]; found {
			return topic, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Topic not found")
	case *pb.DeleteTopicRequest:
		if _, found := m.topics[r.GetTopic()]; !found {
			return nil, true, status.Error(codes.NotFound, "Topic not found")
		}
		delete(m.topics, r.GetTopic())
		return &emptypb.Empty{}, true, nil
	case *pb.ListTopicsRequest:
		list := &pb.ListTopicsResponse{}
		for _, topic := range m.topics {
			list.Topics = append(list.Topics, topic)
		}
		return list, true, nil
	}
	return nil, false, nil
}

// applySubscription serves the subscription control plane. Like the official
// emulator behind the provider, it does not retain labels; the provider does.
func (m *memoryPubSub) applySubscription(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.Subscription:
		result, err := m.createSubscription(r)
		return result, true, err
	case *pb.GetSubscriptionRequest:
		if subscription, found := m.subscriptions[r.GetSubscription()]; found {
			return subscription, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Subscription not found")
	case *pb.DeleteSubscriptionRequest:
		if _, found := m.subscriptions[r.GetSubscription()]; !found {
			return nil, true, status.Error(codes.NotFound, "Subscription not found")
		}
		delete(m.subscriptions, r.GetSubscription())
		return &emptypb.Empty{}, true, nil
	case *pb.ListSubscriptionsRequest:
		list := &pb.ListSubscriptionsResponse{}
		for _, subscription := range m.subscriptions {
			list.Subscriptions = append(list.Subscriptions, subscription)
		}
		return list, true, nil
	case *pb.UpdateSubscriptionRequest:
		result, err := m.updateSubscription(r)
		return result, true, err
	}
	return nil, false, nil
}

// updateSubscription applies the one mask the provider sends: the
// dead-letter policy it forwards or clears as the service agent's grants change.
func (m *memoryPubSub) updateSubscription(r *pb.UpdateSubscriptionRequest) (proto.Message, error) {
	stored, found := m.subscriptions[r.GetSubscription().GetName()]
	if !found {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	if paths := r.GetUpdateMask().GetPaths(); len(paths) != 1 || paths[0] != "dead_letter_policy" {
		return nil, status.Error(codes.Internal, "unexpected update mask")
	}
	m.updates = append(m.updates, proto.CloneOf(r))
	stored.DeadLetterPolicy = proto.CloneOf(r.GetSubscription().GetDeadLetterPolicy())
	return stored, nil
}

// applySnapshot serves snapshots and seek. Like the emulator, it drops labels.
func (m *memoryPubSub) applySnapshot(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.CreateSnapshotRequest:
		source, found := m.subscriptions[r.GetSubscription()]
		if !found {
			return nil, true, status.Error(codes.NotFound, "Subscription does not exist")
		}
		snapshot := &pb.Snapshot{Name: r.GetName(), Topic: source.GetTopic()}
		m.snapshots[r.GetName()] = snapshot
		return snapshot, true, nil
	case *pb.GetSnapshotRequest:
		if snapshot, found := m.snapshots[r.GetSnapshot()]; found {
			return snapshot, true, nil
		}
		return nil, true, status.Error(codes.NotFound, "Snapshot does not exist")
	case *pb.ListSnapshotsRequest:
		list := &pb.ListSnapshotsResponse{}
		for _, snapshot := range m.snapshots {
			list.Snapshots = append(list.Snapshots, snapshot)
		}
		return list, true, nil
	case *pb.DeleteSnapshotRequest:
		delete(m.snapshots, r.GetSnapshot())
		return &emptypb.Empty{}, true, nil
	case *pb.SeekRequest:
		return &pb.SeekResponse{}, true, nil
	}
	return nil, false, nil
}

// createSubscription stores a pull subscription; the provider must have
// stripped any push configuration before forwarding.
func (m *memoryPubSub) createSubscription(r *pb.Subscription) (proto.Message, error) {
	if r.GetPushConfig() != nil {
		return nil, status.Error(codes.Internal, "push config reached the backend")
	}
	if r.GetFilter() == "unparsable" {
		return nil, status.Error(codes.Unknown, "Application error processing RPC")
	}
	if _, found := m.subscriptions[r.GetName()]; found {
		return nil, status.Error(codes.AlreadyExists, "Subscription already exists")
	}
	stored, err := cloneMessage(r)
	if err != nil {
		return nil, err
	}
	stored.Labels = nil
	m.subscriptions[r.GetName()] = stored
	return stored, nil
}

// applyMessages serves publish, pull, ack, and ack-deadline changes.
func (m *memoryPubSub) applyMessages(in proto.Message) (proto.Message, bool, error) {
	switch r := in.(type) {
	case *pb.PublishRequest:
		result, err := m.publish(r)
		return result, true, err
	case *pb.PullRequest:
		result, err := m.pull(r)
		return result, true, err
	case *pb.AcknowledgeRequest:
		for _, id := range r.GetAckIds() {
			delete(m.outstanding, id)
			m.acked = append(m.acked, id)
		}
		return &emptypb.Empty{}, true, nil
	case *pb.ModifyAckDeadlineRequest:
		m.modacks = append(m.modacks, proto.CloneOf(r))
		if r.GetAckDeadlineSeconds() == 0 {
			m.requeue(r.GetSubscription(), r.GetAckIds())
		}
		return &emptypb.Empty{}, true, nil
	}
	return nil, false, nil
}

// requeue returns nacked outstanding messages to the head of their queue.
func (m *memoryPubSub) requeue(subscription string, ackIDs []string) {
	for _, id := range ackIDs {
		if message, found := m.outstanding[id]; found {
			delete(m.outstanding, id)
			m.queues[subscription] = append([]*pb.PubsubMessage{message}, m.queues[subscription]...)
		}
	}
}

func (m *memoryPubSub) publish(r *pb.PublishRequest) (proto.Message, error) {
	if _, found := m.topics[r.GetTopic()]; !found {
		return nil, status.Error(codes.NotFound, "Topic not found")
	}
	response := &pb.PublishResponse{}
	for _, message := range r.GetMessages() {
		m.nextID++
		stored, err := cloneMessage(message)
		if err != nil {
			return nil, err
		}
		stored.MessageId = strconv.Itoa(m.nextID)
		response.MessageIds = append(response.MessageIds, stored.GetMessageId())
		for name, subscription := range m.subscriptions {
			if subscription.GetTopic() == r.GetTopic() {
				m.queues[name] = append(m.queues[name], stored)
			}
		}
	}
	return response, nil
}

func (m *memoryPubSub) pull(r *pb.PullRequest) (proto.Message, error) {
	if _, found := m.subscriptions[r.GetSubscription()]; !found {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	response := &pb.PullResponse{}
	queue := m.queues[r.GetSubscription()]
	for len(queue) > 0 && len(response.GetReceivedMessages()) < int(r.GetMaxMessages()) {
		message := queue[0]
		queue = queue[1:]
		ackID := r.GetSubscription() + "#" + message.GetMessageId()
		m.outstanding[ackID] = message
		response.ReceivedMessages = append(response.ReceivedMessages, &pb.ReceivedMessage{AckId: ackID, Message: message})
	}
	m.queues[r.GetSubscription()] = queue
	return response, nil
}

// memoryFixture builds a provider over the memory backend with two projects:
// owner administers project-a and other administers project-b.
func memoryFixture(t *testing.T) (*Provider, *gateway.Registry, *memoryPubSub) {
	t.Helper()
	index := state.NewIndex()
	admins := map[string]string{testProject: owner, secondProject: other}
	engine, err := iam.New(index)
	must(t, err)
	for project, admin := range admins {
		must(t, index.Put(hierarchy, state.Resource{Name: provider.ProjectResource(project), Service: hierarchy, Kind: kindProject, Number: "123456", Scope: state.Scope{Project: project, Location: state.Global}}))
		_, err = engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{{Role: roleAdmin, Members: []string{admin}}}})
		must(t, err)
	}
	manager := backend.NewManager(time.Second, time.Second)
	registry, b := gateway.NewRegistry(), newMemoryPubSub()
	p, err := newProvider(registry, manager, index, engine, func(state.Scope) (backend.Backend, error) { return b, nil })
	must(t, err)
	t.Cleanup(func() { must(t, manager.Shutdown(context.Background())) })
	return p, registry, b
}

// invokeAs dispatches one gRPC-shaped request through the registry.
func invokeAs(registry *gateway.Registry, actor, method, resource string, request proto.Message) (any, error) {
	response, err := registry.Invoke(call(method, resource, actor), request)
	if err != nil {
		return nil, fmt.Errorf("invoke %s: %w", method, err)
	}
	return response, nil
}

// expectCode fails the test unless err carries the wanted gRPC code.
func expectCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v (%v), want %v", status.Code(err), err, want)
	}
}
