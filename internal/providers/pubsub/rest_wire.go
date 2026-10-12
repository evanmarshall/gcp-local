package pubsub

import (
	"strconv"

	"encoding/json"
	"fmt"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Wire document keys shared by the JSON response shapes.
const (
	keyWireName  = "name"
	keyWireTopic = "topic"
)

// buildRestProto converts one REST request into the gRPC twin's proto.
func (p *Provider) buildRestProto(twin, project string, request *gateway.RESTRequest) (proto.Message, error) {
	if strings.HasPrefix(twin, subscriber) {
		return p.buildSubscriptionProto(twin, project, request)
	}
	switch twin {
	case publisher + "Publish":
		return restPathBound(request, &pb.PublishRequest{}, "topic", restTopicName(project, request.Path["topic"]))
	case publisher + "CreateTopic":
		// The v2 CreateTopic call takes the Topic message directly.
		topic := &pb.Topic{}
		if err := restBody(request, topic); err != nil {
			return nil, err
		}
		topic.Name = restTopicName(project, request.Path["topic"])
		return topic, nil
	case publisher + "GetTopic":
		return &pb.GetTopicRequest{Topic: restTopicName(project, request.Path["topic"])}, nil
	case publisher + "DeleteTopic":
		return &pb.DeleteTopicRequest{Topic: restTopicName(project, request.Path["topic"])}, nil
	case publisher + "ListTopics":
		return &pb.ListTopicsRequest{Project: "projects/" + project}, nil
	default:
		return nil, fmt.Errorf("%w: %s", errRestNoProtoMapping, twin)
	}
}

// buildSubscriptionProto converts one REST subscription request.
func (p *Provider) buildSubscriptionProto(twin, project string, request *gateway.RESTRequest) (proto.Message, error) {
	name := restSubscriptionName(project, request.Path["subscription"])
	switch twin {
	case subscriber + "Pull":
		return restPathBound(request, &pb.PullRequest{}, "subscription", name)
	case subscriber + "Acknowledge":
		return restPathBound(request, &pb.AcknowledgeRequest{}, "subscription", name)
	case subscriber + "ModifyAckDeadline":
		return restPathBound(request, &pb.ModifyAckDeadlineRequest{}, "subscription", name)
	case subscriber + "ListSubscriptions":
		return &pb.ListSubscriptionsRequest{Project: "projects/" + project}, nil
	case subscriber + "CreateSubscription":
		subscription, err := subscriptionFromREST(project, request)
		if err != nil {
			return nil, err
		}
		return subscription, nil
	case subscriber + "GetSubscription":
		return &pb.GetSubscriptionRequest{Subscription: restSubscriptionName(project, request.Path["subscription"])}, nil
	case subscriber + "DeleteSubscription":
		return &pb.DeleteSubscriptionRequest{Subscription: restSubscriptionName(project, request.Path["subscription"])}, nil
	default:
		return nil, fmt.Errorf("%w: %s", errRestNoProtoMapping, twin)
	}
}

// restPathBound decodes a data-plane body (publish, pull, acknowledge,
// modifyAckDeadline) and binds the resource from the path. The REST
// reference's request bodies omit the topic or subscription, which the path
// names. GCP accepts a publish body that repeats the path's topic (recording
// pubsub-rest-data-plane); a body naming a different resource is unrecorded,
// so it is refused rather than silently resolved either way.
func restPathBound(request *gateway.RESTRequest, message proto.Message, field protoreflect.Name, name string) (proto.Message, error) {
	if err := restBody(request, message); err != nil {
		return nil, err
	}
	reflected := message.ProtoReflect()
	descriptor := reflected.Descriptor().Fields().ByName(field)
	if reflected.Has(descriptor) && reflected.Get(descriptor).String() != name {
		return nil, status.Error(codes.InvalidArgument, "Invalid request body: the "+string(field)+" differs from the request path")
	}
	reflected.Set(descriptor, protoreflect.ValueOfString(name))
	return message, nil
}

// dataPlane reports whether a twin is a message operation, whose REST
// response is the proto's own JSON mapping.
func dataPlane(twin string) bool {
	switch twin {
	case publisher + "Publish", subscriber + "Pull", subscriber + "Acknowledge", subscriber + "ModifyAckDeadline":
		return true
	}
	return false
}

// restDataWire renders a message operation's response with Google's JSON
// mapping of the proto, as the REST reference documents: PublishResponse
// carries messageIds; PullResponse carries receivedMessages with ackId,
// message (base64 data, messageId, publishTime, attributes, orderingKey), and
// deliveryAttempt when a dead-letter policy counts it; acknowledge and
// modifyAckDeadline answer an empty object.
func restDataWire(response proto.Message) (*gateway.RESTResponse, error) {
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the Pub/Sub response")
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded)}, nil
}

// restBody decodes a REST body with Google's JSON mapping of the proto.
// Unknown JSON fields are rejected, as GCP does; known fields the provider does
// not implement are refused afterwards by validate as coverage gaps.
func restBody(request *gateway.RESTRequest, message proto.Message) error {
	if len(request.JSON) == 0 {
		return nil
	}
	if err := protojson.Unmarshal(request.JSON, message); err != nil {
		return status.Error(codes.InvalidArgument, "Invalid request body")
	}
	return nil
}

// subscriptionFromREST converts a REST CreateSubscription body. The v2
// CreateSubscription call takes the Subscription directly. A bare topic ID is
// resolved in the request project; the ack deadline defaults to 10 seconds.
func subscriptionFromREST(project string, request *gateway.RESTRequest) (*pb.Subscription, error) {
	subscription := &pb.Subscription{}
	if err := restBody(request, subscription); err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid subscription body")
	}
	if !strings.HasPrefix(subscription.GetTopic(), "projects/") {
		subscription.Topic = restTopicName(project, subscription.GetTopic())
	}
	if subscription.GetAckDeadlineSeconds() == 0 {
		subscription.AckDeadlineSeconds = 10
	}
	subscription.Name = restSubscriptionName(project, request.Path["subscription"])
	return subscription, nil
}

// secondsString renders one protobuf duration in the wire "3600s" shape.
func secondsString(value *durationpb.Duration) string {
	return strconv.FormatInt(value.GetSeconds(), 10) + "s"
}

// restWire maps the emulator's proto response to the Pub/Sub JSON shape.
func restWire(_ string, response proto.Message) map[string]any {
	switch protoMessage := response.(type) {
	case *pb.Topic:
		return topicWire(protoMessage)
	case *pb.Subscription:
		wire := map[string]any{
			keyWireName:          wireRestName(protoMessage.GetName()),
			keyWireTopic:         wireRestName(protoMessage.GetTopic()),
			"ackDeadlineSeconds": protoMessage.GetAckDeadlineSeconds(),
			"labels":             protoMessage.GetLabels(),
		}
		if retention := protoMessage.GetMessageRetentionDuration(); retention != nil {
			wire["messageRetentionDuration"] = secondsString(retention)
		}
		if push := pushWire(protoMessage.GetPushConfig()); push != nil {
			wire["pushConfig"] = push
		}
		deliveryWire(wire, protoMessage)
		return wire
	case *pb.ListTopicsResponse:
		topics := []any{}
		for _, topic := range protoMessage.GetTopics() {
			topics = append(topics, map[string]any{keyWireName: wireRestName(topic.GetName())})
		}
		return map[string]any{"topics": topics}
	case *pb.ListSubscriptionsResponse:
		subs := []any{}
		for _, sub := range protoMessage.GetSubscriptions() {
			subs = append(subs, map[string]any{keyWireName: wireRestName(sub.GetName())})
		}
		return map[string]any{"subscriptions": subs}
	default:
		return map[string]any{}
	}
}

// topicWire renders a topic, with its schema settings when it has them.
func topicWire(topic *pb.Topic) map[string]any {
	wire := map[string]any{keyWireName: wireRestName(topic.GetName()), "labels": topic.GetLabels()}
	if settings := topic.GetSchemaSettings(); settings != nil {
		if encoded, err := protojson.Marshal(settings); err == nil {
			wire["schemaSettings"] = json.RawMessage(encoded)
		}
	}
	return wire
}

// deliveryWire adds the delivery settings a subscription carries, in the JSON
// field names declarative clients read back.
func deliveryWire(wire map[string]any, subscription *pb.Subscription) {
	for key, message := range map[string]proto.Message{"deadLetterPolicy": subscription.GetDeadLetterPolicy(), "retryPolicy": subscription.GetRetryPolicy(), "expirationPolicy": subscription.GetExpirationPolicy()} {
		if message.ProtoReflect().IsValid() {
			if encoded, err := protojson.Marshal(message); err == nil {
				wire[key] = json.RawMessage(encoded)
			}
		}
	}
	for key, value := range map[string]bool{"enableMessageOrdering": subscription.GetEnableMessageOrdering(), "enableExactlyOnceDelivery": subscription.GetEnableExactlyOnceDelivery(), "retainAckedMessages": subscription.GetRetainAckedMessages()} {
		if value {
			wire[key] = true
		}
	}
	if filter := subscription.GetFilter(); filter != "" {
		wire["filter"] = filter
	}
}

// pushWire renders a subscription's push configuration in the JSON field
// names declarative clients read back, or nil for a pull subscription.
func pushWire(push *pb.PushConfig) map[string]any {
	if push.GetPushEndpoint() == "" && push.GetOidcToken().GetServiceAccountEmail() == "" {
		return nil
	}
	wire := map[string]any{"pushEndpoint": push.GetPushEndpoint()}
	if attributes := push.GetAttributes(); len(attributes) > 0 {
		wire["attributes"] = attributes
	}
	if token := push.GetOidcToken(); token != nil {
		wire["oidcToken"] = map[string]any{"serviceAccountEmail": token.GetServiceAccountEmail(), "audience": token.GetAudience()}
	}
	return wire
}

// wireRestName converts a canonical //host name into the wire-relative name.
func wireRestName(name string) string {
	_, rest, found := strings.Cut(name, "googleapis.com/")
	if found {
		return rest
	}
	return name
}
