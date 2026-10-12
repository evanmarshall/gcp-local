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
	"google.golang.org/protobuf/types/known/durationpb"
)

// Wire document keys shared by the JSON response shapes.
const (
	keyWireName  = "name"
	keyWireTopic = "topic"
)

// buildRestProto converts one REST request into the gRPC twin's proto.
func (p *Provider) buildRestProto(twin, project string, request *gateway.RESTRequest) (proto.Message, error) {
	if strings.Contains(twin, "Subscription") {
		return p.buildSubscriptionProto(twin, project, request)
	}
	switch twin {
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
	switch twin {
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
		return &pb.ListSubscriptionsRequest{Project: "projects/" + project}, nil
	}
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
		wire := map[string]any{
			keyWireName: wireRestName(protoMessage.GetName()),
			"labels":    protoMessage.GetLabels(),
		}
		return wire
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
