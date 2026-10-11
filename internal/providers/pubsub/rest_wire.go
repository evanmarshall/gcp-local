package pubsub

import (
	"strconv"
	"time"

	"encoding/json"
	"fmt"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		return &pb.Topic{
			Name:   restTopicName(project, request.Path["topic"]),
			Labels: restStringMap(request.JSON, "labels"),
		}, nil
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
		var body struct {
			Topic                    string         `json:"topic"`
			AckDeadlineSeconds       int32          `json:"ackDeadlineSeconds"`
			MessageRetentionDuration string         `json:"messageRetentionDuration"`
			PushConfig               *pb.PushConfig `json:"pushConfig"`
		}
		if json.Unmarshal(request.JSON, &body) != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid subscription body")
		}
		topic := body.Topic
		if !strings.HasPrefix(topic, "projects/") {
			topic = restTopicName(project, topic)
		}
		deadline := body.AckDeadlineSeconds
		if deadline == 0 {
			deadline = 10
		}
		sub := &pb.Subscription{
			Name:                     restSubscriptionName(project, request.Path["subscription"]),
			Topic:                    topic,
			AckDeadlineSeconds:       deadline,
			MessageRetentionDuration: durationFromSeconds(body.MessageRetentionDuration),
			PushConfig:               body.PushConfig,
		}
		// The v2 CreateSubscription call takes the Subscription directly.
		return sub, nil
	case subscriber + "GetSubscription":
		return &pb.GetSubscriptionRequest{Subscription: restSubscriptionName(project, request.Path["subscription"])}, nil
	case subscriber + "DeleteSubscription":
		return &pb.DeleteSubscriptionRequest{Subscription: restSubscriptionName(project, request.Path["subscription"])}, nil
	default:
		return &pb.ListSubscriptionsRequest{Project: "projects/" + project}, nil
	}
}

// durationFromSeconds parses one "3600s" duration string; empty stays nil.
func durationFromSeconds(raw string) *durationpb.Duration {
	if raw == "" {
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return nil
	}
	return durationpb.New(parsed)
}

// secondsString renders one protobuf duration in the wire "3600s" shape.
func secondsString(value *durationpb.Duration) string {
	return strconv.FormatInt(value.GetSeconds(), 10) + "s"
}

// restStringMap extracts one string map from a JSON body.
func restStringMap(raw []byte, key string) map[string]string {
	var body map[string]json.RawMessage
	if json.Unmarshal(raw, &body) != nil {
		return nil
	}
	segment, ok := body[key]
	if !ok {
		return nil
	}
	var labels map[string]string
	if json.Unmarshal(segment, &labels) != nil {
		return nil
	}
	return labels
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
		}
		if retention := protoMessage.GetMessageRetentionDuration(); retention != nil {
			wire["messageRetentionDuration"] = secondsString(retention)
		}
		if push := protoMessage.GetPushConfig(); push != nil && (push.GetPushEndpoint() != "" || push.GetOidcToken().GetServiceAccountEmail() != "") {
			wire["pushConfig"] = map[string]any{"oidcToken": map[string]any{
				"serviceAccountEmail": protoMessage.GetPushConfig().GetOidcToken().GetServiceAccountEmail(),
				"audience":            protoMessage.GetPushConfig().GetOidcToken().GetAudience(),
			}}
		}
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

// wireRestName converts a canonical //host name into the wire-relative name.
func wireRestName(name string) string {
	_, rest, found := strings.Cut(name, "googleapis.com/")
	if found {
		return rest
	}
	return name
}
