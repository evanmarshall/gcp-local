package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/api/generated/contract"
	pubsubapi "github.com/evanmarshall/gcp-local/api/generated/messaging/pubsub"
	"github.com/evanmarshall/gcp-local/api/generated/mixin"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

// REST surface sentinels: an unknown REST method or a twin without a
// permission mapping is a provider wiring defect, not caller input.
var (
	errRestUnknownTwin    = errors.New("no permission mapping for the REST twin")
	errRestNoProtoMapping = errors.New("no proto mapping for the REST twin")
)

// REST method ids; the generated route table uses the same identifiers. Each
// REST method maps to its gRPC twin's permission and emulator call.
const (
	restTopicsCreate        = "pubsub.http.topics.create"
	restTopicsGet           = "pubsub.http.topics.get"
	restTopicsDelete        = "pubsub.http.topics.delete"
	restTopicsList          = "pubsub.http.topics.list"
	restSubscriptionsCreate = "pubsub.http.subscriptions.create"
	restSubscriptionsGet    = "pubsub.http.subscriptions.get"
	restSubscriptionsDelete = "pubsub.http.subscriptions.delete"
	restSubscriptionsList   = "pubsub.http.subscriptions.list"
	restTopicsPublish       = "pubsub.http.topics.publish"
	restSubscriptionsPull   = "pubsub.http.subscriptions.pull"
	restSubscriptionsAck    = "pubsub.http.subscriptions.acknowledge"
	restSubscriptionsModAck = "pubsub.http.subscriptions.modifyAckDeadline"
)

// restTwins maps each REST method to its gRPC twin: the emulator call and the
// permission the REST method evaluates.
var restTwins = map[string]string{
	restTopicsCreate:        publisher + "CreateTopic",
	restTopicsGet:           publisher + "GetTopic",
	restTopicsDelete:        publisher + "DeleteTopic",
	restTopicsList:          publisher + "ListTopics",
	restSubscriptionsCreate: subscriber + "CreateSubscription",
	restSubscriptionsGet:    subscriber + "GetSubscription",
	restSubscriptionsDelete: subscriber + "DeleteSubscription",
	restSubscriptionsList:   subscriber + "ListSubscriptions",
	restTopicsPublish:       publisher + "Publish",
	restSubscriptionsPull:   subscriber + "Pull",
	restSubscriptionsAck:    subscriber + "Acknowledge",
	restSubscriptionsModAck: subscriber + "ModifyAckDeadline",
	restTopicsGetIam:        policyRPC + "GetIamPolicy",
	restTopicsSetIam:        policyRPC + "SetIamPolicy",
	restSubsGetIam:          policyRPC + "GetIamPolicy",
	restSubsSetIam:          policyRPC + "SetIamPolicy",
}

// REST IAM policy methods share one gRPC twin, so each carries its own permission.
const (
	restTopicsGetIam = "pubsub.http.topics.getIamPolicy"
	restTopicsSetIam = "pubsub.http.topics.setIamPolicy"
	restSubsGetIam   = "pubsub.http.subscriptions.getIamPolicy"
	restSubsSetIam   = "pubsub.http.subscriptions.setIamPolicy"
)

// registerRestPermissions binds every REST method to its twin's permission.
func (p *Provider) registerRestPermissions() error {
	restPermissions := map[string]string{
		restTopicsGetIam: permTopicsGetIam,
		restTopicsSetIam: permTopicsSetIam,
		restSubsGetIam:   permSubscriptionsGetIam,
		restSubsSetIam:   permSubscriptionsSetIam,
	}
	for method, twin := range restTwins {
		permission, ok := restPermissions[method]
		if !ok {
			permission, ok = permissions[twin]
		}
		if !ok {
			return fmt.Errorf("%w: %s", errRestUnknownTwin, twin)
		}
		if err := p.iam.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// restResourceFor derives the canonical resource name for one REST request.
func restResourceFor(method, project string, request *gateway.RESTRequest) string {
	switch method {
	case restTopicsList:
		return provider.ProjectResource(project)
	case restTopicsCreate, restTopicsGet, restTopicsDelete, restTopicsGetIam, restTopicsSetIam, restTopicsPublish:
		return restTopicName(project, request.Path["topic"])
	case restSubscriptionsList:
		return provider.ProjectResource(project)
	case restSubsGetIam, restSubsSetIam:
		return restSubscriptionName(project, request.Path["subscription"])
	default:
		return restSubscriptionName(project, request.Path["subscription"])
	}
}

// restTopicName renders the relative topic resource name.
func restTopicName(project, topic string) string {
	return "projects/" + project + "/topics/" + topic
}

// restSubscriptionName renders the relative subscription resource name.
func restSubscriptionName(project, sub string) string {
	return "projects/" + project + "/subscriptions/" + sub
}

// handleRest is the gateway entry point for the JSON REST surface. The
// provider mirrors the gRPC dispatch: preflight, backend session, authorize,
// invoke the emulator twin, settle the mutation, and render the JSON shape.
func (p *Provider) handleRest(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub REST request")
	}
	twin := restTwins[call.Method]
	if twin == "" {
		return nil, status.Error(codes.Internal, "Unknown Pub/Sub REST method")
	}
	project := request.Path["project"]
	call.Resource = restResourceFor(call.Method, project, request)
	if strings.Contains(twin, "IAMPolicy") {
		// The policy surface is provider-owned: the resource name is the
		// canonical topic/subscription, not a backend resource.
		return p.handleRestPolicy(call, request, project)
	}
	call.Method = twin // the twin drives authorization and the emulator call
	protoRequest, err := p.buildRestProto(twin, project, request)
	if err != nil {
		return nil, err
	}
	if err := p.preflight(call, protoRequest); err != nil {
		return nil, err
	}
	response, err := p.run(call, protoRequest)
	if err != nil {
		return nil, err
	}
	if dataPlane(call.Method) {
		return restDataWire(response)
	}
	return &gateway.RESTResponse{JSON: restWire(call.Method, response)}, nil
}

// handleRestPolicy serves the REST topic/subscription IAM surface through the
// same validation, authorization, and engine update as the gRPC handler.
func (p *Provider) handleRestPolicy(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	call.Method = restTwins[call.Method]
	call.Project = project
	var protoRequest proto.Message
	if call.Method == policyRPC+"GetIamPolicy" {
		protoRequest = &iampb.GetIamPolicyRequest{Resource: call.Resource}
	} else {
		set := &iampb.SetIamPolicyRequest{}
		if protojson.Unmarshal(request.JSON, set) != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
		}
		set.Resource = call.Resource
		protoRequest = set
	}
	result, err := p.policy(call, protoRequest)
	if err != nil {
		return nil, err
	}
	policy, ok := result.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid IAM policy response")
	}
	encoded, err := protojson.Marshal(policy)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the IAM policy")
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded)}, nil
}

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

// The schema REST methods (projects.schemas.*) decode into the gRPC request,
// run through handleSchema or the policy handler, and encode the response with
// Google's JSON mapping, which is the REST wire shape.
const restSchemaPrefix = "pubsub.projects.schemas."

// registerRestSchemas binds each schema REST route and its twin's permission.
func (p *Provider) registerRestSchemas(registry *gateway.Registry) error {
	for _, route := range pubsubapi.PubSubSchemaRestRoutes {
		twin, permission := restSchemaTwin(route.Method)
		if twin == "" {
			return fmt.Errorf("%w: %s", errRestUnknownTwin, route.Method)
		}
		if err := p.iam.Register(route.Method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", route.Method, err)
		}
		if err := registry.Register(route.Method, p.handleRestSchema); err != nil {
			return fmt.Errorf("register %s: %w", route.Method, err)
		}
	}
	return nil
}

// restSchemaTwin returns the gRPC method a REST schema method runs and the
// permission it evaluates.
func restSchemaTwin(method string) (string, string) {
	twin := map[string]string{
		"create": "CreateSchema", "list": "ListSchemas", "validate": "ValidateSchema", "validateMessage": "ValidateMessage",
		"get": "GetSchema", "delete": "DeleteSchema", "listRevisions": "ListSchemaRevisions", "commit": "CommitSchema",
		"rollback": "RollbackSchema", "deleteRevision": "DeleteSchemaRevision", "getIamPolicy": "", "setIamPolicy": "",
	}
	verb := strings.TrimPrefix(method, restSchemaPrefix)
	rpc, found := twin[verb]
	switch {
	case !found:
		return "", ""
	case rpc == "":
		// The policy RPC's permission is per resource kind (pubsub.schemas.*).
		return policyRPC + strings.ToUpper(verb[:1]) + verb[1:], "pubsub.schemas." + verb
	}
	return schemaService + rpc, permissions[schemaService+rpc]
}

// handleRestSchema serves one schema REST call through its gRPC twin.
func (p *Provider) handleRestSchema(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub REST request")
	}
	twin, _ := restSchemaTwin(call.Method)
	call.Project = request.Path["project"]
	parent := "projects/" + call.Project
	call.Resource = parent
	if schema := request.Path["schema"]; schema != "" {
		call.Resource = parent + "/schemas/" + schema
	}
	call.Method = twin
	message, err := restSchemaProto(twin, call.Resource, request)
	if err != nil {
		return nil, err
	}
	var result any
	if strings.HasPrefix(twin, policyRPC) {
		result, err = p.policy(call, message)
	} else {
		result, err = p.handleSchema(call, message)
	}
	if err != nil {
		return nil, err
	}
	response, ok := result.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Pub/Sub schema response")
	}
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the schema response")
	}
	return &gateway.RESTResponse{JSON: json.RawMessage(encoded)}, nil
}

// restSchemaProto builds the gRPC request: the body (when the method takes
// one) with Google's JSON mapping, the path resource in the contract's
// resource field, then the documented query parameters.
func restSchemaProto(twin, resource string, request *gateway.RESTRequest) (proto.Message, error) {
	var rpc contract.RPC
	for _, candidate := range append(append([]contract.RPC(nil), pubsubapi.PubSubMethods...), mixin.IAMMethods...) {
		if candidate.Method == twin {
			rpc = candidate
		}
	}
	if rpc.NewRequest == nil {
		return nil, fmt.Errorf("%w: %s", errRestNoProtoMapping, twin)
	}
	message := rpc.NewRequest()
	target := message
	if create, ok := message.(*pb.CreateSchemaRequest); ok {
		create.Schema = &pb.Schema{}
		target = create.GetSchema() // The create body is the Schema itself.
	}
	if err := restBody(request, target); err != nil {
		return nil, err
	}
	reflected := message.ProtoReflect()
	reflected.Set(reflected.Descriptor().Fields().ByName(protoreflect.Name(rpc.ResourceField)), protoreflect.ValueOfString(resource))
	if request.Query.Get("revisionId") != "" {
		return nil, gap("deleteRevision revisionId; name the revision as SCHEMA@REVISION")
	}
	if version := request.Query.Get("options.requestedPolicyVersion"); version != "" {
		get, ok := message.(*iampb.GetIamPolicyRequest)
		number, err := strconv.ParseInt(version, 10, 32)
		if !ok || err != nil {
			return nil, provider.Invalid("Invalid options.requestedPolicyVersion")
		}
		get.Options = &iampb.GetPolicyOptions{RequestedPolicyVersion: int32(number)}
	}
	return message, restQuery(reflected, request.Query)
}

// restQuery sets the schema methods' query parameters (schemaId, view,
// pageSize, pageToken) on the request fields of the same name.
func restQuery(message protoreflect.Message, query url.Values) error {
	for parameter, name := range map[string]protoreflect.Name{"schemaId": "schema_id", "view": "view", "pageSize": "page_size", "pageToken": "page_token"} {
		value := query.Get(parameter)
		field := message.Descriptor().Fields().ByName(name)
		if value == "" || field == nil {
			continue
		}
		switch field.Kind() {
		case protoreflect.EnumKind:
			number := field.Enum().Values().ByName(protoreflect.Name(value))
			if number == nil || number.Number() == 0 {
				return provider.Invalid("Invalid " + parameter)
			}
			message.Set(field, protoreflect.ValueOfEnum(number.Number()))
		case protoreflect.Int32Kind:
			size, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return provider.Invalid("Invalid " + parameter)
			}
			message.Set(field, protoreflect.ValueOfInt32(int32(size)))
		default:
			message.Set(field, protoreflect.ValueOfString(value))
		}
	}
	return nil
}
