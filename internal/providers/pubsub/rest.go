package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"errors"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	restTopicsGetIam:        policyRPC + "GetIamPolicy",
	restTopicsSetIam:        policyRPC + "SetIamPolicy",
	restSubsGetIam:          policyRPC + "GetIamPolicy",
	restSubsSetIam:          policyRPC + "SetIamPolicy",
}

// registerRestPermissions binds every REST method to its twin's permission.
const (
	restTopicsGetIam = "pubsub.http.topics.getIamPolicy"
	restTopicsSetIam = "pubsub.http.topics.setIamPolicy"
	restSubsGetIam   = "pubsub.http.subscriptions.getIamPolicy"
	restSubsSetIam   = "pubsub.http.subscriptions.setIamPolicy"
)

func (p *Provider) registerRestPermissions() error {
	restPermissions := map[string]string{
		restTopicsGetIam: "pubsub.topics.getIamPolicy",
		restTopicsSetIam: "pubsub.topics.setIamPolicy",
		restSubsGetIam:   "pubsub.subscriptions.getIamPolicy",
		restSubsSetIam:   "pubsub.subscriptions.setIamPolicy",
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
		return projectResourceOf(project)
	case restTopicsCreate, restTopicsGet, restTopicsDelete, restTopicsGetIam, restTopicsSetIam:
		return restTopicName(project, request.Path["topic"])
	case restSubscriptionsList:
		return projectResourceOf(project)
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

// projectResourceOf renders the canonical project resource name.
func projectResourceOf(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
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
	isPolicy := strings.Contains(twin, "IAMPolicy")
	if isPolicy {
		// The policy surface is provider-owned: the resource name is the
		// canonical topic/subscription, not a backend resource.
		return p.handleRestPolicy(call, request, project, call.Method)
	}
	call.Method = twin // the twin drives authorization and the emulator call

	protoRequest, buildErr := p.buildRestProto(twin, project, request)
	if buildErr != nil {
		return nil, buildErr
	}
	if err := p.preflight(call, protoRequest); err != nil {
		return nil, err
	}
	response, err := p.restSession(call, protoRequest)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: restWire(call.Method, response)}, nil
}

// handleRestPolicy serves the REST topic/subscription IAM surface straight
// from the shared IAM engine, mirroring the gRPC policy handler's shape.
func (p *Provider) handleRestPolicy(call gateway.Call, request *gateway.RESTRequest, project, restMethod string) (*gateway.RESTResponse, error) {
	call.Method = restTwins[restMethod]
	if !strings.Contains(call.Resource, "/topics/") && !strings.Contains(call.Resource, "/subscriptions/") {
		return nil, status.Error(codes.InvalidArgument, "Invalid policy resource")
	}
	if call.Method == policyRPC+"GetIamPolicy" {
		policy, err := p.iam.GetPolicy(canonical(call.Resource))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read the policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy}, nil
	}
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.iam.SetPolicy(canonical(call.Resource), envelope.Policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write the policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// restSession opens the backend session and runs the same authorize, invoke,
// and settle flow the gRPC dispatch uses.
func (p *Provider) restSession(call gateway.Call, protoRequest proto.Message) (proto.Message, error) {
	var response proto.Message
	err := p.manager.Use(call.Context, keyOf(call), func(session context.Context, active backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		if err := p.authorize(call, protoRequest); err != nil {
			return err
		}
		return p.restForward(session, active, call, protoRequest, &response)
	})
	if err != nil {
		return nil, fmt.Errorf("run the pub/sub session: %w", err)
	}
	return response, nil
}

// restForward validates the mutation against the index, forwards to the
// emulator, and settles the push lifecycle.
func (p *Provider) restForward(session context.Context, active backend.Backend,
	call gateway.Call, protoRequest proto.Message, response *proto.Message,
) error {
	b, err := driverOf(active)
	if err != nil {
		return err
	}
	creating, deleting := mutationKind(call.Method)
	if creating {
		if _, found := p.index.Get(canonical(call.Resource)); found {
			return status.Error(codes.AlreadyExists, "Pub/Sub resource already exists")
		}
	}
	forwarded, push, splitErr := p.splitPushConfig(call, protoRequest)
	if splitErr != nil {
		return splitErr
	}
	*response, err = invoke(session, b, call.Method, forwarded)
	if !creating && !deleting {
		if err == nil {
			p.restorePush(*response)
		}
		return err
	}
	return p.settleMutation(session, b, call, *response, forwarded, push, creating, err)
}
