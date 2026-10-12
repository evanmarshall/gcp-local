package pubsub

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestIAMPolicyRoundTripOverGRPC sets and reads topic and subscription
// policies through the IAMPolicy service and proves the grants take effect.
func TestIAMPolicyRoundTripOverGRPC(t *testing.T) {
	_, registry, b := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	_, err = invokeAs(registry, owner, subscriber+"CreateSubscription", testSub, &pb.Subscription{Name: testSub, Topic: testTopic})
	must(t, err)
	publish := &pb.PublishRequest{Topic: testTopic, Messages: []*pb.PubsubMessage{{Data: []byte("x")}}}
	_, err = invokeAs(registry, consumer, publisher+"Publish", testTopic, publish)
	expectCode(t, err, codes.PermissionDenied)

	set := &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.publisher", Members: []string{consumer}}}}}
	raw, err := invokeAs(registry, owner, setIAM, testTopic, set)
	must(t, err)
	written, ok := raw.(*iampb.Policy)
	if !ok || len(written.GetBindings()) != 1 || len(written.GetEtag()) == 0 {
		t.Fatalf("set policy = %v", raw)
	}
	raw, err = invokeAs(registry, owner, getIAM, testTopic, &iampb.GetIamPolicyRequest{Resource: testTopic, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 3}})
	must(t, err)
	read, _ := raw.(*iampb.Policy)
	if len(read.GetBindings()) != 1 || read.GetBindings()[0].GetMembers()[0] != consumer {
		t.Fatalf("get policy = %v", read)
	}
	_, err = invokeAs(registry, consumer, publisher+"Publish", testTopic, publish)
	must(t, err)

	// The topic grant does not reach the subscription's policy surface.
	_, err = invokeAs(registry, consumer, getIAM, testSub, &iampb.GetIamPolicyRequest{Resource: testSub})
	expectCode(t, err, codes.PermissionDenied)
	_, err = invokeAs(registry, owner, setIAM, testSub, &iampb.SetIamPolicyRequest{Resource: testSub, Policy: &iampb.Policy{
		Version:  3,
		Bindings: []*iampb.Binding{{Role: roleSubscriber, Members: []string{consumer}, Condition: &expr.Expr{Expression: `request.time < timestamp("2999-01-01T00:00:00Z")`}}},
	}})
	must(t, err)
	if b.called(setIAM) != 0 {
		t.Fatal("the provider-owned policy surface reached the emulator")
	}
}

// TestIAMPolicyRequestsAreValidated covers malformed and unsupported IAM
// requests, foreign callers, and missing resources.
func TestIAMPolicyRequestsAreValidated(t *testing.T) {
	_, registry, _ := memoryFixture(t)
	_, err := invokeAs(registry, owner, publisher+"CreateTopic", testTopic, &pb.Topic{Name: testTopic})
	must(t, err)
	binding := []*iampb.Binding{{Role: roleViewer, Members: []string{consumer}}}
	tests := []struct {
		name, actor, resource string
		method                string
		request               any
		code                  codes.Code
	}{
		{"invalid name", owner, shortTopicName, getIAM, &iampb.GetIamPolicyRequest{Resource: shortTopicName}, codes.InvalidArgument},
		{"project mismatch", owner, foreignTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: foreignTopic}, codes.InvalidArgument},
		{"non-proto", owner, testTopic, getIAM, "raw", codes.InvalidArgument},
		{"foreign request type", owner, testTopic, getIAM, &pb.Topic{Name: testTopic}, codes.Unimplemented},
		{"version two", owner, testTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: testTopic, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: 2}}, codes.Unimplemented},
		{"update mask", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: binding}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bindings"}}}, codes.Unimplemented},
		{"missing policy", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic}, codes.InvalidArgument},
		{"nil binding", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: []*iampb.Binding{nil}}}, codes.InvalidArgument},
		{"audit configs", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{AuditConfigs: []*iampb.AuditConfig{{Service: "allServices"}}}}, codes.Unimplemented},
		{"condition without v3", owner, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Version: 1, Bindings: []*iampb.Binding{{Role: roleViewer, Members: []string{consumer}, Condition: &expr.Expr{Expression: "true"}}}}}, codes.InvalidArgument},
		{"foreign caller", other, testTopic, setIAM, &iampb.SetIamPolicyRequest{Resource: testTopic, Policy: &iampb.Policy{Bindings: binding}}, codes.PermissionDenied},
		{"missing topic", owner, absentTopic, getIAM, &iampb.GetIamPolicyRequest{Resource: absentTopic}, codes.NotFound},
	}
	for _, test := range tests {
		_, err := registry.Invoke(call(test.method, test.resource, test.actor), test.request)
		if got := status.Code(err); got != test.code {
			t.Errorf("%s: got %v (%v), want %v", test.name, got, err, test.code)
		}
	}
}
