package pubsub

import (
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// A schema recreated with a deleted schema's name starts with an empty
// policy: removing the schema from the index drops its grants, as GCP does.
func TestRecreatedSchemaStartsWithEmptyPolicy(t *testing.T) {
	h := schemaFixture(t)
	createProto(t, h.as)
	set := &iampb.SetIamPolicyRequest{Resource: protoSchema1, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/pubsub.viewer", Members: []string{consumer}}}}}
	_, err := h.as(setIAM, protoSchema1, set)
	must(t, err)
	_, err = h.as(schemaService+"DeleteSchema", protoSchema1, &pb.DeleteSchemaRequest{Name: protoSchema1})
	must(t, err)
	createProto(t, h.as)
	raw, err := h.as(getIAM, protoSchema1, &iampb.GetIamPolicyRequest{Resource: protoSchema1})
	must(t, err)
	policy, ok := raw.(*iampb.Policy)
	failIf(t, !ok || len(policy.GetBindings()) != 0, "the recreated schema kept the deleted schema's grants", raw)
}
