package orgpolicy

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"

	"cloud.google.com/go/orgpolicy/apiv2/orgpolicypb"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// nodePrefix is the index name prefix of every hierarchy node.
const nodePrefix = "//cloudresourcemanager.googleapis.com/"

// ParseName splits a policy name, "PARENT/policies/CONSTRAINT", into its
// parent ("projects/p", "folders/1", "organizations/1") and constraint ID.
func ParseName(name string) (string, string, bool) {
	parent, constraint, ok := strings.Cut(name, "/policies/")
	kind, id, cut := strings.Cut(parent, "/")
	if !ok || !cut || id == "" || strings.Contains(id, "/") || constraint == "" || strings.Contains(constraint, "/") {
		return "", "", false
	}
	switch kind {
	case "projects", "folders", "organizations":
		return parent, constraint, true
	}
	return "", "", false
}

// Node resolves a parent reference to its indexed hierarchy node. A project
// is named by ID or number; the v2 reference accepts both.
func Node(index *state.Index, parent string) (state.Resource, bool) {
	kind, id, _ := strings.Cut(parent, "/")
	resource, found := index.Get(nodePrefix + parent)
	if !found && kind == "projects" {
		resource, found = index.ProjectByNumber(id)
	}
	return resource, found && resource.Service == "resourcemanager" && resource.Kind == strings.TrimSuffix(kind, "s")
}

// Parent is a node's parent reference as GCP returns it: a project by
// number ("responses will return the name using the equivalent project
// number", Policy.name), or by ID when the bootstrap gave it none.
func Parent(node state.Resource) string {
	relative := strings.TrimPrefix(node.Name, nodePrefix)
	if node.Kind == "project" && node.Number != "" {
		return "projects/" + node.Number
	}
	return relative
}

// Seal sets a written policy's server-owned fields: each spec's update time
// and etag, then the policy's etag, all derived from the content.
func Seal(policy *orgpolicypb.Policy, now time.Time) {
	for _, spec := range []*orgpolicypb.PolicySpec{policy.GetSpec(), policy.GetDryRunSpec()} {
		if spec != nil {
			spec.Etag, spec.UpdateTime = "", timestamppb.New(now)
			spec.Etag = etag(spec)
		}
	}
	policy.Etag = ""
	policy.Etag = etag(policy)
}

// etag hashes a message's deterministic encoding.
func etag(message proto.Message) string {
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	sum := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(sum[:9])
}
