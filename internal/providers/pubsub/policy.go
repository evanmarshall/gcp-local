package pubsub

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (p *Provider) policy(call gateway.Call, raw any) (any, error) {
	project, err := resourceName(call.Resource, "")
	if err != nil {
		return nil, err
	}
	if project != call.Project {
		return nil, invalid("Policy project mismatch")
	}
	kind := strings.Split(call.Resource, "/")[2]
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, invalid("Invalid IAM request")
	}
	var update *iam.Policy
	switch r := request.(type) {
	case *iampb.GetIamPolicyRequest:
		if err := fields(r, "resource", "options"); err != nil {
			return nil, err
		}
		if r.GetOptions() != nil {
			if err := fields(r.GetOptions(), "requested_policy_version"); err != nil {
				return nil, err
			}
			if r.GetOptions().GetRequestedPolicyVersion() != 0 && r.GetOptions().GetRequestedPolicyVersion() != 1 {
				return nil, gap("requested IAM policy version")
			}
		}
		call.Method = "pubsub." + kind + ".getIamPolicy"
	case *iampb.SetIamPolicyRequest:
		if err := fields(r, "resource", "policy", "update_mask"); err != nil {
			return nil, err
		}
		if r.GetUpdateMask() != nil {
			return nil, gap("partial IAM policy updates")
		}
		if r.GetPolicy() == nil {
			return nil, invalid("Missing IAM policy")
		}
		if err := fields(r.GetPolicy(), "version", "bindings", "etag"); err != nil {
			return nil, err
		}
		policy := iam.Policy{Version: int(r.GetPolicy().GetVersion()), ETag: base64.StdEncoding.EncodeToString(r.GetPolicy().GetEtag())}
		for _, binding := range r.GetPolicy().GetBindings() {
			if binding == nil {
				return nil, invalid("Missing IAM binding")
			}
			if err := fields(binding, "role", "members"); err != nil {
				return nil, err
			}
			policy.Bindings = append(policy.Bindings, iam.Binding{Role: binding.GetRole(), Members: append([]string(nil), binding.GetMembers()...)})
		}
		update = &policy
		call.Method = "pubsub." + kind + ".setIamPolicy"
	default:
		return nil, gap("IAM method")
	}
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	var result iam.Policy
	err = p.manager.Use(call.Context, keyOf(call), func(ctx context.Context, _ backend.Backend) error {
		call.Context = ctx
		if err := p.authorize(call, request); err != nil {
			return err
		}
		var err error
		if update == nil {
			result, err = p.iam.GetPolicy(canonical(call.Resource))
		} else {
			result, err = p.iam.SetPolicy(canonical(call.Resource), *update)
		}
		if err != nil {
			return fmt.Errorf("set iam policy: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("apply policy in the backend session: %w", err)
	}
	etag, err := base64.StdEncoding.DecodeString(result.ETag)
	if err != nil {
		return nil, fmt.Errorf("decode policy etag: %w", err)
	}
	if result.Version < 0 || result.Version > math.MaxInt32 {
		return nil, status.Error(codes.Internal, "Policy version is outside the wire range")
	}
	response := &iampb.Policy{Version: int32(result.Version), Etag: etag}
	for _, binding := range result.Bindings {
		response.Bindings = append(response.Bindings, &iampb.Binding{Role: binding.Role, Members: binding.Members})
	}
	return response, nil
}
