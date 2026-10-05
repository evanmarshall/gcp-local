package pubsub

import (
	"context"
	"encoding/base64"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
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
		if r.Options != nil {
			if err := fields(r.Options, "requested_policy_version"); err != nil {
				return nil, err
			}
			if r.Options.RequestedPolicyVersion != 0 && r.Options.RequestedPolicyVersion != 1 {
				return nil, gap("requested IAM policy version")
			}
		}
		call.Method = "pubsub." + kind + ".getIamPolicy"
	case *iampb.SetIamPolicyRequest:
		if err := fields(r, "resource", "policy", "update_mask"); err != nil {
			return nil, err
		}
		if r.UpdateMask != nil {
			return nil, gap("partial IAM policy updates")
		}
		if r.Policy == nil {
			return nil, invalid("Missing IAM policy")
		}
		if err := fields(r.Policy, "version", "bindings", "etag"); err != nil {
			return nil, err
		}
		policy := iam.Policy{Version: int(r.Policy.Version), ETag: base64.StdEncoding.EncodeToString(r.Policy.Etag)}
		for _, binding := range r.Policy.Bindings {
			if binding == nil {
				return nil, invalid("Missing IAM binding")
			}
			if err := fields(binding, "role", "members"); err != nil {
				return nil, err
			}
			policy.Bindings = append(policy.Bindings, iam.Binding{Role: binding.Role, Members: append([]string(nil), binding.Members...)})
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
		return err
	})
	if err != nil {
		return nil, err
	}
	etag, err := base64.StdEncoding.DecodeString(result.ETag)
	if err != nil {
		return nil, err
	}
	response := &iampb.Policy{Version: int32(result.Version), Etag: etag}
	for _, binding := range result.Bindings {
		response.Bindings = append(response.Bindings, &iampb.Binding{Role: binding.Role, Members: binding.Members})
	}
	return response, nil
}
