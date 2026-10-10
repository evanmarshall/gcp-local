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
	request, ok := raw.(proto.Message)
	if !ok {
		return nil, invalid("Invalid IAM request")
	}
	update, err := p.policyUpdate(&call, request)
	if err != nil {
		return nil, err
	}
	if err := p.authorize(call, request); err != nil {
		return nil, err
	}
	result, err := p.applyPolicy(call, request, update)
	if err != nil {
		return nil, err
	}
	return wirePolicy(result)
}

// policyUpdate validates the IAM request and turns a SetIamPolicy into the
// engine policy. The call method is renamed to the resolved IAM operation.
func (p *Provider) policyUpdate(call *gateway.Call, request proto.Message) (*iam.Policy, error) {
	kind := strings.Split(call.Resource, "/")[2]
	switch r := request.(type) {
	case *iampb.GetIamPolicyRequest:
		if err := fields(r, "resource", "options"); err != nil {
			return nil, err
		}
		if err := validatePolicyOptions(r.GetOptions()); err != nil {
			return nil, err
		}
		call.Method = "pubsub." + kind + ".getIamPolicy"
		return nil, nil
	case *iampb.SetIamPolicyRequest:
		policy, err := decodeSetPolicy(r)
		if err != nil {
			return nil, err
		}
		call.Method = "pubsub." + kind + ".setIamPolicy"
		return policy, nil
	default:
		return nil, gap("IAM method")
	}
}

// validatePolicyOptions accepts only policy version 1, which is what the
// emulator's conditional-policy gap already excludes.
func validatePolicyOptions(options *iampb.GetPolicyOptions) error {
	if options == nil {
		return nil
	}
	if err := fields(options, "requested_policy_version"); err != nil {
		return err
	}
	if version := options.GetRequestedPolicyVersion(); version != 0 && version != 1 {
		return gap("requested IAM policy version")
	}
	return nil
}

// decodeSetPolicy converts the wire policy into the engine representation.
func decodeSetPolicy(r *iampb.SetIamPolicyRequest) (*iam.Policy, error) {
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
	return &policy, nil
}

// applyPolicy runs the get or set inside one backend session.
func (p *Provider) applyPolicy(call gateway.Call, request proto.Message, update *iam.Policy) (iam.Policy, error) {
	var result iam.Policy
	err := p.manager.Use(call.Context, keyOf(call), func(session context.Context, _ backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		if err := p.authorize(call, request); err != nil {
			return err
		}
		var policyErr error
		if update == nil {
			result, policyErr = p.iam.GetPolicy(canonical(call.Resource))
		} else {
			result, policyErr = p.iam.SetPolicy(canonical(call.Resource), *update)
		}
		if policyErr != nil {
			return fmt.Errorf("set iam policy: %w", policyErr)
		}
		return nil
	})
	if err != nil {
		return iam.Policy{}, fmt.Errorf("apply policy in the backend session: %w", err)
	}
	return result, nil
}

// wirePolicy converts the engine policy back to the wire representation.
func wirePolicy(result iam.Policy) (*iampb.Policy, error) {
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
