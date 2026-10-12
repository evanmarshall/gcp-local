package run

import (
	"fmt"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// methodServicesActAs is the engine-only plan for the deployer's act on a
// revision's runtime service account; it has no route. Cloud Tasks OIDC and
// Pub/Sub push check the same permission the same way.
const methodServicesActAs = "run.services.actAs"

// authorizeRuntimeAccount requires iam.serviceAccounts.actAs on the service
// account a create or update deploys as, before anything is stored or
// launched. Every rollout is a deploy, so the check runs on each one. A
// template without an account deploys as the project's default compute
// account, NUMBER-compute@developer.gserviceaccount.com
// (https://cloud.google.com/run/docs/configuring/services/service-identity).
// GCP creates that account with the project; an environment that declares it
// checks its own policy, and otherwise it has none, so the project's decides.
func (p *Provider) authorizeRuntimeAccount(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) error {
	body, err := decodeRequest(request.JSON)
	if err != nil {
		return err
	}
	template, err := resolveTemplate(body.Template)
	if err != nil {
		return err
	}
	email, target, declared := p.runtimeAccount(template.ServiceAccount, scope)
	if !declared {
		return actAsDenied(email)
	}
	call.Method, call.Resource = methodServicesActAs, target
	_, err = p.engine.Authorize(call, iam.Target{Name: target}, nil)
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return actAsDenied(email)
	case codes.Unimplemented:
		return coverageGap("a runtime service account outside the service's project")
	default:
		return fmt.Errorf("authorize the runtime service account: %w", err)
	}
}

// runtimeAccount names the account a template deploys as and the resource
// whose policy decides actAs on it, and reports whether that account exists
// locally. The default compute account always exists.
func (p *Provider) runtimeAccount(email string, scope state.Scope) (string, string, bool) {
	project := provider.ProjectResource(scope.Project)
	if email != "" {
		account := iam.AccountResource(accountProject(email), email)
		return email, account, p.indexed(account)
	}
	resource, ok := p.index.Get(project)
	if !ok || resource.Number == "" {
		return "the default compute service account", project, true
	}
	email = resource.Number + "-compute@developer.gserviceaccount.com"
	if account := iam.AccountResource(scope.Project, email); p.indexed(account) {
		return email, account, true
	}
	return email, project, true
}

// indexed reports whether a resource is in the index.
func (p *Provider) indexed(name string) bool {
	_, ok := p.index.Get(name)
	return ok
}

// actAsDenied is the refusal Cloud Run returns for a deployer without actAs.
// GCP words a missing account the same way.
func actAsDenied(email string) error {
	return status.Errorf(codes.PermissionDenied,
		"Permission 'iam.serviceaccounts.actAs' denied on service account %s (or it may not exist).", email)
}

// accountProject is the project that owns a user-managed service account,
// named in its email domain.
func accountProject(email string) string {
	_, domain, _ := strings.Cut(email, "@")
	return strings.TrimSuffix(domain, ".iam.gserviceaccount.com")
}
