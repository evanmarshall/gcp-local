package provider

import (
	"fmt"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Denial message formats GCP uses, each taking the permission, then the
// resource. Services differ; a service's recorded denial decides.
const (
	// LacksPermission is Cloud Scheduler's (recording cloudscheduler-job-lifecycle).
	LacksPermission = `The principal (user or service account) lacks IAM permission "%s" for the resource "%s" (or the resource may not exist).`
	// PermissionDeniedOn is Memorystore's (recording memorystore-redis-instance-admin).
	PermissionDeniedOn = "Permission '%s' denied on '%s'"
)

// Authorizer is the IAM engine's authorization entry point; *iam.Engine
// implements it.
type Authorizer interface {
	Authorize(call gateway.Call, target iam.Target, features map[string]bool) (iam.Decision, error)
}

// Gate authorizes one provider's calls with the IAM engine and renders its
// refusals as GCP does.
type Gate struct {
	Engine Authorizer
	// Index holds the resources Nearest picks among.
	Index *state.Index
	// Message formats a 403's message from the permission and resource
	// (LacksPermission, PermissionDeniedOn).
	Message string
	// Domain is the ErrorInfo domain a 403 carries, the service's API host.
	// Empty sends no details, as Cloud Scheduler's and Memorystore's recorded
	// denials have none. Set it only from a recorded denial with details.
	Domain string
}

// Check authorizes call on target. It returns the decision and nil when the
// engine allows the call, the error GCP returns when it refuses (Denial), and
// any other engine refusal wrapped: Unverified reports one. resource is the
// relative name the denial reports, usually the addressed resource
// ("projects/p/locations/l/jobs/j") or, for create and list, its parent.
//
// Call Check inside the store callback that confirms an addressed resource
// exists, and return its error from the callback: a request racing a delete
// then sees NOT_FOUND, never an unverified target.
func (g Gate) Check(call gateway.Call, target iam.Target, resource string) (iam.Decision, error) {
	decision, err := g.Engine.Authorize(call, target, nil)
	switch status.Code(err) {
	case codes.OK:
		return decision, nil
	case codes.PermissionDenied, codes.Unauthenticated:
		return decision, g.Denial(call, decision, resource)
	default:
		return decision, fmt.Errorf("authorize %s: %w", call.Method, err)
	}
}

// Nearest authorizes call on the first of candidates the index holds, most
// specific first ("//x.googleapis.com/.../tables/t", its instance, its
// project). The last candidate, usually the project, is used whether or not
// it is indexed. resource is the name a denial reports; "" reports the
// chosen target's name. It returns the decision, the chosen target, and
// Check's error.
//
// It picks the target and calls Check inside store's callback for scope, so
// every index removal of the service's resources must hold that same scope
// lock. Then a concurrent delete can't unindex the target between the pick
// and the engine's check (which refuses an unindexed target as
// UNIMPLEMENTED); a missing resource is authorized at its parent and then
// reads 404. An indexed candidate never falls back to its parent, so an
// engine refusal on it stays closed, and a scope the store refuses fails
// closed.
func Nearest[T any](g Gate, store *state.Store[T], scope state.Scope, call gateway.Call, resource string, candidates ...string) (iam.Decision, iam.Target, error) {
	var decision iam.Decision
	var target iam.Target
	var checkErr error
	err := store.With(scope, func(*T) error {
		target = iam.Target{Name: candidates[len(candidates)-1]}
		for _, name := range candidates[:len(candidates)-1] {
			if _, indexed := g.Index.Get(name); indexed {
				target = iam.Target{Name: name}
				break
			}
		}
		reported := resource
		if reported == "" {
			reported = target.Name
		}
		decision, checkErr = g.Check(call, target, reported)
		return nil
	})
	if err != nil {
		return decision, target, fmt.Errorf("authorize in scope %s/%s: %w", scope.Project, scope.Location, err)
	}
	return decision, target, checkErr
}

// Denial is the error GCP returns when IAM refuses call: 401 UNAUTHENTICATED
// for a caller without valid credentials; otherwise 403 PERMISSION_DENIED
// naming the first missing permission and resource, with the ErrorInfo
// {reason IAM_PERMISSION_DENIED, Domain, metadata {permission, resource}}
// when Domain is set. Use it directly only for a check Check can't make
// (request features).
func (g Gate) Denial(call gateway.Call, decision iam.Decision, resource string) error {
	if call.Principal.State != gateway.Authenticated {
		return status.Error(codes.Unauthenticated, "Request had invalid authentication credentials.")
	}
	permission := DeniedPermission(decision)
	message := fmt.Sprintf(g.Message, permission, resource)
	if g.Domain == "" {
		return status.Error(codes.PermissionDenied, message)
	}
	return ErrorInfo(codes.PermissionDenied, message, "IAM_PERMISSION_DENIED", g.Domain,
		map[string]string{"permission": permission, "resource": resource})
}

// WithDecision sets the decision ID header, which the gateway forwards to gRPC
// clients as metadata, on an executed call's response.
func WithDecision(response *gateway.RESTResponse, decision iam.Decision) *gateway.RESTResponse {
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	return response
}
