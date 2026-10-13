// Package orgpolicy implements the Organization Policy v2 API on
// organizations, folders, and projects for the constraints the emulator
// enforces. Policies live in the IAM engine, which bootstrap declarations and
// this API share, and which the enforcing providers consult at each mutation.
// Custom constraints are not implemented.
package orgpolicy

import (
	"errors"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Verbs of the REST method IDs, "orgpolicy.COLLECTION.VERB"; the generated
// route table spells each one under projects, folders, and organizations.
const (
	verbConstraintsList = "constraints.list"
	verbPoliciesList    = "policies.list"
	verbPoliciesCreate  = "policies.create"
	verbPoliciesGet     = "policies.get"
	verbPoliciesGetEff  = "policies.getEffectivePolicy"
	verbPoliciesPatch   = "policies.patch"
	verbPoliciesDelete  = "policies.delete"
)

// permissions is each verb's permission, from the v2 reference. get and
// getEffectivePolicy both need orgpolicy.policy.get (recorded).
var permissions = map[string]string{
	verbConstraintsList: "orgpolicy.constraints.list",
	verbPoliciesList:    "orgpolicy.policies.list",
	verbPoliciesCreate:  "orgpolicy.policies.create",
	verbPoliciesGet:     "orgpolicy.policy.get",
	verbPoliciesGetEff:  "orgpolicy.policy.get",
	verbPoliciesPatch:   "orgpolicy.policies.update",
	verbPoliciesDelete:  "orgpolicy.policies.delete",
}

// collections are the parents every method serves, with each route's path
// variable.
var collections = map[string]string{"projects": "project", "folders": "folder", "organizations": "organization"}

// split parses "orgpolicy.COLLECTION.VERB".
func split(method string) (string, string) {
	rest := strings.TrimPrefix(method, "orgpolicy.")
	collection, verb, _ := strings.Cut(rest, ".")
	return collection, verb
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("orgpolicy requires registry, index, and iam")

// gap refuses an unmodeled request shape with UNIMPLEMENTED.
var gap = provider.CoverageGap("Organization Policy")

// Error domains of the recorded denials: methods on a parent name the
// Resource Manager node, methods on a policy name the policy.
const (
	parentDomain = "cloudresourcemanager.googleapis.com"
	policyDomain = "orgpolicy.googleapis.com"
)
