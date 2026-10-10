package iam

import (
	"errors"
	"regexp"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// ServiceAccount is an explicit local identity resource, not a cloud import or
// a credential. Private bootstrap owns it until an IAM account provider exists.
type ServiceAccount struct {
	Project  string `json:"project"`
	Email    string `json:"email"`
	UniqueID string `json:"unique_id"`
}

func AccountResource(project, email string) string {
	return "//iam.googleapis.com/projects/" + project + "/serviceAccounts/" + email
}

// errAccountShape rejects account declarations that cannot address a resource.
var errAccountShape = errors.New("local service accounts require a matching project email and numeric unique id")

func (a ServiceAccount) resource() (state.Resource, error) {
	scope := state.Scope{Project: a.Project, Location: state.Global}
	if err := scope.Validate(); err != nil {
		return state.Resource{}, err
	}
	pattern := `^[a-z][a-z0-9-]{4,28}[a-z0-9]@` + regexp.QuoteMeta(a.Project) + `\.iam\.gserviceaccount\.com$`
	if !regexp.MustCompile(pattern).MatchString(a.Email) || !regexp.MustCompile(`^[1-9][0-9]{5,20}$`).MatchString(a.UniqueID) {
		return state.Resource{}, errAccountShape
	}
	return state.Resource{Name: AccountResource(a.Project, a.Email), Service: "iam", Kind: "serviceAccount", Scope: scope,
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + a.Project, Number: a.UniqueID}, nil
}
