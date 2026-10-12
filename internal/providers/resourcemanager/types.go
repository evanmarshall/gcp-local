// Package resourcemanager implements the project-level IAM policy surface.
// Project create/list stays out of scope: projects exist only as index
// entries seeded at startup.
package resourcemanager

import (
	"errors"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method name constants; the generated route table uses the same identifiers.
const (
	methodProjectsGetIam = "resourcemanager.projects.getIamPolicy"
	methodProjectsSetIam = "resourcemanager.projects.setIamPolicy"
)

// permissions follows the documented access-control rows.
var permissions = map[string]string{
	methodProjectsGetIam: "resourcemanager.projects.getIamPolicy",
	methodProjectsSetIam: "resourcemanager.projects.setIamPolicy",
}

var projectIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,28}[a-z0-9]$`)

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("resourcemanager requires registry, manager, index, and iam")

// notFound uses Resource Manager's quoted-project text, not the shared kind/name form.
func notFound(project string) error {
	return status.Errorf(codes.NotFound, "The project %q was not found", project)
}
