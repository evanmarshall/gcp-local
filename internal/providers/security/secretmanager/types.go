package secretmanager

import (
	"net/url"
	"regexp"
)

// Wire document keys shared by the response shapes.
const keyName = "name"

// serviceName is the provider and backend registry name.
const serviceName = "secretmanager"

// uidPattern pins the Secret Manager name shape.
var uidPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_]{0,254}$`)

// wirePrefix starts the "//" resource form the IAM index requires.
const wirePrefix = "//secretmanager.googleapis.com/"

func secretResource(project, id string) string {
	return wirePrefix + "projects/" + url.PathEscape(project) + "/secrets/" + url.PathEscape(id)
}

// secretWireName builds the document name: the relative form the real API
// returns, without the index's "//host" prefix.
func secretWireName(project, id string) string {
	return "projects/" + url.PathEscape(project) + "/secrets/" + url.PathEscape(id)
}

// versionWireName builds the document name of one version.
func versionWireName(project, id, version string) string {
	return secretWireName(project, id) + "/versions/" + url.PathEscape(version)
}
