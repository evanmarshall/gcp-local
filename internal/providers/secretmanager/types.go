package secretmanager

import (
	"net/url"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Wire document keys shared by the response shapes.
const keyName = "name"

// serviceName is the provider and backend registry name.
const serviceName = "secretmanager"

// uidPattern pins the Secret Manager name shape.
var uidPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_]{0,254}$`)

// scopeOf builds the location scope of one secret path.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// wirePrefix is the canonical resource prefix.
const wirePrefix = "//secretmanager.googleapis.com/"

// secretResource builds the canonical secret name.
func secretResource(project, id string) string {
	return wirePrefix + "projects/" + url.PathEscape(project) + "/secrets/" + url.PathEscape(id)
}

// versionResource builds the canonical version name.
func versionResource(project, id, version string) string {
	return secretResource(project, id) + "/versions/" + url.PathEscape(version)
}

// stamp renders the Secret Manager timestamp shape.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
