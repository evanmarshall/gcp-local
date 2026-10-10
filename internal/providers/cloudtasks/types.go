package cloudtasks

import (
	"net/url"
	"regexp"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// serviceName is the provider and backend registry name.
const serviceName = "cloudtasks"

// uidPattern pins the queue and task name shapes.
var uidPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_.~+]{0,254}$`)

// maxPayloadBounds the task HTTP body size (1 MiB, the cloud limit).
const maxPayload = 1 << 20

// scopeOf builds the location scope of one queue or task path.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// wirePrefix is the canonical resource prefix.
const wirePrefix = "//cloudtasks.googleapis.com/"

// queueResource builds the canonical queue name.
func queueResource(project, location, id string) string {
	return wirePrefix + "projects/" + url.PathEscape(project) + "/locations/" +
		url.PathEscape(location) + "/queues/" + url.PathEscape(id)
}

// taskResource builds the canonical task name.
func taskResource(project, location, queue, id string) string {
	return queueResource(project, location, queue) + "/tasks/" + url.PathEscape(id)
}

// notFound reports the Cloud Tasks not-found status.
func notFound(kind, name string) error {
	return statusNotFound("The resource '" + kind + "/" + name + "' was not found")
}

// stamp renders the Cloud Tasks timestamp shape.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }
