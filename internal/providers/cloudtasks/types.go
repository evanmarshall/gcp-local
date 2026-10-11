package cloudtasks

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// serviceName is the provider and backend registry name.
const serviceName = "cloudtasks"

// queuePattern and taskPattern pin the documented queue and task ID shapes.
var (
	queuePattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,100}$`)
	taskPattern  = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)
)

// maxPayload bounds the decoded task HTTP body size (1 MiB, the cloud limit).
const maxPayload = 1 << 20

// scopeOf builds the location scope of one queue or task path.
func scopeOf(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// wirePrefix is the full resource-name prefix of the index and IAM policies.
const wirePrefix = "//cloudtasks.googleapis.com/"

// queueName builds the relative queue name GCP returns on the wire.
func queueName(project, location, id string) string {
	return "projects/" + url.PathEscape(project) + "/locations/" +
		url.PathEscape(location) + "/queues/" + url.PathEscape(id)
}

// queueResource builds the full queue name the index and IAM policies use.
func queueResource(project, location, id string) string {
	return wirePrefix + queueName(project, location, id)
}

// taskName builds the relative task name GCP returns on the wire.
func taskName(project, location, queue, id string) string {
	return queueName(project, location, queue) + "/tasks/" + url.PathEscape(id)
}

// notFound reports the Cloud Tasks not-found status.
func notFound(kind, name string) error {
	return statusNotFound("The resource '" + kind + "/" + name + "' was not found")
}

// stamp renders the Cloud Tasks timestamp shape.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }

// parseDuration reads a protobuf JSON duration such as "1s" or "0.100s".
func parseDuration(value string) (time.Duration, bool) {
	seconds, found := strings.CutSuffix(value, "s")
	if !found {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(seconds, 64)
	if err != nil || parsed < 0 || parsed > 315576000000 {
		return 0, false
	}
	return time.Duration(parsed * float64(time.Second)), true
}

// formatDuration renders a protobuf JSON duration the way GCP echoes it:
// whole seconds bare, otherwise millisecond or nanosecond precision.
func formatDuration(value time.Duration) string {
	whole, fraction := int64(value/time.Second), int64(value%time.Second)
	switch {
	case fraction == 0:
		return fmt.Sprintf("%ds", whole)
	case fraction%int64(time.Millisecond) == 0:
		return fmt.Sprintf("%d.%03ds", whole, fraction/int64(time.Millisecond))
	default:
		return fmt.Sprintf("%d.%09ds", whole, fraction)
	}
}
