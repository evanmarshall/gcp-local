package cloudtasks

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// serviceName is the provider and backend registry name.
const serviceName = "cloudtasks"

// keyName is the name field of every Cloud Tasks resource.
const keyName = "name"

// queuePattern and taskPattern pin the documented queue and task ID shapes.
var (
	queuePattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,100}$`)
	taskPattern  = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)
)

// maxPayload bounds the decoded task HTTP body size (1 MiB, the cloud limit).
const maxPayload = 1 << 20

// wirePrefix is the full resource-name prefix of the index and IAM policies.
const wirePrefix = "//cloudtasks.googleapis.com/"

// locationName builds the relative location name a queue create or list
// denial reports (recording cloudtasks-dispatch-retry).
func locationName(project, location string) string {
	return "projects/" + url.PathEscape(project) + "/locations/" + url.PathEscape(location)
}

// queueName builds the relative queue name GCP returns on the wire.
func queueName(project, location, id string) string {
	return locationName(project, location) + "/queues/" + url.PathEscape(id)
}

// queueResource builds the full queue name the index and IAM policies use.
func queueResource(project, location, id string) string {
	return wirePrefix + queueName(project, location, id)
}

// taskName builds the relative task name GCP returns on the wire.
func taskName(project, location, queue, id string) string {
	return queueName(project, location, queue) + "/tasks/" + url.PathEscape(id)
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

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Cloud Tasks")

// statusExists reports a 409 for a duplicate queue or task.
func statusExists(message string) error {
	return status.Error(codes.AlreadyExists, message)
}

// queueOutput are the Queue fields GCP sets itself; a body may carry them
// and they are ignored, and an update mask may not name them. The v2beta3
// descriptors don't mark them OUTPUT_ONLY.
var queueOutput = []string{keyName, "state", "purgeTime", "stats"}

// unmodeledQueue are the Queue input fields the provider refuses unless
// unset: App Engine queues, queue-level HTTP target overrides, and Cloud
// Logging sampling.
var unmodeledQueue = provider.Unmodeled(&taskspb.Queue{},
	append([]string{"rateLimits", "retryConfig", "taskTtl", "tombstoneTtl", "type"}, queueOutput...)...)

// unmodeledTask are the CreateTaskRequest fields the provider refuses unless
// unset: App Engine and pull tasks, OAuth tokens, and per-task retry
// configuration. The task's output fields are accepted and ignored.
var unmodeledTask = provider.Unmodeled(&taskspb.CreateTaskRequest{},
	"parent", "responseView", "task.name", "task.scheduleTime", "task.dispatchDeadline",
	"task.httpRequest.url", "task.httpRequest.httpMethod", "task.httpRequest.headers", "task.httpRequest.body",
	"task.httpRequest.oidcToken",
	"task.createTime", "task.dispatchCount", "task.responseCount", "task.firstAttempt", "task.lastAttempt", "task.view")

// decodeQueue strictly decodes a queues.create or queues.patch body, which
// binds the request's queue field.
func decodeQueue(body []byte) (*taskspb.Queue, error) {
	queue := &taskspb.Queue{}
	if err := provider.DecodeProto(body, "queue", queue, gap, unmodeledQueue...); err != nil {
		return nil, err
	}
	return queue, nil
}

// decodeRequest strictly decodes a body that binds the whole request message
// (body: "*"): pause, resume, purge, run, and the IAM methods.
func decodeRequest(body []byte, message proto.Message, unmodeled ...string) error {
	return provider.DecodeProto(body, "", message, gap, unmodeled...)
}

// render returns a message's REST document.
func render(message proto.Message) map[string]any {
	raw, err := protojson.Marshal(message)
	var document map[string]any
	if err != nil || json.Unmarshal(raw, &document) != nil {
		return map[string]any{}
	}
	return document
}
