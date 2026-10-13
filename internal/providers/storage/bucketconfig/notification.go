package bucketconfig

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
)

// Notification event types (notifications resource, event_types).
const (
	EventFinalize       = "OBJECT_FINALIZE"
	EventMetadataUpdate = "OBJECT_METADATA_UPDATE"
	EventDelete         = "OBJECT_DELETE"
	EventArchive        = "OBJECT_ARCHIVE"
)

// Payload formats (notifications resource, payload_format).
const (
	PayloadJSON = "JSON_API_V1"
	PayloadNone = "NONE"
)

var eventTypes = []string{EventFinalize, EventMetadataUpdate, EventDelete, EventArchive}

// TopicPrefix starts the full Pub/Sub topic name a configuration reports.
const TopicPrefix = "//pubsub.googleapis.com/"

// topicName matches projects/P/topics/T after an optional TopicPrefix.
var topicName = regexp.MustCompile(`^(//pubsub\.googleapis\.com/)?projects/[^/]+/topics/[^/]+$`)

// Notification is one notification configuration's request fields; the
// provider adds id, etag, kind, and selfLink.
type Notification struct {
	Topic            string            `json:"topic"`
	PayloadFormat    string            `json:"payload_format"`
	EventTypes       []string          `json:"event_types,omitempty"`
	CustomAttributes map[string]string `json:"custom_attributes,omitempty"`
	ObjectNamePrefix string            `json:"object_name_prefix,omitempty"`
}

// ParseNotification reads a notifications.insert body. A topic is required
// and must name a Pub/Sub topic, with or without TopicPrefix, and is
// reported with it; the payload format must be one of two. GCP drops event
// types it does not know, so a configuration naming none of the four gets
// every event (recorded: storage-notifications "event-unknown",
// "topic-missing", "topic-malformed", "payload-unknown").
func ParseNotification(raw json.RawMessage) (Notification, error) {
	var config Notification
	if json.Unmarshal(raw, &config) != nil {
		return Notification{}, GapError("notification configuration values of another JSON type")
	}
	switch {
	case config.Topic == "":
		return Notification{}, &RefusalError{Status: 400, Reason: "required", Message: "You must specify a Cloud Pub/Sub topic in the 'topic' field."}
	case !topicName.MatchString(config.Topic):
		return Notification{}, invalid("Invalid Google Cloud Pub/Sub topic. It should look like '//pubsub.googleapis.com/projects/*/topics/*.'")
	case config.PayloadFormat != PayloadJSON && config.PayloadFormat != PayloadNone:
		return Notification{}, &RefusalError{Status: 400, Reason: "required", Message: "You must specify a payload format in the 'payload_format' field."}
	}
	if !strings.HasPrefix(config.Topic, TopicPrefix) {
		config.Topic = TopicPrefix + config.Topic
	}
	config.EventTypes = slices.DeleteFunc(config.EventTypes, func(kind string) bool { return !slices.Contains(eventTypes, kind) })
	if len(config.EventTypes) == 0 {
		config.EventTypes = nil
	}
	return config, nil
}

// Matches reports whether an event on an object reaches this configuration.
func (n Notification) Matches(eventType, object string) bool {
	return (len(n.EventTypes) == 0 || slices.Contains(n.EventTypes, eventType)) && strings.HasPrefix(object, n.ObjectNamePrefix)
}
