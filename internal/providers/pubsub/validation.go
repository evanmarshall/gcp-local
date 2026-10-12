package pubsub

import (
	"regexp"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

var namePattern = regexp.MustCompile(`^projects/([a-z][a-z0-9-]{4,61}[a-z0-9])/(topics|subscriptions|snapshots|schemas)/([A-Za-z][A-Za-z0-9\-_.~+%]{2,254})$`)

var gap = provider.CoverageGap("Pub/Sub")

func canonical(name string) string { return "//pubsub.googleapis.com/" + name }

func resourceName(name, kind string) (string, error) {
	parts := namePattern.FindStringSubmatch(name)
	if len(parts) != 4 || (kind != "" && parts[2] != kind) || strings.HasPrefix(parts[3], "goog") {
		return "", provider.Invalid("Invalid Pub/Sub resource name")
	}
	return parts[1], nil
}

// Refuse every populated field outside the tested slice, including fields
// added by newer clients. Upstream silently accepts several unsupported ones.
func fields(message proto.Message, allowed ...string) error {
	if len(message.ProtoReflect().GetUnknown()) > 0 {
		return gap("unknown protobuf fields")
	}
	accept := map[protoreflect.Name]bool{}
	for _, name := range allowed {
		accept[protoreflect.Name(name)] = true
	}
	var err error
	message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if !accept[field.Name()] {
			err = gap("field " + string(field.Name()))
			return false
		}
		return true
	})
	return err
}

func validate(request proto.Message) error {
	switch r := request.(type) {
	case *pb.Topic:
		if err := fields(r, "name", "labels", "schema_settings"); err != nil {
			return err
		}
		return validateSchemaSettings(r.GetSchemaSettings())
	case *pb.Subscription:
		return validateSubscription(r)
	case *pb.PublishRequest:
		return validatePublish(r)
	case *pb.PullRequest:
		return validatePull(r)
	case *pb.AcknowledgeRequest:
		return fields(r, "subscription", "ack_ids")
	case *pb.ModifyAckDeadlineRequest:
		return validateModifyAckDeadline(r)
	default:
		return validateOther(request)
	}
}

// validateOther checks snapshot and seek requests and refuses unknown fields
// on every other request.
func validateOther(request proto.Message) error {
	switch r := request.(type) {
	case *pb.CreateSnapshotRequest:
		return validateCreateSnapshot(r)
	case *pb.SeekRequest:
		return validateSeek(r)
	}
	if len(request.ProtoReflect().GetUnknown()) > 0 {
		return gap("unknown protobuf fields")
	}
	return nil
}

// validatePull bounds a pull to one thousand messages.
func validatePull(r *pb.PullRequest) error {
	if err := fields(r, "subscription", "max_messages", "return_immediately"); err != nil {
		return err
	}
	if r.GetMaxMessages() < 1 || r.GetMaxMessages() > 1000 {
		return provider.Invalid("Max messages must be one through 1000")
	}
	return nil
}

// validateModifyAckDeadline bounds the ack deadline the client resets.
func validateModifyAckDeadline(r *pb.ModifyAckDeadlineRequest) error {
	if err := fields(r, "subscription", "ack_ids", "ack_deadline_seconds"); err != nil {
		return err
	}
	if r.GetAckDeadlineSeconds() < 0 || r.GetAckDeadlineSeconds() > 600 {
		return provider.Invalid("Ack deadline must be zero through 600 seconds")
	}
	return nil
}

// validateSubscription checks the subscription fields against the
// Subscription reference (google/pubsub/v1/pubsub.proto). Fields the provider
// or the emulator implements are accepted; every other populated field is a
// coverage gap.
func validateSubscription(r *pb.Subscription) error {
	if err := fields(r, "name", "topic", "labels", "ack_deadline_seconds", "message_retention_duration", "retain_acked_messages", "attributes", "push_config",
		"dead_letter_policy", "retry_policy", "enable_message_ordering", "filter", "enable_exactly_once_delivery", "expiration_policy"); err != nil {
		return err
	}
	if r.GetPushConfig() != nil {
		if err := validatePush(r.GetPushConfig()); err != nil {
			return err
		}
		if r.GetEnableExactlyOnceDelivery() {
			return gap("exactly-once delivery on push subscriptions")
		}
	}
	if _, err := resourceName(r.GetTopic(), "topics"); err != nil {
		return err
	}
	if deadline := r.GetAckDeadlineSeconds(); deadline != 0 && (deadline < 10 || deadline > 600) {
		return provider.Invalid("Ack deadline must be 10 through 600 seconds")
	}
	return validateDelivery(r)
}

// validateDelivery checks the delivery-policy fields of a subscription.
func validateDelivery(r *pb.Subscription) error {
	if policy := r.GetDeadLetterPolicy(); policy != nil {
		if err := fields(policy, "dead_letter_topic", "max_delivery_attempts"); err != nil {
			return err
		}
	}
	if retry := r.GetRetryPolicy(); retry != nil {
		if err := validateRetry(retry); err != nil {
			return err
		}
	}
	// The emulator never expires subscriptions, which is what an empty TTL means.
	if expiration := r.GetExpirationPolicy(); expiration != nil && expiration.GetTtl() != nil {
		return gap("subscription expiration TTL")
	}
	// Google's filtering guide limits a filter to 256 bytes.
	if len(r.GetFilter()) > 256 {
		return provider.Invalid("Filter must be at most 256 bytes")
	}
	return nil
}

// validateRetry bounds both backoffs to 0 through 600 seconds (RetryPolicy reference).
func validateRetry(retry *pb.RetryPolicy) error {
	if err := fields(retry, "minimum_backoff", "maximum_backoff"); err != nil {
		return err
	}
	for _, value := range []*durationpb.Duration{retry.GetMinimumBackoff(), retry.GetMaximumBackoff()} {
		if value != nil && (value.AsDuration() < 0 || value.AsDuration() > 600*time.Second) {
			return provider.Invalid("Retry backoff must be 0 through 600 seconds")
		}
	}
	if retry.GetMinimumBackoff() != nil && retry.GetMaximumBackoff() != nil && retry.GetMinimumBackoff().AsDuration() > retry.GetMaximumBackoff().AsDuration() {
		return provider.Invalid("Minimum backoff must not exceed maximum backoff")
	}
	return nil
}

// validateCreateSnapshot checks a snapshot's source and labels.
func validateCreateSnapshot(r *pb.CreateSnapshotRequest) error {
	if err := fields(r, "name", "subscription", "labels"); err != nil {
		return err
	}
	_, err := resourceName(r.GetSubscription(), "subscriptions")
	return err
}

// validateSeek requires exactly one seek target (SeekRequest reference).
func validateSeek(r *pb.SeekRequest) error {
	if err := fields(r, "subscription", "time", "snapshot"); err != nil {
		return err
	}
	if r.GetTarget() == nil {
		return provider.Invalid("Seek requires a time or a snapshot")
	}
	if r.GetTime() == nil {
		if _, err := resourceName(r.GetSnapshot(), "snapshots"); err != nil {
			return err
		}
	}
	return nil
}

// validatePublish bounds a publication to one thousand non-empty messages and
// one MiB in total.
func validatePublish(r *pb.PublishRequest) error {
	if err := fields(r, "topic", "messages"); err != nil {
		return err
	}
	if len(r.GetMessages()) == 0 || len(r.GetMessages()) > 1000 {
		return provider.Invalid("Supply one through 1000 messages")
	}
	for _, m := range r.GetMessages() {
		if m == nil {
			return provider.Invalid("Missing message")
		}
		if err := fields(m, "data", "attributes", "ordering_key"); err != nil {
			return err
		}
		if len(m.GetData()) == 0 && len(m.GetAttributes()) == 0 {
			return provider.Invalid("Messages need data or attributes")
		}
	}
	if proto.Size(r) > 1<<20 {
		return status.Error(codes.ResourceExhausted, "Local publication limit is 1 MiB")
	}
	return nil
}
