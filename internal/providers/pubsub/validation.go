package pubsub

import (
	"regexp"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var namePattern = regexp.MustCompile(`^projects/([a-z][a-z0-9-]{4,61}[a-z0-9])/(topics|subscriptions)/([A-Za-z][A-Za-z0-9\-_.~+%]{2,254})$`)

func gap(message string) error {
	return status.Error(codes.Unimplemented, "Pub/Sub coverage gap: "+message)
}
func invalid(message string) error { return status.Error(codes.InvalidArgument, message) }
func canonical(name string) string { return "//pubsub.googleapis.com/" + name }
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

func resourceName(name, kind string) (string, error) {
	parts := namePattern.FindStringSubmatch(name)
	if len(parts) != 4 || (kind != "" && parts[2] != kind) || strings.HasPrefix(parts[3], "goog") {
		return "", invalid("Invalid Pub/Sub resource name")
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
		return fields(r, "name", "labels")
	case *pb.Subscription:
		if err := fields(r, "name", "topic", "labels", "ack_deadline_seconds", "push_config"); err != nil {
			return err
		}
		if r.PushConfig != nil {
			if err := validatePush(r.PushConfig); err != nil {
				return err
			}
		}
		if _, err := resourceName(r.Topic, "topics"); err != nil {
			return err
		}
		if r.AckDeadlineSeconds != 0 && (r.AckDeadlineSeconds < 10 || r.AckDeadlineSeconds > 600) {
			return invalid("Ack deadline must be 10 through 600 seconds")
		}
	case *pb.PublishRequest:
		if err := fields(r, "topic", "messages"); err != nil {
			return err
		}
		if len(r.Messages) == 0 || len(r.Messages) > 1000 {
			return invalid("Supply one through 1000 messages")
		}
		for _, m := range r.Messages {
			if m == nil {
				return invalid("Missing message")
			}
			if err := fields(m, "data", "attributes"); err != nil {
				return err
			}
			if len(m.Data) == 0 && len(m.Attributes) == 0 {
				return invalid("Messages need data or attributes")
			}
		}
		if proto.Size(r) > 1<<20 {
			return status.Error(codes.ResourceExhausted, "Local publication limit is 1 MiB")
		}
	case *pb.PullRequest:
		if err := fields(r, "subscription", "max_messages", "return_immediately"); err != nil {
			return err
		}
		if r.MaxMessages < 1 || r.MaxMessages > 1000 {
			return invalid("Max messages must be one through 1000")
		}
	case *pb.AcknowledgeRequest:
		if err := fields(r, "subscription", "ack_ids"); err != nil {
			return err
		}
	case *pb.ModifyAckDeadlineRequest:
		if err := fields(r, "subscription", "ack_ids", "ack_deadline_seconds"); err != nil {
			return err
		}
		if r.AckDeadlineSeconds < 0 || r.AckDeadlineSeconds > 600 {
			return invalid("Ack deadline must be zero through 600 seconds")
		}
	default:
		if len(request.ProtoReflect().GetUnknown()) > 0 {
			return gap("unknown protobuf fields")
		}
	}
	return nil
}
