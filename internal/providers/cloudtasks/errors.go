package cloudtasks

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// statusNotFound reports a 404 for one resource type.
func statusNotFound(message string) error {
	return status.Error(codes.NotFound, message)
}

// statusInvalid reports a 400 for one malformed field.
func statusInvalid(message string) error {
	return status.Error(codes.InvalidArgument, message)
}

// gap reports an unimplemented slice boundary.
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Cloud Tasks coverage gap: "+message)
}

// statusExists reports a 409 for a duplicate queue or task.
func statusExists(message string) error {
	return status.Error(codes.AlreadyExists, message)
}
