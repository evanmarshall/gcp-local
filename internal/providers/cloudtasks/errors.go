package cloudtasks

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gap reports an unimplemented slice boundary.
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Cloud Tasks coverage gap: "+message)
}

// statusExists reports a 409 for a duplicate queue or task.
func statusExists(message string) error {
	return status.Error(codes.AlreadyExists, message)
}
