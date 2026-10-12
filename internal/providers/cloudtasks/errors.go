package cloudtasks

import (
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Cloud Tasks")

// statusExists reports a 409 for a duplicate queue or task.
func statusExists(message string) error {
	return status.Error(codes.AlreadyExists, message)
}
