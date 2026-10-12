package provider

import (
	"time"

	"google.golang.org/grpc/codes"
)

// httpStatusCodes is the documented HTTP-to-RPC mapping Cloud Tasks and Cloud
// Scheduler apply to an HTTP target's response.
var httpStatusCodes = map[int]codes.Code{
	400: codes.InvalidArgument, 401: codes.Unauthenticated, 403: codes.PermissionDenied,
	404: codes.NotFound, 409: codes.Aborted, 416: codes.OutOfRange, 429: codes.ResourceExhausted,
	499: codes.Canceled, 501: codes.Unimplemented, 503: codes.Unavailable, 504: codes.DeadlineExceeded,
}

// HTTPCode maps a non-2xx HTTP target status to its RPC code.
func HTTPCode(statusCode int) codes.Code {
	if code, ok := httpStatusCodes[statusCode]; ok {
		return code
	}
	if statusCode >= 400 && statusCode < 500 {
		return codes.FailedPrecondition
	}
	return codes.Internal
}

// Backoff is the retry interval after the given number of failed attempts,
// as Cloud Tasks and Cloud Scheduler document it: minimum doubles
// maxDoublings times, then grows linearly, capped at maximum.
func Backoff(minimum, maximum time.Duration, maxDoublings, failures int) time.Duration {
	interval := minimum
	step := failures - 1
	for i := 0; i < step && i < maxDoublings && interval < maximum; i++ {
		interval *= 2
	}
	if linear := step - maxDoublings; linear > 0 && interval > 0 && interval < maximum {
		if int64(linear) >= int64(maximum/interval) {
			return maximum
		}
		interval += interval * time.Duration(linear)
	}
	return min(interval, maximum)
}
