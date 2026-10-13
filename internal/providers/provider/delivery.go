package provider

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
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

// internalHeader carries a dispatcher's proof that a request comes from inside
// the emulator. Cloud Run's ingress counts Pub/Sub push, Cloud Tasks, and
// Cloud Scheduler deliveries as internal for services in their own project
// (https://cloud.google.com/run/docs/securing/ingress).
const internalHeader = "X-Gcp-Local-Internal"

// internalPath is the local Cloud Run invocation prefix; only requests to it
// carry the proof, so other targets never see it.
const internalPath = "/_run/"

// internalKey signs the proof. It lives only in this process, so a caller
// outside it can't make one.
var internalKey = func() []byte {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return key
}()

// internalMAC binds a proof to one project, method, host, and path, so a
// proof seen by one target can't be replayed to another.
func internalMAC(project, method, host, path string) string {
	mac := hmac.New(sha256.New, internalKey)
	mac.Write([]byte(project + "\x00" + method + "\x00" + host + "\x00" + path))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// MarkInternal marks a dispatcher's request to a local Cloud Run service as
// internal traffic from project.
func MarkInternal(request *http.Request, project string) {
	if !strings.HasPrefix(request.URL.Path, internalPath) {
		return
	}
	host := request.Host
	if host == "" {
		host = request.URL.Host
	}
	request.Header.Set(internalHeader, project+"."+internalMAC(project, request.Method, host, request.URL.Path))
}

// InternalProject returns the project a request was marked internal for by
// MarkInternal in this process.
func InternalProject(request *http.Request) (string, bool) {
	// A domain-scoped project ID can hold a dot; the MAC can't.
	proof := request.Header.Get(internalHeader)
	at := strings.LastIndex(proof, ".")
	if at < 0 {
		return "", false
	}
	project, mac := proof[:at], proof[at+1:]
	if !hmac.Equal([]byte(mac), []byte(internalMAC(project, request.Method, request.Host, request.URL.Path))) {
		return "", false
	}
	return project, true
}
