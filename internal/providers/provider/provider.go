// Package provider holds the wire, naming, and IAM helpers that several
// service providers share verbatim. A helper belongs here only when every
// caller renders byte-identical output; a service whose recorded GCP behavior
// differs keeps its own variant.
package provider

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WireError renders the Google API error body {"error":{code,status,message}}.
func WireError(code int, statusText, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		"error": map[string]any{"code": code, "status": statusText, "message": message},
	}}
}

// DeniedPermission names the first failed check, or "unspecified".
func DeniedPermission(decision iam.Decision) string {
	for _, check := range decision.Checks {
		if !check.Allowed {
			return check.Permission
		}
	}
	return "unspecified"
}

// Denied renders the 403 body that names "The caller" rather than the principal.
func Denied(decision iam.Decision, resource string) *gateway.RESTResponse {
	return WireError(403, "PERMISSION_DENIED", fmt.Sprintf("The caller does not have %s access to %s.", DeniedPermission(decision), resource))
}

// RegisterPermissions binds each method to its single baseline permission.
func RegisterPermissions(engine *iam.Engine, permissions map[string]string) error {
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// NotFound reports a missing resource in the "kind/name" form.
func NotFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// Invalid rejects a request with INVALID_ARGUMENT and the given message.
func Invalid(message string) error {
	return status.Error(codes.InvalidArgument, message)
}

// ProjectResource is the IAM target name of a project.
func ProjectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}

// ProjectScope is the state scope of a project's global resources.
func ProjectScope(project string) state.Scope {
	return state.Scope{Project: project, Location: state.Global}
}

// Stamp renders a timestamp with the microsecond precision most APIs return.
func Stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000000Z") }

// LastSegment returns the final path segment of a resource name.
func LastSegment(name string) string {
	if index := strings.LastIndex(name, "/"); index >= 0 {
		return name[index+1:]
	}
	return name
}

// Text returns value when it is a string and "" otherwise.
func Text(value any) string {
	if value, ok := value.(string); ok {
		return value
	}
	return ""
}

// MapList keeps the object entries of a decoded JSON array.
func MapList(value any) []map[string]any {
	entries, _ := value.([]any)
	result := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if object, ok := entry.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

// DecodeObject decodes a JSON object request body.
func DecodeObject(body []byte) (map[string]any, error) {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid request body")
	}
	return value, nil
}
