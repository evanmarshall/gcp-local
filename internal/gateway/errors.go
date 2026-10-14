package gateway

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func normalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	// A provider may wrap a status for its own context; clients get the
	// innermost status, not the internal wrapping text.
	var wrapped interface{ GRPCStatus() *status.Status }
	if errors.As(err, &wrapped) {
		return wrapped.GRPCStatus().Err()
	}
	// Unexpected backend failures can contain credentials or resource payloads.
	return status.Error(codes.Internal, "Internal provider error")
}

// encodeErrorBody renders the wire error body. It never fails for the
// JSON-compatible values passed here, and falls back to a static body rather
// than writing a partial response.
func encodeErrorBody(body map[string]any) []byte {
	encoded, err := json.Marshal(map[string]any{"error": body})
	if err != nil {
		return []byte(`{"error":{"code":500,"status":"INTERNAL","message":"encode failed"}}`)
	}
	return encoded
}

// statusHTTP maps a wire status to the HTTP code a REST client expects.
var statusHTTP = map[codes.Code]int{
	codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400,
	codes.Unauthenticated: 401, codes.PermissionDenied: 403, codes.NotFound: 404,
	codes.AlreadyExists: 409, codes.Aborted: 409, codes.ResourceExhausted: 429,
	codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503, codes.DeadlineExceeded: 504,
}

// statusNames renames the wire statuses whose Google spelling differs.
var statusNames = map[codes.Code]string{
	codes.Canceled: "CANCELLED", codes.InvalidArgument: "INVALID_ARGUMENT",
	codes.DeadlineExceeded: "DEADLINE_EXCEEDED", codes.NotFound: "NOT_FOUND",
	codes.AlreadyExists: "ALREADY_EXISTS", codes.PermissionDenied: "PERMISSION_DENIED",
	codes.ResourceExhausted: "RESOURCE_EXHAUSTED", codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.OutOfRange: "OUT_OF_RANGE", codes.DataLoss: "DATA_LOSS",
}

func writeError(w http.ResponseWriter, err error) {
	code, body := statusBody(status.Convert(normalizeError(err)))
	writeJSONError(w, code, body)
}

// statusBody is the google.rpc.Status JSON body for a status, with the HTTP
// code a REST client expects.
func statusBody(s *status.Status) (int, map[string]any) {
	code := http.StatusInternalServerError // Every unmapped status stays 500.
	if mapped, ok := statusHTTP[s.Code()]; ok {
		code = mapped
	}
	body := map[string]any{"code": code, "status": statusName(s.Code()), keyMessage: s.Message()}
	if len(s.Proto().GetDetails()) > 0 {
		var details []json.RawMessage
		for _, detail := range s.Proto().GetDetails() {
			encoded, e := protojson.Marshal(detail)
			if e == nil {
				details = append(details, encoded)
			}
		}
		if len(details) > 0 {
			body["details"] = details
		}
	}
	return code, body
}

func writeJSONError(w http.ResponseWriter, code int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(encodeErrorBody(body)); err != nil {
		// The status is already committed; the client sees a truncated body.
		return
	}
}

const (
	keyMessage    = "message"
	reasonInvalid = "invalid"
	// global is the default location and the errors[] domain.
	global = "global"
)

// maxJSON caps a REST JSON body. GCP's caps are per API and larger (Secret
// Manager 1 MiB, Pub/Sub 10 MiB, BigQuery about 10 MiB; probed 2026-10-13).
const maxJSON = 2 << 20

// errPayloadTooLarge refuses a JSON body over maxJSON in the wording Google's
// front end uses for an API's own payload limit (Secret Manager and Pub/Sub,
// probed 2026-10-13). It is a 400, not a 429 that clients retry.
var errPayloadTooLarge = status.Errorf(codes.InvalidArgument, "Request payload size exceeds the limit: %d bytes.", maxJSON)

// writeRouteError renders a refusal of a matched REST route before dispatch
// (credentials, path, query, or body) in the route's API family's shape, as
// probed on GCP 2026-10-13: the Storage XML document, the Storage JSON
// errors[] body, the v1/v2 JSON APIs' body with both errors[] and status
// (BigQuery, Compute, Cloud SQL), or google.rpc.Status for the rest.
func writeRouteError(w http.ResponseWriter, method string, err error) {
	s := status.Convert(normalizeError(err))
	family, _, _ := strings.Cut(method, ".")
	switch {
	case s.Code() == codes.Canceled || s.Code() == codes.DeadlineExceeded:
		writeError(w, s.Err())
	case strings.HasPrefix(method, "storage.xml."):
		writeStorageXMLError(w, s)
	case family == serviceStorage:
		code, reason, message := storageRefusal(s)
		writeJSONError(w, code, map[string]any{"code": code, keyMessage: message,
			"errors": []any{errorEntry(reason, message, s.Code() == codes.Unauthenticated)}})
	default:
		writeAPIError(w, family, s)
	}
}

// writeAPIError renders google.rpc.Status, plus errors[] on BigQuery,
// Compute, and Cloud SQL. A 401 carries ErrorInfo CREDENTIALS_MISSING naming
// the API host, as GCP's does.
func writeAPIError(w http.ResponseWriter, family string, s *status.Status) {
	unauthenticated := s.Code() == codes.Unauthenticated
	if unauthenticated && len(s.Proto().GetDetails()) == 0 {
		s = credentialsMissing(s, family)
	}
	code, body := statusBody(s)
	if family == "bigquery" || family == "compute" || family == "sql" {
		reason, message := legacyReason(s.Code()), s.Message()
		if unauthenticated {
			message = "Invalid Credentials"
		}
		body["errors"] = []any{errorEntry(reason, message, unauthenticated)}
	}
	writeJSONError(w, code, body)
}

// errorEntry is one errors[] entry; a credential refusal names the header.
func errorEntry(reason, message string, header bool) map[string]any {
	entry := map[string]any{"domain": global, "reason": reason, keyMessage: message}
	if header {
		entry["locationType"], entry["location"] = "header", "Authorization"
	}
	return entry
}

// storageRefusal maps a gateway refusal to the Storage JSON API's code,
// reason, and message. Nothing is 5xx: the Go client retries every 5xx
// without a limit.
func storageRefusal(s *status.Status) (int, string, string) {
	switch {
	case s.Code() == codes.Unauthenticated:
		return http.StatusUnauthorized, "authError", "Invalid Credentials"
	case s.Message() == status.Convert(errPayloadTooLarge).Message():
		return http.StatusRequestEntityTooLarge, "uploadTooLarge", "Request Too Large"
	case s.Code() == codes.InvalidArgument:
		return http.StatusBadRequest, reasonInvalid, s.Message()
	default:
		return http.StatusBadRequest, reasonInvalid, "Storage emulator fault: " + s.Message()
	}
}

// writeStorageXMLError renders the XML API's error document. GCP answers an
// unusable credential with 401 AuthenticationRequired.
func writeStorageXMLError(w http.ResponseWriter, s *status.Status) {
	code, kind, message := http.StatusBadRequest, "InvalidArgument", s.Message()
	if s.Code() == codes.Unauthenticated {
		code, kind, message = http.StatusUnauthorized, "AuthenticationRequired", "Authentication required."
	}
	encoded, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: kind, Message: message})
	if err != nil {
		encoded = nil
	}
	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(code)
	_, _ = w.Write(append([]byte(xml.Header), encoded...))
}

// legacyReason is the errors[] reason of a gateway refusal on BigQuery,
// Compute, or Cloud SQL.
func legacyReason(code codes.Code) string {
	switch code {
	case codes.Unauthenticated:
		return "authError"
	case codes.InvalidArgument:
		return reasonInvalid
	default:
		return "backendError"
	}
}

// apiHosts names the API hosts whose discovery prefix differs.
var apiHosts = map[string]string{"sql": "sqladmin", "bigtable": "bigtableadmin", "resourcemanager": "cloudresourcemanager"}

// credentialsMissing adds the ErrorInfo GCP attaches to a 401. It names the
// API host only; GCP also names the RPC, which a REST route doesn't know.
func credentialsMissing(s *status.Status, family string) *status.Status {
	host := family
	if mapped, ok := apiHosts[family]; ok {
		host = mapped
	}
	detailed, err := s.WithDetails(&errdetails.ErrorInfo{Reason: "CREDENTIALS_MISSING", Domain: "googleapis.com",
		Metadata: map[string]string{"service": host + ".googleapis.com"}})
	if err != nil {
		return s
	}
	return detailed
}
