package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

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
	s := status.Convert(normalizeError(err))
	code := http.StatusInternalServerError // Every unmapped status stays 500.
	if mapped, ok := statusHTTP[s.Code()]; ok {
		code = mapped
	}
	body := map[string]any{"code": code, "status": statusName(s.Code()), "message": s.Message()}
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(encodeErrorBody(body)); err != nil {
		// The status is already committed; the client sees a truncated body.
		return
	}
}
