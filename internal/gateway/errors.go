package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

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
	if _, ok := status.FromError(err); ok {
		return err
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

func writeError(w http.ResponseWriter, err error) {
	s := status.Convert(normalizeError(err))
	code := http.StatusInternalServerError
	switch s.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		code = 400
	case codes.Unauthenticated:
		code = 401
	case codes.PermissionDenied:
		code = 403
	case codes.NotFound:
		code = 404
	case codes.AlreadyExists, codes.Aborted:
		code = 409
	case codes.ResourceExhausted:
		code = 429
	case codes.Canceled:
		code = 499
	case codes.Unimplemented:
		code = 501
	case codes.Unavailable:
		code = 503
	case codes.DeadlineExceeded:
		code = 504
	default:
		// Every other status stays 500; the wire status name still describes it.
	}
	names := map[codes.Code]string{codes.Canceled: "CANCELLED", codes.InvalidArgument: "INVALID_ARGUMENT", codes.DeadlineExceeded: "DEADLINE_EXCEEDED", codes.NotFound: "NOT_FOUND", codes.AlreadyExists: "ALREADY_EXISTS", codes.PermissionDenied: "PERMISSION_DENIED", codes.ResourceExhausted: "RESOURCE_EXHAUSTED", codes.FailedPrecondition: "FAILED_PRECONDITION", codes.OutOfRange: "OUT_OF_RANGE", codes.DataLoss: "DATA_LOSS"}
	name := names[s.Code()]
	if name == "" {
		name = strings.ToUpper(s.Code().String())
	}
	body := map[string]any{"code": code, "status": name, "message": s.Message()}
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
