package vertexai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamWriter frames streamGenerateContent chunks as server-sent events or
// a JSON array. It holds back one chunk so the final chunk can carry the
// finish reason and usage, as Vertex AI does.
type streamWriter struct {
	p       *Provider
	writer  *io.PipeWriter
	started chan error
	sse     bool
	version string
	id      string
	first   bool
	wrote   bool
	held    []Part
}

// frame writes one encoded value with the stream's separators.
func (s *streamWriter) frame(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return status.Error(codes.Internal, "Response encoding failed")
	}
	var frame []byte
	switch {
	case s.sse:
		frame = append(append([]byte("data: "), encoded...), "\r\n\r\n"...)
	case !s.wrote:
		frame = append([]byte("["), encoded...)
	default:
		frame = append([]byte(",\r\n"), encoded...)
	}
	s.wrote = true
	if _, err := s.writer.Write(frame); err != nil {
		return status.Error(codes.Canceled, "The client closed the stream")
	}
	return nil
}

// signal releases the waiting handler once, with the first outcome.
func (s *streamWriter) signal(err error) {
	if s.first {
		s.first = false
		s.started <- err
	}
}

// emit receives partial parts from the backend.
func (s *streamWriter) emit(parts []Part) error {
	s.signal(nil)
	if s.held != nil {
		if err := s.frame(s.p.render(s.held, nil, s.version, s.id)); err != nil {
			return err
		}
	}
	s.held = parts
	return nil
}

// close ends the array framing and the pipe.
func (s *streamWriter) close() {
	if !s.sse && s.wrote {
		_, _ = s.writer.Write([]byte("]"))
	}
	_ = s.writer.Close()
}

// finish writes the held chunk as the final one, or an in-stream error.
func (s *streamWriter) finish(result Result, err error) {
	defer s.close()
	if err != nil {
		if s.first {
			s.signal(backendError(err))
			return
		}
		_ = s.frame(streamError(err))
		return
	}
	s.signal(nil)
	if s.held == nil {
		s.held = []Part{}
	}
	_ = s.frame(s.p.render(s.held, &result, s.version, s.id))
}

// stream answers streamGenerateContent as server-sent events (alt=sse) or a
// JSON array. A backend failure before the first chunk is an ordinary error;
// a later one ends the stream with an error object.
func (p *Provider) stream(ctx context.Context, sse bool, t target, version string, body *GenerateRequest) (*gateway.RESTResponse, error) {
	reader, writer := io.Pipe()
	s := &streamWriter{p: p, writer: writer, started: make(chan error, 1), sse: sse, version: version, id: newResponseID(), first: true}
	go func() {
		result, err := p.model.Generate(ctx, t.model, body, s.emit)
		s.finish(result, err)
	}()
	if err := <-s.started; err != nil {
		return nil, err
	}
	headers := http.Header{}
	if sse {
		headers.Set("Content-Type", "text/event-stream")
	} else {
		headers.Set("Content-Type", "application/json; charset=UTF-8")
	}
	return &gateway.RESTResponse{Headers: headers, Body: reader}, nil
}

// streamError renders a failure after the stream has started, in the REST
// error shape.
func streamError(err error) map[string]any {
	converted := status.Convert(err)
	code, name := 500, "INTERNAL"
	switch converted.Code() {
	case codes.Unavailable:
		code, name = 503, "UNAVAILABLE"
	case codes.FailedPrecondition:
		code, name = 400, "FAILED_PRECONDITION"
	case codes.Canceled:
		code, name = 499, "CANCELLED"
	case codes.DeadlineExceeded:
		code, name = 504, "DEADLINE_EXCEEDED"
	default:
	}
	return map[string]any{"error": map[string]any{"code": code, "message": converted.Message(), "status": name}}
}
