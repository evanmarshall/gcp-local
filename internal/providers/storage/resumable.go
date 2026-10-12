package storage

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // GCS reports and checks MD5 content hashes
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// uploadSession is one resumable upload. The provider owns the received bytes
// until the final chunk commits them to the backend. As in GCP, the session URI
// is the credential for the chunks; the commit is reauthorized as the principal
// that started the session, against the object as it exists at commit time.
type uploadSession struct {
	mu        sync.Mutex
	bucket    string
	upload    *uploadRequest
	principal gateway.Principal
	data      []byte
	// result is the committed object, returned again to a repeated final request.
	result any
	closed bool
	// cancelled marks a session ended by DELETE. It stays in the catalog so
	// later requests get GCP's 499, not the 404 of an unknown session.
	cancelled bool
}

const uploadIDHeader = "X-GUploader-UploadID"

// startUpload records an authorized resumable initiation and returns its
// session URI. Preconditions are checked now and again at commit.
func (p *Provider) startUpload(req request, current map[string]any) (*gateway.RESTResponse, error) {
	if response, err := preconditions(req.rest, current, MethodObjectsInsert); response != nil || err != nil {
		return response, err
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return nil, status.Error(codes.Internal, "Unable to create an upload ID")
	}
	id := base64.RawURLEncoding.EncodeToString(raw)
	session := &uploadSession{bucket: req.bucket, upload: req.upload, principal: req.call.Principal}
	if err := p.catalog.publishUpload(req.scope, id, session); err != nil {
		return nil, fmt.Errorf("publish the upload session: %w", err)
	}
	query := url.Values{queryUploadType: {uploadResumable}, keyName: {req.upload.name}, "upload_id": {id}}
	location := "http://" + req.rest.Host + "/upload/storage/v1/b/" + url.PathEscape(req.bucket) + "/o?" + query.Encode()
	return &gateway.RESTResponse{Headers: http.Header{"Location": {location}, uploadIDHeader: {id}}}, nil
}

// sessionQuery lists the options a session URI carries.
var sessionQuery = map[string]bool{queryUploadType: true, keyName: true, "upload_id": true}

// continueUpload serves a PUT (a chunk or a status query) or a DELETE (cancel)
// on a session URI. Possession of the upload ID authorizes these requests.
func (p *Provider) continueUpload(call gateway.Call, r *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := validateSessionQuery(r.Query); err != nil {
		return nil, err
	}
	bucket, id := r.Path[keyBucket], r.Query.Get("upload_id")
	resource, found := p.index.Get(BucketResource(bucket))
	if !found {
		return notFound(bucket, ""), nil
	}
	var response *gateway.RESTResponse
	err := p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: resource.Scope}, func(ctx context.Context, driver backend.Backend) error {
		session := p.catalog.upload(resource.Scope, bucket, id)
		if session == nil {
			response = uploadMissing()
			return nil
		}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.cancelled {
			response = uploadCancelled()
			return nil
		}
		if session.closed {
			response = uploadMissing()
			return nil
		}
		call.Context = ctx //nolint:fatcontext // one request-scoped handoff of the manager session
		var serveErr error
		response, serveErr = p.applyUpload(call, driver, r, resource.Scope, id, session)
		return serveErr
	})
	if err != nil {
		return response, fmt.Errorf("continue the upload: %w", err)
	}
	return response, nil
}

// validateSessionQuery accepts only a session URI's own options. As on every
// JSON API method, an option discovery does not define for objects.insert is
// ignored.
func validateSessionQuery(query url.Values) error {
	for key, values := range query {
		if !sessionQuery[key] && !definedOption(MethodObjectsInsert, key) {
			continue
		}
		if !sessionQuery[key] && !slices.Contains(universalQueryOptions, key) {
			return gap("unsupported query option " + key)
		}
		if len(values) != 1 {
			return status.Error(codes.InvalidArgument, "Query options must not repeat")
		}
	}
	if query.Get("upload_id") == "" || query.Get(queryUploadType) != uploadResumable {
		return status.Error(codes.InvalidArgument, "Continue a resumable upload at its session URI")
	}
	return nil
}

func uploadMissing() *gateway.RESTResponse {
	return wireError(404, "notFound", "No such upload session")
}

// uploadCancelled answers a cancel and every later request to that session
// with GCP's 499 Client Closed Request (recorded: storage-resumable-signed-url
// "cancel" and "after-cancel").
func uploadCancelled() *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: 499}
}

// applyUpload cancels, reports, extends, or commits one session.
func (p *Provider) applyUpload(call gateway.Call, driver backend.Backend, r *gateway.RESTRequest, scope state.Scope, id string, session *uploadSession) (*gateway.RESTResponse, error) {
	if r.Verb == http.MethodDelete {
		session.closed, session.cancelled, session.data = true, true, nil
		return uploadCancelled(), nil
	}
	if session.result != nil {
		return &gateway.RESTResponse{JSON: session.result}, nil
	}
	chunk, err := readChunk(r.Media)
	if err != nil {
		return nil, err
	}
	span, err := parseContentRange(r.Headers.Get("Content-Range"), int64(len(chunk)))
	if err != nil {
		return nil, err
	}
	if err := session.apply(span, chunk); err != nil {
		return nil, err
	}
	if total := session.upload.total; total < 0 || int64(len(session.data)) < total {
		return incompleteUpload(session, r.Headers.Get("X-Guploader-No-308") == "yes"), nil
	}
	return p.commitUpload(call, driver, r, scope, id, session)
}

// readChunk reads one request body, at most one object.
func readChunk(media io.Reader) ([]byte, error) {
	if media == nil {
		return nil, nil
	}
	chunk, err := io.ReadAll(io.LimitReader(media, maxObjectSize+1))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Cannot read upload")
	}
	if len(chunk) > maxObjectSize {
		return nil, errUploadTooLarge
	}
	return chunk, nil
}

// contentRange is one parsed Content-Range header. total is -1 for "*";
// data is false for a status query ("bytes */N").
type contentRange struct {
	start, total int64
	data         bool
}

var errContentRange = status.Error(codes.InvalidArgument, "Content-Range must be bytes START-END/TOTAL, bytes START-END/*, or bytes */TOTAL, and match the body")

// parseContentRange parses a chunk's Content-Range. Without one, the body is
// the whole object.
func parseContentRange(header string, size int64) (contentRange, error) {
	if header == "" {
		return contentRange{start: 0, total: size, data: true}, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes ")
	span, totalText, hasTotal := strings.Cut(spec, "/")
	if !ok || !hasTotal {
		return contentRange{}, errContentRange
	}
	total, err := parseTotal(totalText)
	if err != nil {
		return contentRange{}, err
	}
	parsed := contentRange{total: total}
	if span == "*" {
		if size != 0 {
			return contentRange{}, errContentRange
		}
		return parsed, nil
	}
	start, err := parseSpan(span, size, parsed.total)
	parsed.start, parsed.data = start, true
	return parsed, err
}

// parseSpan parses START-END, which must cover exactly the body and end
// before a known total.
func parseSpan(span string, size, total int64) (int64, error) {
	startText, endText, ok := strings.Cut(span, "-")
	start, startErr := strconv.ParseInt(startText, 10, 64)
	end, endErr := strconv.ParseInt(endText, 10, 64)
	if !ok || startErr != nil || endErr != nil || start < 0 || end < start || end-start+1 != size || (total >= 0 && end >= total) {
		return 0, errContentRange
	}
	return start, nil
}

// parseTotal parses a Content-Range total; "*" is unknown (-1).
func parseTotal(text string) (int64, error) {
	if text == "*" {
		return -1, nil
	}
	total, err := strconv.ParseInt(text, 10, 64)
	if err != nil || total < 0 {
		return 0, errContentRange
	}
	return total, nil
}

// apply records a declared total and appends a chunk's new bytes. Bytes that
// repeat already received ones (a client retry) are skipped.
func (s *uploadSession) apply(span contentRange, chunk []byte) error {
	if err := s.declare(span.total); err != nil || !span.data {
		return err
	}
	received := int64(len(s.data))
	if span.start > received {
		return status.Errorf(codes.InvalidArgument, "The chunk starts at byte %d, past the %d bytes received", span.start, received)
	}
	skip := received - span.start
	if skip >= int64(len(chunk)) {
		return nil
	}
	// Check the new length before appending, so a refused chunk leaves the
	// session as it was.
	length := span.start + int64(len(chunk))
	if s.upload.total >= 0 && length > s.upload.total {
		return status.Error(codes.InvalidArgument, "The upload has more bytes than its declared total")
	}
	if length > maxObjectSize {
		return errUploadTooLarge
	}
	s.data = append(s.data, chunk[skip:]...)
	return nil
}

// declare records a chunk's total size, which must not change once known.
func (s *uploadSession) declare(total int64) error {
	if total < 0 {
		return nil
	}
	if total > maxObjectSize {
		return errUploadTooLarge
	}
	if s.upload.total >= 0 && s.upload.total != total {
		return status.Error(codes.InvalidArgument, "The upload's total size changed")
	}
	s.upload.total = total
	return nil
}

// incompleteUpload is the 308 Resume Incomplete reply with the received range.
// A client that sends X-GUploader-No-308: yes (the Go client) gets 200 with
// X-Http-Status-Code-Override: 308 instead, as GCP's upload endpoint answers.
func incompleteUpload(session *uploadSession, no308 bool) *gateway.RESTResponse {
	headers := http.Header{}
	if len(session.data) > 0 {
		headers.Set("Range", "bytes=0-"+strconv.Itoa(len(session.data)-1))
	}
	if no308 {
		headers.Set("X-Http-Status-Code-Override", "308")
		return &gateway.RESTResponse{Status: http.StatusOK, Headers: headers}
	}
	return &gateway.RESTResponse{Status: http.StatusPermanentRedirect, Headers: headers}
}

// commitUpload writes a complete session through the normal insert path, as
// the session's initiator. A refused or failed commit ends the session.
func (p *Provider) commitUpload(call gateway.Call, driver backend.Backend, r *gateway.RESTRequest, scope state.Scope, id string, session *uploadSession) (*gateway.RESTResponse, error) {
	if refused := checkUploadHash(r.Headers.Get("X-Goog-Hash"), session.data); refused != nil {
		p.closeUpload(scope, id, session)
		return refused, nil
	}
	call.Method, call.Principal = MethodObjectsInsert, session.principal
	upload, err := multipartUpload(session.upload.name, session.upload.metadata, session.data)
	if err != nil {
		return nil, err
	}
	target, targetScope, refused, err := p.resolveTarget(call, session.bucket, upload.name)
	if refused != nil || err != nil || targetScope != scope {
		p.closeUpload(scope, id, session)
		return notFound(session.bucket, ""), err
	}
	call.Resource, call.Location = target.Name, state.Global
	req := request{call: call, target: target, scope: scope, bucket: session.bucket, object: upload.name, upload: upload,
		rest: &gateway.RESTRequest{Path: map[string]string{keyBucket: session.bucket}, Query: session.upload.query, Headers: session.upload.headers}}
	response, err := p.serve(call.Context, driver, req)
	if err != nil {
		return nil, err
	}
	if response.Status == http.StatusOK {
		session.result, session.data = response.JSON, nil
		return response, nil
	}
	p.closeUpload(scope, id, session)
	return response, nil
}

// closeUpload ends a session; later requests to its URI get 404.
func (p *Provider) closeUpload(scope state.Scope, id string, session *uploadSession) {
	session.closed, session.data = true, nil
	p.catalog.forgetUpload(scope, id)
}

// checkUploadHash verifies the X-Goog-Hash a client sends with the final chunk.
func checkUploadHash(header string, data []byte) *gateway.RESTResponse {
	if header == "" {
		return nil
	}
	sum := md5.Sum(data) //nolint:gosec // GCS reports and checks MD5 content hashes
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	computed := map[string]string{"md5": base64.StdEncoding.EncodeToString(sum[:]), "crc32c": base64.StdEncoding.EncodeToString(crc)}
	for item := range strings.SplitSeq(header, ",") {
		kind, value, _ := strings.Cut(strings.TrimSpace(item), "=")
		if expected, known := computed[kind]; known && value != expected {
			return wireError(400, "invalid", fmt.Sprintf("Provided %s %q doesn't match calculated %s %q.", strings.ToUpper(kind), value, strings.ToUpper(kind), expected))
		}
	}
	return nil
}

// multipartUpload renders an object and its metadata as the multipart/related
// insert the backend stores, so every upload protocol commits the same way.
func multipartUpload(name string, metadata map[string]json.RawMessage, data []byte) (*uploadRequest, error) {
	document := maps.Clone(metadata)
	if document == nil {
		document = map[string]json.RawMessage{}
	}
	document[keyName] = rawString(name)
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the object metadata")
	}
	kind := "application/octet-stream"
	if raw, ok := document[keyContentType]; ok && json.Unmarshal(raw, &kind) != nil {
		return nil, status.Error(codes.InvalidArgument, "contentType must be a string")
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, part := range []struct {
		kind    string
		content []byte
	}{{"application/json; charset=UTF-8", encoded}, {kind, data}} {
		w, err := writer.CreatePart(textproto.MIMEHeader{headerContentType: {part.kind}})
		if err != nil {
			return nil, status.Error(codes.Internal, "Unable to encode the upload")
		}
		_, _ = w.Write(part.content)
	}
	if err := writer.Close(); err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the upload")
	}
	return &uploadRequest{name: name, total: int64(len(data)), body: body.Bytes(),
		query:   url.Values{queryUploadType: {"multipart"}, keyName: {name}},
		headers: http.Header{headerContentType: {"multipart/related; boundary=" + writer.Boundary()}}}, nil
}

// rawString encodes one JSON string value.
func rawString(value string) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return encoded
}
