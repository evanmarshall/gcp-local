package storage

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // GCS reports and checks MD5 content hashes
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// xmlQuery lists the XML API query options served; GET also takes
// generation and the response-header overrides.
var xmlQuery = map[string]bool{
	"X-Goog-Algorithm": true, "X-Goog-Credential": true, "X-Goog-Date": true,
	"X-Goog-Expires": true, "X-Goog-SignedHeaders": true, paramSignature: true,
}

// xmlGetQuery lists the options only an XML GET takes.
var xmlGetQuery = map[string]bool{keyGeneration: true, "response-content-type": true, "response-content-disposition": true}

// xmlHeaders lists the x-goog- request headers an XML request may carry.
var xmlHeaders = map[string]bool{"x-goog-api-client": true, "x-goog-gcs-idempotency-token": true}

// validateXMLRequest refuses XML API options and headers this provider does
// not implement, so none is silently ignored.
func validateXMLRequest(r *gateway.RESTRequest) error {
	for key, values := range r.Query {
		if !xmlQuery[key] && (!xmlGetQuery[key] || r.Verb != http.MethodGet) {
			return gap("unsupported XML API query option " + key)
		}
		if len(values) != 1 {
			return status.Error(codes.InvalidArgument, "Query options must not repeat")
		}
	}
	for key := range r.Headers {
		if err := validateXMLHeader(r, key); err != nil {
			return err
		}
	}
	if !validObjectName(r.Path["object"]) {
		return status.Error(codes.InvalidArgument, "Invalid object name")
	}
	return nil
}

// validateXMLHeader refuses a conditional or x-goog- header that is not served.
func validateXMLHeader(r *gateway.RESTRequest, key string) error {
	lower := strings.ToLower(key)
	switch {
	case lower == "if-match" || lower == "if-none-match":
		return gap("XML API conditional headers")
	case lower == headerPayloadHash && r.Headers.Get(key) != unsignedPayload:
		return gap("signed payload hashes; sign with UNSIGNED-PAYLOAD")
	case strings.HasPrefix(lower, "x-goog-meta-") && r.Verb == http.MethodPut:
	case strings.HasPrefix(lower, "x-goog-") && !xmlHeaders[lower] && lower != headerPayloadHash:
		return gap("XML API header " + lower)
	}
	return nil
}

// newXMLRequest resolves an XML API object request. A V4 signature makes its
// signer the principal; otherwise the bearer token's principal applies.
func (p *Provider) newXMLRequest(call gateway.Call, r *gateway.RESTRequest) (request, *gateway.RESTResponse, error) {
	if err := validateXMLRequest(r); err != nil {
		return request{}, nil, err
	}
	bucket := r.Path[keyBucket]
	signed := isSigned(r.Query)
	if signed {
		if call.Principal.State != gateway.Anonymous {
			return request{}, xmlError(400, "InvalidArgument", "Authenticate with a signature or an Authorization header, not both.", ""), nil
		}
		principal, refused := verifySignedURL(r, time.Now(), p.iam.VerifySignature)
		if refused != nil {
			return request{}, refused, nil
		}
		call.Principal = principal
	}
	if _, found := p.index.Get(BucketResource(bucket)); !found {
		if !signed {
			// The XML route matches almost any path. Without a signature or a
			// known bucket, report the missing route as the gateway would.
			return request{}, nil, status.Error(codes.Unimplemented, "No provider implements this REST API method, and no bucket "+bucket+" exists for the XML API")
		}
		return request{}, xmlError(404, "NoSuchBucket", "The specified bucket does not exist.", ""), nil
	}
	req := request{call: call, rest: r, bucket: bucket, object: r.Path["object"], xml: true}
	req.call.Method = MethodObjectsGet
	if r.Verb == http.MethodPut {
		req.call.Method = MethodObjectsInsert
		upload, refused, err := xmlUpload(r, req.object)
		if refused != nil || err != nil {
			return request{}, refused, err
		}
		req.upload = upload
	}
	return p.resolveRequest(req)
}

// xmlUpload reads an XML PUT body and its object headers.
func xmlUpload(r *gateway.RESTRequest, name string) (*uploadRequest, *gateway.RESTResponse, error) {
	data, err := readChunk(r.Media)
	if err != nil {
		return nil, nil, err
	}
	if want := r.Headers.Get("Content-MD5"); want != "" {
		sum := md5.Sum(data) //nolint:gosec // GCS reports and checks MD5 content hashes
		if want != base64.StdEncoding.EncodeToString(sum[:]) {
			return nil, xmlError(400, "BadDigest", "The Content-MD5 you specified did not match what we received.", ""), nil
		}
	}
	metadata := map[string]json.RawMessage{}
	for header, field := range map[string]string{headerContentType: keyContentType, "Cache-Control": keyCacheControl,
		"Content-Disposition": keyDisposition, "Content-Language": keyLanguage} {
		if value := r.Headers.Get(header); value != "" {
			metadata[field] = rawString(value)
		}
	}
	custom := map[string]string{}
	for key := range r.Headers {
		if lower := strings.ToLower(key); strings.HasPrefix(lower, "x-goog-meta-") {
			custom[strings.TrimPrefix(lower, "x-goog-meta-")] = r.Headers.Get(key)
		}
	}
	if len(custom) > 0 {
		encoded, err := json.Marshal(custom)
		if err != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "Invalid x-goog-meta- headers")
		}
		metadata["metadata"] = encoded
	}
	upload, err := multipartUpload(name, metadata, data)
	return upload, nil, err
}

// executeXML reads an object's media, or writes one and answers with the XML
// API's empty body and object headers.
func executeXML(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	if req.call.Method != MethodObjectsInsert {
		return xmlGet(ctx, b, req)
	}
	response, err := insertObject(ctx, b, req, current)
	if err != nil || response.Status != http.StatusOK {
		return response, err
	}
	object, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	return xmlStored(object), nil
}

// xmlGet reads an object's media with the response-header overrides applied.
func xmlGet(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	current, refused, err := currentObject(ctx, b, req.bucket, req.object)
	if refused != nil || err != nil {
		return refused, err
	}
	query := url.Values{queryAlt: {altMedia}}
	if generation := req.rest.Query.Get(keyGeneration); generation != "" {
		if current[keyGeneration] != generation {
			return notFound(req.bucket, req.object), nil
		}
		query.Set(keyGeneration, generation)
	}
	response, err := b.forward(ctx, http.MethodGet, objectPath(req.bucket, req.object), query, req.rest.Headers, nil)
	if err != nil || response.Status >= 300 {
		return response, err
	}
	for option, header := range map[string]string{"response-content-type": headerContentType, "response-content-disposition": "Content-Disposition"} {
		if value := req.rest.Query.Get(option); value != "" {
			response.Headers.Set(header, value)
		}
	}
	return response, nil
}

// xmlStored is the XML PUT reply: no body, the object's identity in headers.
func xmlStored(object map[string]any) *gateway.RESTResponse {
	headers := http.Header{}
	md5Hash, _ := object["md5Hash"].(string)
	if sum, err := base64.StdEncoding.DecodeString(md5Hash); err == nil {
		headers.Set("ETag", `"`+hex.EncodeToString(sum)+`"`)
		crc, _ := object["crc32c"].(string)
		headers.Set("X-Goog-Hash", "crc32c="+crc+",md5="+md5Hash)
	}
	for field, header := range map[string]string{keyGeneration: "X-Goog-Generation", "metageneration": "X-Goog-Metageneration", "size": "X-Goog-Stored-Content-Length"} {
		if value, ok := object[field].(string); ok {
			headers.Set(header, value)
		}
	}
	return &gateway.RESTResponse{Status: http.StatusOK, Headers: headers, Body: bytes.NewReader(nil)}
}

// xmlErrorBody is the XML API error document.
type xmlErrorBody struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
	Details string   `xml:"Details,omitempty"`
}

func xmlError(code int, kind, message, details string) *gateway.RESTResponse {
	encoded, err := xml.Marshal(xmlErrorBody{Code: kind, Message: message, Details: details})
	if err != nil {
		encoded = nil
	}
	return &gateway.RESTResponse{Status: code, Headers: http.Header{headerContentType: {"application/xml; charset=UTF-8"}},
		Body: bytes.NewReader(append([]byte(xml.Header), encoded...))}
}

// xmlResult renders a JSON API error from the shared object path as the XML
// API's error document, keeping the decision ID.
func xmlResult(response *gateway.RESTResponse) *gateway.RESTResponse {
	if response.Status < 300 || response.JSON == nil {
		return response
	}
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if encoded, err := json.Marshal(response.JSON); err == nil {
		_ = json.Unmarshal(encoded, &body)
	}
	code, kind, message := response.Status, "InvalidArgument", body.Error.Message
	switch response.Status {
	case 401, 403:
		// The XML API refuses anonymous and unauthorized callers alike with 403.
		code, kind, message = 403, "AccessDenied", "Access denied."
	case 404:
		kind, message = "NoSuchKey", "The specified key does not exist."
		if !strings.HasPrefix(body.Error.Message, "No such object") {
			kind, message = "NoSuchBucket", "The specified bucket does not exist."
		}
	case 412:
		kind = "PreconditionFailed"
	}
	rendered := xmlError(code, kind, message, body.Error.Message)
	if id := response.Headers.Get("X-Gcp-Local-Decision-Id"); id != "" {
		rendered.Headers.Set("X-Gcp-Local-Decision-Id", id)
	}
	return rendered
}
