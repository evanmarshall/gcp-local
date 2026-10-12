package xmlapi

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
)

// Stored is the XML PUT reply: no body, the object's identity in headers.
func Stored(object map[string]any) *gateway.RESTResponse {
	headers := http.Header{}
	md5Hash, _ := object["md5Hash"].(string)
	if sum, err := base64.StdEncoding.DecodeString(md5Hash); err == nil {
		headers.Set("ETag", `"`+hex.EncodeToString(sum)+`"`)
		crc, _ := object["crc32c"].(string)
		headers.Set("X-Goog-Hash", "crc32c="+crc+",md5="+md5Hash)
	}
	for field, header := range map[string]string{"generation": "X-Goog-Generation", "metageneration": "X-Goog-Metageneration", "size": "X-Goog-Stored-Content-Length"} {
		if value, ok := object[field].(string); ok {
			headers.Set(header, value)
		}
	}
	return &gateway.RESTResponse{Status: http.StatusOK, Headers: headers, Body: bytes.NewReader(nil)}
}

// errorBody is the XML API error document.
type errorBody struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
	Details string   `xml:"Details,omitempty"`
}

// Error renders an XML API error document.
func Error(code int, kind, message, details string) *gateway.RESTResponse {
	encoded, err := xml.Marshal(errorBody{Code: kind, Message: message, Details: details})
	if err != nil {
		encoded = nil
	}
	return &gateway.RESTResponse{Status: code, Headers: http.Header{"Content-Type": {"application/xml; charset=UTF-8"}},
		Body: bytes.NewReader(append([]byte(xml.Header), encoded...))}
}

// Result renders a JSON API error from the shared object path as the XML
// API's error document, keeping the decision ID.
func Result(response *gateway.RESTResponse) *gateway.RESTResponse {
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
	rendered := Error(code, kind, message, body.Error.Message)
	if id := response.Headers.Get("X-Gcp-Local-Decision-Id"); id != "" {
		rendered.Headers.Set("X-Gcp-Local-Decision-Id", id)
	}
	return rendered
}
