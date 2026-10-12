package storage

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

// rawRequest sends one administrator request with explicit headers.
func (f *fixture) rawRequest(method, path, contentType string, headers map[string]string, body []byte, expected int) *httptest.ResponseRecorder {
	f.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+admin)
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	f.g.ServeHTTP(w, r)
	if w.Code != expected {
		f.t.Fatalf("%s %s: got %d want %d: %s", method, path, w.Code, expected, w.Body.String())
	}
	return w
}

// multipartBody builds a multipart/related upload from raw parts.
func multipartBody(parts ...string) []byte {
	var body strings.Builder
	for _, part := range parts {
		body.WriteString("--b0undary\r\nContent-Type: application/octet-stream\r\n\r\n" + part + "\r\n")
	}
	body.WriteString("--b0undary--\r\n")
	return []byte(body.String())
}

// Conditional request headers.
const (
	ifMatch     = "If-Match"
	ifNoneMatch = "If-None-Match"
)

const multipartType = "multipart/related; boundary=b0undary"

// TestMultipartUploadsAndMetadataLimits covers the multipart upload protocol
// and every metadata shape the provider refuses before the backend.
func TestMultipartUploadsAndMetadataLimits(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "mp-bucket", 200)
	upload := "/upload/storage/v1/b/mp-bucket/o?uploadType=multipart"
	created := decodeBody(t, f.rawRequest("POST", upload, multipartType, nil,
		multipartBody(`{"name":"doc.txt","bucket":"mp-bucket","contentType":"text/plain","storageClass":"STANDARD","metadata":{"k":"v"}}`, "multipart media"), 200).Body.Bytes())
	if created["name"] != "doc.txt" {
		t.Fatalf("multipart create = %v", created)
	}
	// Links name the emulator's public origin, never the backend's.
	generation, _ := created["generation"].(string)
	if created["mediaLink"] != "http://example.com/download/storage/v1/b/mp-bucket/o/doc.txt?alt=media&generation="+generation ||
		created["selfLink"] != "http://example.com/storage/v1/b/mp-bucket/o/doc.txt" {
		t.Fatalf("object links = %v, %v", created["mediaLink"], created["selfLink"])
	}
	if f.request(admin, "GET", "/download/storage/v1/b/mp-bucket/o/doc.txt?alt=media&generation="+generation, nil, 200).Body.String() != "multipart media" {
		t.Fatal("mediaLink did not download the object")
	}
	if f.request(admin, "GET", "/storage/v1/b/mp-bucket/o/doc.txt?alt=media", nil, 200).Body.String() != "multipart media" {
		t.Fatal("multipart media did not round-trip")
	}
	// A query name that agrees with the metadata name is accepted.
	f.rawRequest("POST", upload+"&name=same.txt", multipartType, nil, multipartBody(`{"name":"same.txt"}`, "x"), 200)

	tests := []struct {
		name, path, contentType string
		body                    []byte
		want                    int
	}{
		{"unsupported metadata", upload, multipartType, multipartBody(`{"name":"a","acl":[]}`, "x"), 400},
		{"storage class", upload, multipartType, multipartBody(`{"name":"a","storageClass":"COLDLINE"}`, "x"), 400},
		{"bucket mismatch", upload, multipartType, multipartBody(`{"name":"a","bucket":"other"}`, "x"), 400},
		{"names disagree", upload + "&name=b", multipartType, multipartBody(`{"name":"a"}`, "x"), 400},
		{"name type", upload, multipartType, multipartBody(`{"name":1}`, "x"), 400},
		{"no name", upload, multipartType, multipartBody(`{"contentType":"text/plain"}`, "x"), 400},
		{"metadata not object", upload, multipartType, multipartBody(`[]`, "x"), 400},
		{"missing media", upload, multipartType, multipartBody(`{"name":"a"}`), 400},
		{"third part", upload, multipartType, multipartBody(`{"name":"a"}`, "x", "y"), 400},
		{"no parts", upload, multipartType, []byte("--b0undary--\r\n"), 400},
		{"wrong content type", upload, "multipart/form-data; boundary=b0undary", multipartBody(`{"name":"a"}`, "x"), 400},
		{"no boundary", upload, "multipart/related", multipartBody(`{"name":"a"}`, "x"), 400},
		{"media name newline", "/upload/storage/v1/b/mp-bucket/o?uploadType=media&name=a%0Ab", "text/plain", []byte("x"), 400},
	}
	for _, test := range tests {
		f.rawRequest("POST", test.path, test.contentType, nil, test.body, test.want)
	}
	f.rawRequest("POST", "/upload/storage/v1/b/mp-bucket/o?uploadType=media&name=enc", "text/plain", map[string]string{"X-Goog-Encryption-Key": "k"}, []byte("x"), 400)
	listed := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/mp-bucket/o", nil, 200).Body.Bytes())
	if items, _ := listed["items"].([]any); len(items) != 2 {
		t.Fatalf("rejected uploads reached the backend: %v", listed)
	}
}

// TestObjectPreconditionsAndConditionalReads covers generation, metageneration,
// and etag preconditions on create, read, and delete.
func TestObjectPreconditionsAndConditionalReads(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "pre-bucket", 200)
	media := "/upload/storage/v1/b/pre-bucket/o?uploadType=media&name=obj"
	created := decodeBody(t, f.rawRequest("POST", media+"&ifGenerationMatch=0", "text/plain", nil, []byte("v1"), 200).Body.Bytes())
	generation, _ := created["generation"].(string)
	etag, _ := created["etag"].(string)
	if generation == "" || etag == "" {
		t.Fatalf("create = %v", created)
	}
	f.rawRequest("POST", media+"&ifGenerationMatch=0", "text/plain", nil, []byte("v2"), 412)
	f.rawRequest("POST", media+"&ifGenerationMatch=abc", "text/plain", nil, []byte("v2"), 400)
	f.rawRequest("POST", media+"&ifGenerationMatch=-1", "text/plain", nil, []byte("v2"), 400)

	object := "/storage/v1/b/pre-bucket/o/obj"
	read := f.rawRequest("GET", object, "", nil, nil, 200)
	if read.Header().Get("ETag") != `"`+etag+`"` {
		t.Fatalf("etag header = %q, body etag %q", read.Header().Get("ETag"), etag)
	}
	f.rawRequest("GET", object, "", map[string]string{ifNoneMatch: `"` + etag + `"`}, nil, 304)
	f.rawRequest("GET", object, "", map[string]string{ifNoneMatch: `"other"`}, nil, 200)
	f.rawRequest("GET", object, "", map[string]string{ifMatch: `"other"`}, nil, 412)
	f.rawRequest("GET", object, "", map[string]string{ifMatch: `"other", "` + etag + `"`}, nil, 200)
	f.rawRequest("GET", object+"?ifMetagenerationMatch=1&ifGenerationNotMatch=0", "", nil, nil, 200)
	f.rawRequest("GET", object+"?ifMetagenerationNotMatch=1", "", nil, nil, 412)
	f.rawRequest("GET", object+"?generation=1", "", nil, nil, 404)
	f.rawRequest("GET", object+"?generation="+generation+"&alt=media", "", nil, nil, 200)
	f.rawRequest("GET", "/storage/v1/b/pre-bucket/o/absent", "", nil, nil, 404)

	f.rawRequest("DELETE", object+"?ifGenerationNotMatch="+generation, "", nil, nil, 412)
	f.rawRequest("DELETE", object, "", map[string]string{ifNoneMatch: "*"}, nil, 412)
	f.rawRequest("DELETE", object+"?ifGenerationMatch="+generation, "", nil, nil, 204)
	f.rawRequest("DELETE", object, "", nil, nil, 404)
	// Overwrite-protection: replacing a missing object with If-Match: * fails.
	f.rawRequest("POST", media, "text/plain", map[string]string{ifMatch: "*"}, []byte("v3"), 412)
}
