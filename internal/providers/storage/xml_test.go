package storage

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // GCS reports and checks MD5 content hashes
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"
)

// xmlRequest sends one XML API request with explicit headers.
func (f *fixture) xmlRequest(actor, method, path string, headers map[string]string, body []byte, expected int) *httptest.ResponseRecorder {
	f.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(body))
	if actor != "" {
		r.Header.Set("Authorization", "Bearer "+actor)
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

// TestXMLObjectReadWriteWithBearerTokens covers the XML API object paths the
// Go client reads and other XML clients write, and each refusal.
func TestXMLObjectReadWriteWithBearerTokens(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "xml-bucket", 200)
	stored := f.xmlRequest(admin, "PUT", "/xml-bucket/dir/obj.txt", map[string]string{
		"Content-Type": "text/plain", "X-Goog-Meta-Color": "blue", "Cache-Control": "no-cache",
	}, []byte("xml body"), 200)
	sum := md5.Sum([]byte("xml body")) //nolint:gosec // GCS reports and checks MD5 content hashes
	if stored.Header().Get("X-Goog-Generation") == "" || !strings.Contains(stored.Header().Get("X-Goog-Hash"), base64.StdEncoding.EncodeToString(sum[:])) || stored.Body.Len() != 0 {
		t.Fatalf("PUT reply = %v %q", stored.Header(), stored.Body.String())
	}
	object := decodeBody(t, f.request(admin, "GET", "/storage/v1/b/xml-bucket/o/dir%2Fobj.txt", nil, 200).Body.Bytes())
	if metadata, _ := object["metadata"].(map[string]any); metadata["color"] != "blue" || object["cacheControl"] != "no-cache" || object["contentType"] != "text/plain" {
		t.Fatalf("XML metadata = %v", object)
	}
	read := f.xmlRequest(admin, "GET", "/xml-bucket/dir/obj.txt?response-content-disposition=attachment", nil, nil, 200)
	if read.Body.String() != "xml body" || read.Header().Get("Content-Disposition") != "attachment" {
		t.Fatalf("GET = %q %v", read.Body.String(), read.Header())
	}
	generation, _ := object["generation"].(string)
	f.xmlRequest(admin, "GET", "/xml-bucket/dir/obj.txt?generation="+generation, nil, nil, 200)
}

// TestXMLRefusals covers each XML API request the provider refuses.
func TestXMLRefusals(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "xml-bucket", 200)
	f.xmlRequest(admin, "PUT", "/xml-bucket/dir/obj.txt", nil, []byte("xml body"), 200)
	for _, tc := range []struct {
		name, actor, method, path string
		headers                   map[string]string
		code                      int
		want                      string
	}{
		{"missing key", admin, "GET", "/xml-bucket/absent.txt", nil, 404, "NoSuchKey"},
		{"stale generation", admin, "GET", "/xml-bucket/dir/obj.txt?generation=1", nil, 404, "NoSuchKey"},
		{"anonymous", "", "GET", "/xml-bucket/dir/obj.txt", nil, 403, "AccessDenied"},
		{"no grant", "stranger@project-b.test", "PUT", "/xml-bucket/new.txt", nil, 403, "AccessDenied"},
		{"bad Content-MD5", admin, "PUT", "/xml-bucket/new.txt", map[string]string{"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}, 400, "BadDigest"},
		{"conditional header", admin, "GET", "/xml-bucket/dir/obj.txt", map[string]string{"If-None-Match": "x"}, 400, "Storage coverage gap"},
		{"precondition header", admin, "PUT", "/xml-bucket/new.txt", map[string]string{"X-Goog-If-Generation-Match": "0"}, 400, "Storage coverage gap"},
		{"payload hash", admin, "PUT", "/xml-bucket/new.txt", map[string]string{"X-Goog-Content-Sha256": "abc"}, 400, "Storage coverage gap"},
		{"metadata on GET", admin, "GET", "/xml-bucket/dir/obj.txt", map[string]string{"X-Goog-Meta-A": "b"}, 400, "Storage coverage gap"},
		{"unknown option", admin, "GET", "/xml-bucket/dir/obj.txt?acl", nil, 400, "Storage coverage gap"},
		{"GET-only option on PUT", admin, "PUT", "/xml-bucket/new.txt?generation=1", nil, 400, "Storage coverage gap"},
		{"repeated option", admin, "GET", "/xml-bucket/dir/obj.txt?generation=1&generation=2", nil, 400, "must not repeat"},
		{"unknown bucket", admin, "GET", "/no-such-bucket/obj.txt", nil, 501, "no bucket no-such-bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.t = t
			if w := f.xmlRequest(tc.actor, tc.method, tc.path, tc.headers, []byte("new"), tc.code); !strings.Contains(w.Body.String(), tc.want) {
				t.Fatalf("body = %s", w.Body.String())
			}
		})
	}
	f.t = t
	f.request(admin, "GET", "/storage/v1/b/xml-bucket/o/new.txt", nil, 404)
}

// TestRewriteCopiesWithBothSidesAuthorized covers the rclone-style rewrite
// with a null body and each refusal.
func TestRewriteCopiesWithBothSidesAuthorized(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "rw-bucket", 200)
	f.bucket(testProject, "rw-other", 200)
	f.bucket(peerProject, "rw-peer", 200)
	f.rawRequest("POST", "/upload/storage/v1/b/rw-bucket/o?uploadType=media&name=src.txt", "text/plain", nil, []byte("source media"), 200)
	rewrite := "/storage/v1/b/rw-bucket/o/src.txt/rewriteTo/b/rw-other/o/"
	result := decodeBody(t, f.request(admin, "POST", rewrite+"dst.txt", []byte("null"), 200).Body.Bytes())
	resource, _ := result["resource"].(map[string]any)
	if result["done"] != true || result["objectSize"] != "12" || resource["contentType"] != "text/plain" ||
		resource["mediaLink"] == nil {
		t.Fatalf("rewrite = %v", result)
	}
	if f.request(admin, "GET", "/storage/v1/b/rw-other/o/dst.txt?alt=media", nil, 200).Body.String() != "source media" {
		t.Fatal("rewrite did not copy the media")
	}
	// A body replaces the copied metadata.
	replaced := decodeBody(t, f.request(admin, "POST", rewrite+"typed.txt", []byte(`{"contentType":"application/json"}`), 200).Body.Bytes())
	if resource, _ := replaced["resource"].(map[string]any); resource["contentType"] != "application/json" {
		t.Fatalf("rewrite metadata = %v", replaced)
	}
	reader := "reader@project-b.test"
	f.policy(t, "rw-bucket", reader, "roles/storage.objectViewer")
	for _, tc := range []struct {
		name, actor, path, body string
		code                    int
	}{
		{"no destination create", reader, rewrite + "denied.txt", "", 403},
		{"no source read", "stranger@project-b.test", rewrite + "denied.txt", "", 403},
		{"missing source", admin, "/storage/v1/b/rw-bucket/o/absent.txt/rewriteTo/b/rw-other/o/x.txt", "", 404},
		{"stale source generation", admin, rewrite + "x.txt?sourceGeneration=1", "", 404},
		{"destination precondition", admin, rewrite + "dst.txt?ifGenerationMatch=0", "", 412},
		{"continuation token", admin, rewrite + "x.txt?rewriteToken=abc", "", 400},
		{"another project", admin, "/storage/v1/b/rw-bucket/o/src.txt/rewriteTo/b/rw-peer/o/x.txt", "", 400},
		{"names disagree", admin, rewrite + "x.txt", `{"name":"y.txt"}`, 400},
		{"unsupported metadata", admin, rewrite + "x.txt", `{"acl":[]}`, 400},
		{"missing destination bucket", admin, "/storage/v1/b/rw-bucket/o/src.txt/rewriteTo/b/absent-bucket/o/x.txt", "", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.t = t
			f.request(tc.actor, "POST", tc.path, []byte(tc.body), tc.code)
		})
	}
	f.t = t
	f.request(admin, "GET", "/storage/v1/b/rw-other/o/denied.txt", nil, 404)
	f.request(admin, "GET", "/storage/v1/b/rw-other/o/x.txt", nil, 404)
}
