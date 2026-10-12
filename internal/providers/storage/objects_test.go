package storage

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // GCS reports and checks MD5 content hashes
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/state"
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
	if created["mediaLink"] != "http://example.com/download/storage/v1/b/mp-bucket/o/doc.txt?generation="+generation+"&alt=media" ||
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

// patchObjectPath is the object every patch test updates.
const patchObjectPath = "/storage/v1/b/patch-bucket/o/doc.txt"

// patchFixture creates patch-bucket with doc.txt.
func patchFixture(t *testing.T) (*fixture, map[string]any) {
	t.Helper()
	f := setup(t)
	f.bucket(testProject, "patch-bucket", 200)
	created := decodeBody(t, f.rawRequest("POST", "/upload/storage/v1/b/patch-bucket/o?uploadType=media&name=doc.txt", "application/octet-stream", nil, []byte("patch me"), 200).Body.Bytes())
	return f, created
}

// requireRefusal asserts a 400 whose message names the refusal.
func requireRefusal(t *testing.T, f *fixture, actor, method, path, body, message string) {
	t.Helper()
	if got := f.request(actor, method, path, []byte(body), 400).Body.String(); !strings.Contains(got, message) {
		t.Fatalf("%s %s %s: %s, want %q", method, path, body, got, message)
	}
}

// TestObjectPatchUpdatesMetadataInPlace covers the fields the Go client's
// ObjectHandle.Update sends, with the request shape it uses.
func TestObjectPatchUpdatesMetadataInPlace(t *testing.T) {
	f, created := patchFixture(t)
	patched := decodeBody(t, f.request(admin, "PATCH", patchObjectPath+"?alt=json&prettyPrint=false&projection=full",
		[]byte(`{"bucket":"patch-bucket","name":"doc.txt","contentType":"text/plain","cacheControl":"no-cache","contentDisposition":"inline","contentEncoding":"identity","contentLanguage":"en"}`), 200).Body.Bytes())
	generation, _ := created[keyGeneration].(string)
	for field, want := range map[string]string{
		keyContentType: "text/plain", keyCacheControl: "no-cache", keyDisposition: "inline",
		"contentEncoding": "identity", keyLanguage: "en", "metageneration": "2", keyGeneration: generation,
	} {
		if patched[field] != want {
			t.Fatalf("%s = %v, want %q: %v", field, patched[field], want, patched)
		}
	}
	if patched["etag"] == created["etag"] || patched["selfLink"] != "http://example.com"+patchObjectPath {
		t.Fatalf("patched etag/link = %v, %v", patched["etag"], patched["selfLink"])
	}
	// Reads report the new metageneration, in the body and in media headers.
	if read := decodeBody(t, f.request(admin, "GET", patchObjectPath, nil, 200).Body.Bytes()); read["metageneration"] != "2" || read["etag"] != patched["etag"] {
		t.Fatalf("read after patch = %v", read)
	}
	media := f.request(admin, "GET", patchObjectPath+"?alt=media", nil, 200)
	if media.Header().Get("X-Goog-Metageneration") != "2" || media.Body.String() != "patch me" {
		t.Fatalf("media after patch = %v %q", media.Header(), media.Body.String())
	}
	// Replacing the object starts a new generation at metageneration 1.
	replaced := decodeBody(t, f.rawRequest("POST", "/upload/storage/v1/b/patch-bucket/o?uploadType=media&name=doc.txt", "text/plain", nil, []byte("v2"), 200).Body.Bytes())
	if replaced["metageneration"] != "1" {
		t.Fatalf("replacement metageneration = %v", replaced["metageneration"])
	}
}

// TestObjectPatchMergesMetadata checks that custom metadata merges by key:
// a null value removes one key, and null removes them all.
func TestObjectPatchMergesMetadata(t *testing.T) {
	f, _ := patchFixture(t)
	f.request(admin, "PATCH", patchObjectPath, []byte(`{"contentType":"text/plain","metadata":{"a":"1","b":"2"}}`), 200)
	merged := decodeBody(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"metadata":{"a":null,"c":"3"}}`), 200).Body.Bytes())
	if metadata, _ := merged[keyMetadata].(map[string]any); len(metadata) != 2 || metadata["b"] != "2" || metadata["c"] != "3" || merged[keyContentType] != "text/plain" {
		t.Fatalf("merged metadata = %v", merged)
	}
	cleared := decodeBody(t, f.request(admin, "PATCH", patchObjectPath, []byte(`{"metadata":null}`), 200).Body.Bytes())
	if metadata, _ := cleared[keyMetadata].(map[string]any); len(metadata) != 0 || cleared["metageneration"] != "4" {
		t.Fatalf("cleared metadata = %v", cleared)
	}
}

// TestObjectPatchPreconditions covers the generation and metageneration
// conditions the Go client sends from ObjectHandle.If and Generation.
func TestObjectPatchPreconditions(t *testing.T) {
	f, created := patchFixture(t)
	generation, _ := created[keyGeneration].(string)
	body := []byte(`{"contentType":"text/plain"}`)
	f.request(admin, "PATCH", patchObjectPath+"?ifMetagenerationMatch=1&ifGenerationMatch="+generation, body, 200)
	f.request(admin, "PATCH", patchObjectPath+"?ifMetagenerationMatch=1", body, 412)
	f.request(admin, "PATCH", patchObjectPath+"?ifMetagenerationNotMatch=2", body, 412)
	f.request(admin, "PATCH", patchObjectPath+"?ifGenerationMatch=1", body, 412)
	f.request(admin, "PATCH", patchObjectPath+"?ifGenerationNotMatch="+generation, body, 412)
	f.request(admin, "PATCH", patchObjectPath+"?ifMetagenerationMatch=abc", body, 400)
	f.request(admin, "PATCH", patchObjectPath+"?generation=1", body, 404)
	f.request(admin, "PATCH", patchObjectPath+"?generation="+generation+"&ifMetagenerationMatch=2", body, 200)
	f.request(admin, "PATCH", "/storage/v1/b/patch-bucket/o/absent.txt", body, 404)
	f.request(admin, "PATCH", "/storage/v1/b/absent-bucket/o/doc.txt", body, 404)
	if read := decodeBody(t, f.request(admin, "GET", patchObjectPath, nil, 200).Body.Bytes()); read["metageneration"] != "3" {
		t.Fatalf("failed preconditions advanced the metageneration: %v", read)
	}
}

// TestObjectPatchAuthorizesBeforeMutation checks the storage.objects.update
// plan: a reader is denied and the object is unchanged; an object user may patch.
func TestObjectPatchAuthorizesBeforeMutation(t *testing.T) {
	f, _ := patchFixture(t)
	reader, writer := "reader@project-b.test", "writer@project-b.test"
	f.policy(t, "patch-bucket", reader, "roles/storage.objectViewer")
	denied := f.request(reader, "PATCH", patchObjectPath, []byte(`{"contentType":"text/plain"}`), 403).Body.String()
	if !strings.Contains(denied, "storage.objects.update") {
		t.Fatalf("denial = %s", denied)
	}
	// A denied caller learns nothing about unsupported fields either.
	f.request(reader, "PATCH", patchObjectPath, []byte(`{"acl":[]}`), 403)
	if read := decodeBody(t, f.request(admin, "GET", patchObjectPath, nil, 200).Body.Bytes()); read[keyContentType] != "application/octet-stream" || read["metageneration"] != "1" {
		t.Fatalf("denied patch changed the object: %v", read)
	}
	f.policy(t, "patch-bucket", writer, "roles/storage.objectUser")
	f.request(writer, "PATCH", patchObjectPath, []byte(`{"contentType":"text/plain"}`), 200)
}

// TestObjectPatchRefusals asserts each field, value, and option the patch
// refuses, and that a refused patch leaves the object unchanged.
func TestObjectPatchRefusals(t *testing.T) {
	f, _ := patchFixture(t)
	for _, test := range []struct{ path, body, message string }{
		{patchObjectPath, `{"acl":[]}`, "Storage coverage gap: unsupported object patch field acl"},
		{patchObjectPath, `{"customTime":"2026-01-01T00:00:00Z"}`, "Storage coverage gap: unsupported object patch field customTime"},
		{patchObjectPath, `{"contentType":"text/plain","temporaryHold":true}`, "Storage coverage gap: unsupported object patch field temporaryHold"},
		{patchObjectPath, `{"contentType":null}`, "Storage coverage gap: clearing object field contentType"},
		{patchObjectPath, `{"cacheControl":""}`, "Storage coverage gap: clearing object field cacheControl"},
		{patchObjectPath + "?predefinedAcl=private", `{}`, "Storage coverage gap: unsupported query option predefinedAcl"},
		{patchObjectPath + "?overrideUnlockedRetention=true", `{}`, "Storage coverage gap: unsupported query option overrideUnlockedRetention"},
		{patchObjectPath + "?userProject=x", `{}`, "Storage coverage gap: unsupported query option userProject"},
		{patchObjectPath, `[]`, "Expected a JSON object body"},
		{patchObjectPath, `{"bucket":"other","contentType":"text/plain"}`, "Object bucket does not match the patch target"},
		{patchObjectPath, `{"name":"other.txt"}`, "Object name does not match the patch target"},
		{patchObjectPath, `{"contentType":1}`, "contentType must be a string"},
		{patchObjectPath, `{"metadata":{"a":1}}`, "metadata values must be strings or null"},
	} {
		requireRefusal(t, f, admin, "PATCH", test.path, test.body, test.message)
	}
	if read := decodeBody(t, f.request(admin, "GET", patchObjectPath, nil, 200).Body.Bytes()); read[keyContentType] != "application/octet-stream" || read["metageneration"] != "1" {
		t.Fatalf("a refused patch changed the object: %v", read)
	}
	// objects.update (PUT) remains a gap.
	requireRefusal(t, f, admin, "PUT", patchObjectPath, `{"contentType":"text/plain"}`, "storage.objects.update is not implemented")
}

// TestUndefinedQueryOptionsAreIgnored checks that an option discovery does
// not define for the method is ignored, as GCP ignores it, while defined
// options the emulator does not serve and credentials stay refused.
func TestUndefinedQueryOptionsAreIgnored(t *testing.T) {
	f, _ := patchFixture(t)
	f.request(admin, "GET", "/storage/v1/b/patch-bucket?alt=json&prettyPrint=false&projection=full&unsupportedOption=true", nil, 200)
	f.request(admin, "GET", "/storage/v1/b/patch-bucket/o?unsupportedOption=a&unsupportedOption=b", nil, 200)
	f.request(admin, "GET", patchObjectPath+"?alt=media&ifSourceGenerationMatch=1", nil, 200)
	for _, test := range []struct{ path, message string }{
		{"/storage/v1/b/patch-bucket?userProject=x", "unsupported query option userProject"},
		{"/storage/v1/b/patch-bucket?generation=1", "unsupported query option generation"},
		{"/storage/v1/b/patch-bucket?quotaUser=x", "unsupported query option quotaUser"},
		{"/storage/v1/b/patch-bucket?oauth_token=x", "unsupported query option oauth_token"},
		{"/storage/v1/b/patch-bucket?access_token=x", "unsupported query option access_token"},
		{"/storage/v1/b/patch-bucket/o?filter=x", "unsupported query option filter"},
	} {
		requireRefusal(t, f, admin, "GET", test.path, "", "Storage coverage gap: "+test.message)
	}
	// Ignoring an option never lets an anonymous caller through.
	f.request("", "GET", "/storage/v1/b/patch-bucket?unsupportedOption=true", nil, 401)
}

// session sends one request to a resumable session URI with optional bearer actor.
func (f *fixture) session(actor, method, location string, headers map[string]string, body []byte, expected int) *httptest.ResponseRecorder {
	f.t.Helper()
	parsed, err := url.Parse(location)
	if err != nil {
		f.t.Fatal(err)
	}
	r := httptest.NewRequestWithContext(context.Background(), method, parsed.RequestURI(), bytes.NewReader(body))
	if actor != "" {
		r.Header.Set("Authorization", "Bearer "+actor)
	}
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	f.g.ServeHTTP(w, r)
	if w.Code != expected {
		f.t.Fatalf("%s %s: got %d want %d: %s", method, location, w.Code, expected, w.Body.String())
	}
	return w
}

// startSession initiates a resumable upload and returns its session URI.
func (f *fixture) startSession(actor, bucket, query, metadata string, headers map[string]string, expected int) string {
	f.t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=resumable"+query, strings.NewReader(metadata))
	r.Header.Set("Authorization", "Bearer "+actor)
	r.Header.Set("Content-Type", "application/json; charset=UTF-8")
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	f.g.ServeHTTP(w, r)
	if w.Code != expected {
		f.t.Fatalf("initiate: got %d want %d: %s", w.Code, expected, w.Body.String())
	}
	return w.Header().Get("Location")
}

func crc32cHash(data []byte) string {
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	return "crc32c=" + base64.StdEncoding.EncodeToString(sum)
}

func span(start, end int, total string) map[string]string {
	return map[string]string{"Content-Range": "bytes " + strconv.Itoa(start) + "-" + strconv.Itoa(end) + "/" + total}
}

// TestResumableUploadChunksStatusAndCommit uploads a 9 MiB object, above the
// old 8 MiB limit, in chunks with a status query and a retried chunk.
func TestResumableUploadChunksStatusAndCommit(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "big-bucket", 200)
	payload := bytes.Repeat([]byte("0123456789abcdef"), (9<<20)/16)
	location := f.startSession(admin, "big-bucket", "", `{"name":"big.bin","metadata":{"k":"v"}}`,
		map[string]string{"X-Upload-Content-Type": "application/x-test"}, 200)
	if !strings.HasPrefix(location, "http://example.com/upload/storage/v1/b/big-bucket/o?") || !strings.Contains(location, "upload_id=") {
		t.Fatalf("session URI = %q", location)
	}
	half := 4 << 20
	// The session URI is the credential: a chunk needs no Authorization.
	w := f.session("", "PUT", location, span(0, half-1, "*"), payload[:half], 308)
	if w.Header().Get("Range") != "bytes=0-"+strconv.Itoa(half-1) {
		t.Fatalf("Range = %q", w.Header().Get("Range"))
	}
	if f.session("", "PUT", location, map[string]string{"Content-Range": "bytes */*"}, nil, 308).Header().Get("Range") != w.Header().Get("Range") {
		t.Fatal("status query moved the range")
	}
	// A retried chunk overlapping received bytes keeps only the new bytes.
	f.session("", "PUT", location, span(half/2, 2*half-1, "*"), payload[half/2:2*half], 308)
	final := f.session("", "PUT", location, map[string]string{
		"Content-Range": "bytes " + strconv.Itoa(2*half) + "-" + strconv.Itoa(len(payload)-1) + "/" + strconv.Itoa(len(payload)),
		"X-Goog-Hash":   crc32cHash(payload),
	}, payload[2*half:], 200)
	object := decodeBody(t, final.Body.Bytes())
	metadata, _ := object["metadata"].(map[string]any)
	if object["name"] != "big.bin" || object["size"] != strconv.Itoa(len(payload)) || object["contentType"] != "application/x-test" || metadata["k"] != "v" {
		t.Fatalf("committed object = %v", object)
	}
	assertCommitted(t, f, location, object["generation"], payload)
}

// assertCommitted checks that a repeated final request returns the committed
// object again and that the media round-trips.
func assertCommitted(t *testing.T, f *fixture, location string, generation any, payload []byte) {
	t.Helper()
	again := decodeBody(t, f.session("", "PUT", location, map[string]string{"Content-Range": "bytes */*"}, nil, 200).Body.Bytes())
	if again["generation"] != generation {
		t.Fatal("repeated commit wrote a new generation")
	}
	if !bytes.Equal(f.request(admin, "GET", "/storage/v1/b/big-bucket/o/big.bin?alt=media", nil, 200).Body.Bytes(), payload) {
		t.Fatal("resumable upload did not round-trip")
	}
}

// TestResumableSingleRequestAndCancel covers a whole-object PUT without
// Content-Range and a cancelled session.
func TestResumableSingleRequestAndCancel(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "big-bucket", 200)
	single := f.startSession(admin, "big-bucket", "&name=single.txt", "", nil, 200)
	// A session URI ignores an option discovery does not define, as GCP does.
	f.session("", "PUT", single+"&unsupportedOption=true", nil, []byte("one shot"), 200)
	cancelled := f.startSession(admin, "big-bucket", "&name=cancelled.txt", "", nil, 200)
	f.session("", "DELETE", cancelled, nil, nil, 499)
	f.session("", "PUT", cancelled, nil, []byte("late"), 499)
	f.session("", "DELETE", cancelled, nil, nil, 499)
	f.request(admin, "GET", "/storage/v1/b/big-bucket/o/cancelled.txt", nil, 404)
}

// TestResumableCommitReauthorizesTheInitiator checks that the final chunk is
// authorized as the session's initiator against the object at commit time.
func TestResumableCommitReauthorizesTheInitiator(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "auth-bucket", 200)
	creator := "creator@project-b.test"
	f.policy(t, "auth-bucket", creator, "roles/storage.objectCreator")
	f.startSession("stranger@project-b.test", "auth-bucket", "&name=x", "", nil, 403)
	replaced := f.startSession(creator, "auth-bucket", "&name=taken.txt", "", nil, 200)
	// The object appears after initiation, so committing now replaces it,
	// which needs storage.objects.delete the creator lacks.
	f.request(admin, "POST", "/upload/storage/v1/b/auth-bucket/o?uploadType=media&name=taken.txt", []byte("admin"), 200)
	w := f.session(admin, "PUT", replaced, nil, []byte("creator"), 403)
	decision, ok := f.p.iam.Decision(w.Header().Get("X-Gcp-Local-Decision-Id"))
	if !ok || decision.Principal.Subject != "serviceAccount:"+creator {
		t.Fatalf("commit authorized as %+v, not the initiator", decision.Principal)
	}
	f.session(admin, "PUT", replaced, nil, []byte("creator"), 404)
	if f.request(admin, "GET", "/storage/v1/b/auth-bucket/o/taken.txt?alt=media", nil, 200).Body.String() != "admin" {
		t.Fatal("refused commit replaced the object")
	}
	// A grant revoked mid-upload refuses the commit.
	revoked := f.startSession(creator, "auth-bucket", "&name=revoked.txt", "", nil, 200)
	f.session("", "PUT", revoked, span(0, 0, "*"), []byte("a"), 308)
	f.policy(t, "auth-bucket", creator, "")
	f.session("", "PUT", revoked, span(1, 1, "2"), []byte("b"), 403)
	f.request(admin, "GET", "/storage/v1/b/auth-bucket/o/revoked.txt", nil, 404)
}

// TestResumableRefusals covers each malformed or refused session request.
func TestResumableRefusals(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "ref-bucket", 200)
	f.bucket(peerProject, "peer-bucket", 200)
	f.request(admin, "POST", "/upload/storage/v1/b/ref-bucket/o?uploadType=media&name=exists.txt", []byte("x"), 200)
	f.startSession(admin, "ref-bucket", "&name=exists.txt&ifGenerationMatch=0", "", nil, 412)
	f.startSession(admin, "ref-bucket", "", `{"name":"a","acl":[]}`, nil, 400)
	f.startSession(admin, "ref-bucket", "", `null`, nil, 400)
	f.startSession(admin, "ref-bucket", "", "", nil, 400)
	f.startSession(admin, "ref-bucket", "&name=a", "", map[string]string{"X-Upload-Content-Length": strconv.Itoa(maxObjectSize + 1)}, 429)
	f.startSession(admin, "ref-bucket", "&name=a", "", map[string]string{"X-Upload-Content-Length": "-1"}, 400)
	location := f.startSession(admin, "ref-bucket", "&name=a.txt", "", map[string]string{"X-Upload-Content-Length": "4"}, 200)
	for _, tc := range []struct {
		name    string
		headers map[string]string
		body    string
		code    int
	}{
		{"not bytes", map[string]string{"Content-Range": "items 0-1/4"}, "ab", 400},
		{"length mismatch", span(0, 2, "4"), "ab", 400},
		{"past the total", span(0, 4, "4"), "abcde", 400},
		{"status query with a body", map[string]string{"Content-Range": "bytes */4"}, "ab", 400},
		{"gap after received", span(2, 3, "4"), "cd", 400},
		{"total changed", span(0, 1, "5"), "ab", 400},
		{"declared total exceeded", span(0, 4, "*"), "abcde", 400},
		{"total over the limit", map[string]string{"Content-Range": "bytes */" + strconv.Itoa(maxObjectSize+1)}, "", 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.t = t
			f.session("", "PUT", location, tc.headers, []byte(tc.body), tc.code)
		})
	}
	f.t = t
	// The refusals left the session usable.
	f.session("", "PUT", location, span(0, 1, "4"), []byte("ab"), 308)
	f.session("", "PUT", location, map[string]string{"Content-Range": "bytes 2-3/4", "X-Goog-Hash": "crc32c=AAAAAA=="}, []byte("cd"), 400)
	f.session("", "PUT", location, span(2, 3, "4"), []byte("cd"), 404)

	other := f.startSession(admin, "ref-bucket", "&name=b.txt", "", nil, 200)
	parsed, _ := url.Parse(other)
	id := parsed.Query().Get("upload_id")
	// A session belongs to its bucket and project.
	f.session("", "PUT", "/upload/storage/v1/b/peer-bucket/o?uploadType=resumable&upload_id="+id, nil, []byte("x"), 404)
	f.session("", "PUT", "/upload/storage/v1/b/missing-bucket/o?uploadType=resumable&upload_id="+id, nil, []byte("x"), 404)
	f.session("", "PUT", "/upload/storage/v1/b/ref-bucket/o?uploadType=resumable&upload_id=unknown", nil, []byte("x"), 404)
	f.session("", "PUT", "/upload/storage/v1/b/ref-bucket/o?uploadType=resumable", nil, []byte("x"), 400)
	f.session("", "PUT", other+"&userProject=x", nil, []byte("x"), 400)
	// Preconditions are evaluated again at commit.
	late := f.startSession(admin, "ref-bucket", "&name=late.txt&ifGenerationMatch=0", "", nil, 200)
	f.request(admin, "POST", "/upload/storage/v1/b/ref-bucket/o?uploadType=media&name=late.txt", []byte("first"), 200)
	f.session("", "PUT", late, nil, []byte("second"), 412)
	// Deleting the bucket ends its sessions.
	f.request(admin, "DELETE", "/storage/v1/b/ref-bucket/o/exists.txt", nil, 204)
	f.request(admin, "DELETE", "/storage/v1/b/ref-bucket/o/late.txt", nil, 204)
	f.request(admin, "DELETE", "/storage/v1/b/ref-bucket", nil, 204)
	f.bucket(testProject, "ref-bucket", 200)
	f.session("", "PUT", other, nil, []byte("x"), 404)
}

// TestResetEndsUploadSessions checks that a scope reset drops open sessions.
func TestResetEndsUploadSessions(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "reset-bucket", 200)
	location := f.startSession(admin, "reset-bucket", "&name=open.txt", "", nil, 200)
	scope := state.Scope{Project: testProject, Location: state.Global}
	if err := f.p.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: scope}); err != nil {
		t.Fatal(err)
	}
	f.bucket(testProject, "reset-bucket", 200)
	f.session("", "PUT", location, nil, []byte("x"), 404)
}

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
