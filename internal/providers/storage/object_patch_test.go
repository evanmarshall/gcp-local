package storage

import (
	"strings"
	"testing"
)

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
