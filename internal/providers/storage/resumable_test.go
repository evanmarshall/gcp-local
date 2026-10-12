package storage

import (
	"bytes"
	"context"
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
