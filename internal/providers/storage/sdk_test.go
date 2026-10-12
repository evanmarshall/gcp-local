package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// bearer adds a fixture token to every request, as an authorized transport does.
type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r) //nolint:wrapcheck // test transport
}

// goClient serves the fixture over HTTP and returns a Go Storage client with
// default settings and the documented endpoint override.
func goClient(t *testing.T, f *fixture, token string) *gcs.Client {
	t.Helper()
	server := httptest.NewServer(f.g)
	t.Cleanup(server.Close)
	client, err := gcs.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: bearer{token}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniformBucket is the bucket shape the provider accepts.
func uniformBucket() *gcs.BucketAttrs {
	return &gcs.BucketAttrs{Location: "US-CENTRAL1", UniformBucketLevelAccess: gcs.UniformBucketLevelAccess{Enabled: true},
		PublicAccessPrevention: gcs.PublicAccessPreventionEnforced, SoftDeletePolicy: &gcs.SoftDeletePolicy{RetentionDuration: 0}}
}

// TestGoClientBucketAndObjectLifecycle drives cloud.google.com/go/storage at
// default settings: it sends projection=full on reads, lists, and writes.
func TestGoClientBucketAndObjectLifecycle(t *testing.T) {
	f := setup(t)
	client := goClient(t, f, admin)
	ctx := context.Background()
	bucket := client.Bucket("go-bucket")
	if err := bucket.Create(ctx, testProject, uniformBucket()); err != nil {
		t.Fatal(err)
	}
	attrs, err := bucket.Attrs(ctx)
	if err != nil || attrs.Name != "go-bucket" || !attrs.UniformBucketLevelAccess.Enabled {
		t.Fatalf("bucket attrs = %+v, %v", attrs, err)
	}
	if listed, err := client.Buckets(ctx, testProject).Next(); err != nil || listed.Name != "go-bucket" {
		t.Fatalf("bucket list = %+v, %v", listed, err)
	}
	goObjectLifecycle(t, bucket)
	if err := bucket.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Buckets(ctx, testProject).Next(); !errors.Is(err, iterator.Done) {
		t.Fatalf("bucket list after delete: %v", err)
	}
}

// goObjectLifecycle writes, reads, lists, and deletes one object.
func goObjectLifecycle(t *testing.T, bucket *gcs.BucketHandle) {
	t.Helper()
	ctx := context.Background()
	handle := bucket.Object("dir/small.txt")
	writer := handle.NewWriter(ctx)
	writer.ContentType = "text/plain"
	if _, err := writer.Write([]byte("small object")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	goObjectReads(t, bucket, handle)
	goObjectUpdate(t, handle)
	if got := readAll(t, handle); string(got) != "small object" {
		t.Fatalf("read = %q", got)
	}
	if err := handle.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Attrs(ctx); !errors.Is(err, gcs.ErrObjectNotExist) {
		t.Fatalf("deleted object attrs: %v", err)
	}
}

// goObjectReads checks the object's attributes and listing.
func goObjectReads(t *testing.T, bucket *gcs.BucketHandle, handle *gcs.ObjectHandle) {
	t.Helper()
	ctx := context.Background()
	object, err := handle.Attrs(ctx)
	if err != nil || object.Size != 12 || object.ContentType != "text/plain" || !strings.HasPrefix(object.MediaLink, "http://127.0.0.1:") {
		t.Fatalf("object attrs = %+v, %v", object, err)
	}
	if listed, err := bucket.Objects(ctx, nil).Next(); err != nil || listed.Name != "dir/small.txt" {
		t.Fatalf("object list = %+v, %v", listed, err)
	}
}

// goObjectUpdate patches the object's metadata with ObjectHandle.Update under
// a metageneration condition, which then no longer holds.
func goObjectUpdate(t *testing.T, handle *gcs.ObjectHandle) {
	t.Helper()
	ctx := context.Background()
	conditional := handle.If(gcs.Conditions{MetagenerationMatch: 1})
	update := gcs.ObjectAttrsToUpdate{ContentType: "text/markdown", CacheControl: "no-cache", Metadata: map[string]string{"k": "v"}}
	updated, err := conditional.Update(ctx, update)
	if err != nil || updated.ContentType != "text/markdown" || updated.CacheControl != "no-cache" || updated.Metadata["k"] != "v" || updated.Metageneration != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	var failed *googleapi.Error
	if _, err := conditional.Update(ctx, update); !errors.As(err, &failed) || failed.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale update: %v", err)
	}
}

// readAll reads an object through the Go client, which reads over the XML API.
func readAll(t *testing.T, object *gcs.ObjectHandle) []byte {
	t.Helper()
	reader, err := object.NewReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestGoClientResumableUploadAndRewrite writes 20 MiB in 8 MiB chunks, reads
// it back over the XML API, and copies it with objects.rewrite.
func TestGoClientResumableUploadAndRewrite(t *testing.T) {
	f := setup(t)
	client := goClient(t, f, admin)
	ctx := context.Background()
	bucket := client.Bucket("go-large")
	if err := bucket.Create(ctx, testProject, uniformBucket()); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 20<<20)
	_, _ = rand.Read(payload)
	writer := bucket.Object("large.bin").NewWriter(ctx)
	writer.ChunkSize = 8 << 20
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if writer.Attrs().Size != int64(len(payload)) {
		t.Fatalf("uploaded size = %d", writer.Attrs().Size)
	}
	if !bytes.Equal(readAll(t, bucket.Object("large.bin")), payload) {
		t.Fatal("resumable upload did not round-trip")
	}
	copied, err := bucket.Object("copy.bin").CopierFrom(bucket.Object("large.bin")).Run(ctx)
	if err != nil || copied.Size != int64(len(payload)) {
		t.Fatalf("rewrite = %+v, %v", copied, err)
	}
	if !bytes.Equal(readAll(t, bucket.Object("copy.bin")), payload) {
		t.Fatal("rewrite did not copy the media")
	}
}

// TestGoClientGapFailsPromptly checks that an unimplemented Storage method
// answers 400, which the Go client does not retry, instead of hanging it.
func TestGoClientGapFailsPromptly(t *testing.T) {
	f := setup(t)
	client := goClient(t, f, admin)
	f.bucket(testProject, "gap-bucket", 200)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	_, err := client.Bucket("gap-bucket").ACL().List(ctx)
	if err == nil || !strings.Contains(err.Error(), "Storage coverage gap") || time.Since(started) > 5*time.Second {
		t.Fatalf("ACL list after %s: %v", time.Since(started), err)
	}
}

// signer is one service-account key published for verification.
type signer struct {
	email string
	pem   []byte
}

func newSigner(t *testing.T, f *fixture, account string) signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	email := account + "@" + testProject + ".iam.gserviceaccount.com"
	f.p.iam.PublishSigningKey("iam", state.Scope{Project: testProject, Location: state.Global}, email, "key-1", &key.PublicKey)
	return signer{email: email, pem: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})}
}

// sign makes a one-minute V4 signed URL with the Go client's signer.
func (s signer) sign(t *testing.T, server *httptest.Server, method, bucket, object, contentType string) string {
	t.Helper()
	signed, err := gcs.SignedURL(bucket, object, &gcs.SignedURLOptions{
		GoogleAccessID: s.email, PrivateKey: s.pem, Method: method, Expires: time.Now().Add(time.Minute),
		Scheme: gcs.SigningSchemeV4, Insecure: true, Hostname: strings.TrimPrefix(server.URL, "http://"), ContentType: contentType,
	})
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// reply is one HTTP response read whole.
type reply struct {
	header http.Header
	body   []byte
}

// send makes one request and requires its status.
func send(t *testing.T, method, target string, headers map[string]string, body []byte, expected int) reply {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expected {
		t.Fatalf("%s %s: got %d want %d: %s", method, target, response.StatusCode, expected, payload)
	}
	return reply{header: response.Header, body: payload}
}

// withQuery sets one query option of a URL, or removes it when value is empty.
func withQuery(t *testing.T, raw, key, value string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Del(key)
	if value != "" {
		query.Set(key, value)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// xmlCode extracts an XML API error code.
func xmlCode(t *testing.T, payload []byte) string {
	t.Helper()
	start, end := bytes.Index(payload, []byte("<Code>")), bytes.Index(payload, []byte("</Code>"))
	if start < 0 || end < start {
		t.Fatalf("not an XML error: %s", payload)
	}
	return string(payload[start+6 : end])
}

// TestSignedURLsAuthorizeAsTheSigner drives Go-client V4 signed URLs: the
// signer's own permissions authorize the read and write.
func TestSignedURLsAuthorizeAsTheSigner(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "signed-bucket", 200)
	server := httptest.NewServer(f.g)
	t.Cleanup(server.Close)
	key := newSigner(t, f, "signer")
	f.policy(t, "signed-bucket", key.email, "roles/storage.objectUser")
	send(t, http.MethodPut, key.sign(t, server, http.MethodPut, "signed-bucket", "dir/a b.txt", "text/plain"),
		map[string]string{"Content-Type": "text/plain"}, []byte("signed body"), 200)
	got := send(t, http.MethodGet, key.sign(t, server, http.MethodGet, "signed-bucket", "dir/a b.txt", ""), nil, nil, 200)
	if string(got.body) != "signed body" || got.header.Get("Content-Type") != "text/plain" {
		t.Fatalf("signed read = %q (%s)", got.body, got.header.Get("Content-Type"))
	}
	read := key.sign(t, server, http.MethodGet, "signed-bucket", "dir/a b.txt", "")
	// Revoking the signer's grant refuses its still-valid signature.
	f.policy(t, "signed-bucket", key.email, "roles/storage.objectViewer")
	denied := send(t, http.MethodPut, key.sign(t, server, http.MethodPut, "signed-bucket", "new.txt", ""), nil, []byte("x"), 403)
	if xmlCode(t, denied.body) != "AccessDenied" || !strings.Contains(string(denied.body), key.email) {
		t.Fatalf("denial = %s", denied.body)
	}
	send(t, http.MethodGet, read, nil, nil, 200)
	f.policy(t, "signed-bucket", key.email, "")
	send(t, http.MethodGet, read, nil, nil, 403)
	// A deleted key no longer verifies.
	f.policy(t, "signed-bucket", key.email, "roles/storage.objectViewer")
	f.p.iam.ForgetSigningKeys(key.email, "key-1")
	if code := xmlCode(t, send(t, http.MethodGet, read, nil, nil, 403).body); code != "SignatureDoesNotMatch" {
		t.Fatalf("code = %s", code)
	}
	// A signed request for a missing key or bucket answers in XML.
	key = newSigner(t, f, "signer")
	if code := xmlCode(t, send(t, http.MethodGet, key.sign(t, server, http.MethodGet, "signed-bucket", "absent.txt", ""), nil, nil, 404).body); code != "NoSuchKey" {
		t.Fatalf("code = %s", code)
	}
	if code := xmlCode(t, send(t, http.MethodGet, key.sign(t, server, http.MethodGet, "no-such-bucket", "x", ""), nil, nil, 404).body); code != "NoSuchBucket" {
		t.Fatalf("code = %s", code)
	}
}

// TestSignedURLRefusals refuses tampered, rescoped, and malformed signatures.
func TestSignedURLRefusals(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "signed-bucket", 200)
	server := httptest.NewServer(f.g)
	t.Cleanup(server.Close)
	key := newSigner(t, f, "signer")
	f.policy(t, "signed-bucket", key.email, "roles/storage.objectUser")
	f.request(admin, "POST", "/upload/storage/v1/b/signed-bucket/o?uploadType=media&name=a%20b.txt", []byte("x"), 200)
	read := key.sign(t, server, http.MethodGet, "signed-bucket", "a b.txt", "")
	send(t, http.MethodGet, read, nil, nil, 200)
	for _, tc := range []struct {
		name, method, target, code string
		status                     int
	}{
		{"tampered signature", http.MethodGet, withQuery(t, read, "X-Goog-Signature", strings.Repeat("ab", 256)), "SignatureDoesNotMatch", 403},
		{"undecodable signature", http.MethodGet, withQuery(t, read, "X-Goog-Signature", "zz"), "SignatureDoesNotMatch", 403},
		{"other object", http.MethodGet, strings.Replace(read, "a%20b.txt", "other.txt", 1), "SignatureDoesNotMatch", 403},
		{"other method", http.MethodPut, read, "SignatureDoesNotMatch", 403},
		{"other service scope", http.MethodGet, strings.Replace(read, "%2Fstorage%2F", "%2Fs3%2F", 1), "InvalidArgument", 400},
		{"HMAC algorithm", http.MethodGet, strings.Replace(read, "GOOG4-RSA-SHA256", "GOOG4-HMAC-SHA256", 1), "InvalidArgument", 400},
		{"no signed host", http.MethodGet, strings.Replace(read, "X-Goog-SignedHeaders=host", "X-Goog-SignedHeaders=content-type", 1), "InvalidArgument", 400},
		{"missing parameter", http.MethodGet, withQuery(t, read, "X-Goog-Expires", ""), "InvalidArgument", 400},
		{"unsigned extra parameter", http.MethodGet, withQuery(t, read, "generation", "1"), "SignatureDoesNotMatch", 403},
		{"unknown signer", http.MethodGet, strings.Replace(read, "signer%40", "nobody%40", 1), "SignatureDoesNotMatch", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := xmlCode(t, send(t, tc.method, tc.target, nil, nil, tc.status).body); code != tc.code {
				t.Fatalf("code = %s", code)
			}
		})
	}
	// Both a signature and a bearer token is ambiguous.
	send(t, http.MethodGet, read, map[string]string{"Authorization": "Bearer " + admin}, nil, http.StatusBadRequest)
}

// TestSignedURLWindow checks the expiry and clock-skew bounds directly.
func TestSignedURLWindow(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		date, expires, code string
	}{
		{"20261011T113000Z", "60", "ExpiredToken"},
		{"20261011T130000Z", "60", "InvalidArgument"},
		{"20261011T115959Z", "604801", "InvalidArgument"},
		{"2026-10-11", "60", "InvalidArgument"},
	} {
		query := map[string][]string{"X-Goog-Date": {tc.date}, "X-Goog-Expires": {tc.expires}}
		if _, refused := signedWindow(query, now); refused == nil {
			t.Fatalf("%s/%s accepted", tc.date, tc.expires)
		} else if body, _ := io.ReadAll(refused.Body); !bytes.Contains(body, []byte("<Code>"+tc.code+"</Code>")) {
			t.Fatalf("%s/%s: %s", tc.date, tc.expires, body)
		}
	}
	if _, refused := signedWindow(map[string][]string{"X-Goog-Date": {"20261011T115930Z"}, "X-Goog-Expires": {"60"}}, now); refused != nil {
		t.Fatal("valid window refused")
	}
}
