package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/localtls"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const tlsProject = "tls-project"

// writeTLSFixture enrolls one granted service account and returns the core flags.
func writeTLSFixture(t *testing.T, dir, subjectToken string) []string {
	t.Helper()
	member := "serviceAccount:app@" + tlsProject + ".iam.gserviceaccount.com"
	resource := "//cloudresourcemanager.googleapis.com/projects/" + tlsProject
	files := map[string]any{
		"server.json": map[string]any{"principals": []any{map[string]any{"subject": member, "project": tlsProject, "subject_token": subjectToken}}},
		"iam.json": map[string]any{
			"resources": []any{map[string]any{"name": resource, "service": "resourcemanager", "kind": "project", "number": "424242424242", "scope": map[string]any{"project": tlsProject, "location": "global"}}},
			"policies":  map[string]any{resource: map[string]any{"bindings": []any{map[string]any{"role": "roles/secretmanager.admin", "members": []any{member}}}}},
		},
	}
	for name, value := range files {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return []string{
		"-listen=127.0.0.1:0", "-grpc-listen=127.0.0.1:0", "-grpc-tls-listen=127.0.0.1:0",
		"-grpc-tls-dir=" + filepath.Join(dir, "tls"), "-services=secretmanager",
		"-identity-config=" + filepath.Join(dir, "server.json"), "-iam-config=" + filepath.Join(dir, "iam.json"),
	}
}

// exchangeToken trades the local subject token for an access token at /v1/token.
func exchangeToken(t *testing.T, endpoint, subjectToken string) string {
	t.Helper()
	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "audience": {identity.Audience},
		"requested_token_type": {identity.AccessTokenType}, "subject_token_type": {identity.AccessTokenType},
		"subject_token": {subjectToken}, "scope": {"https://www.googleapis.com/auth/cloud-platform"},
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/v1/token", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token exchange: %d %v", response.StatusCode, err)
	}
	return body.AccessToken
}

// createSecret calls CreateSecret over connection, with a bearer token when one is given.
func createSecret(connection *grpc.ClientConn, id, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	_, err := secretmanagerpb.NewSecretManagerServiceClient(connection).CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/" + tlsProject, SecretId: id,
		Secret: &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}}}},
	})
	return err //nolint:wrapcheck // the test compares gRPC status codes
}

func dial(t *testing.T, address string, transport credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(transport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

// serveTLSFixture starts the core with identity, IAM, Secret Manager, and the
// TLS listener, and returns its listeners and the environment's CA pool.
func serveTLSFixture(t *testing.T, subject string) (core.Listeners, *x509.CertPool) {
	t.Helper()
	dir := t.TempDir()
	options, ok := parseServeFlags(writeTLSFixture(t, dir, subject), &bytes.Buffer{})
	if !ok {
		t.Fatal("flags refused")
	}
	server, listeners, err := assemble(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listeners) }()
	t.Cleanup(func() { cancel(); <-served })
	root, err := os.OpenRoot(filepath.Join(dir, "tls"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	authority, err := root.ReadFile(localtls.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(authority)
	return listeners, roots
}

// checkAuth requires anonymous and forged callers to be refused and the
// granted token to succeed on connection.
func checkAuth(t *testing.T, name string, connection *grpc.ClientConn, token string) {
	t.Helper()
	if err := createSecret(connection, name+"-anonymous", ""); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("%s anonymous: %v", name, err)
	}
	if err := createSecret(connection, name+"-forged", "forged"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("%s unknown token: %v", name, err)
	}
	if err := createSecret(connection, name+"-granted", token); err != nil {
		t.Fatalf("%s granted: %v", name, err)
	}
}

// The TLS listener is the plaintext gateway's gRPC server behind a TLS
// handshake: the same identity resolution and IAM decide every call.
func TestGRPCTLSListenerEnforcesTheSameAuth(t *testing.T) {
	subject := "gls.tls-test-subject-0123456789abcdefghijklmnop"
	listeners, roots := serveTLSFixture(t, subject)
	tlsAddress := listeners.GRPCTLS.Addr().String()
	token := exchangeToken(t, "http://"+listeners.HTTP.Addr().String(), subject)
	checkAuth(t, "tls", dial(t, tlsAddress, credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})), token)
	checkAuth(t, "plaintext", dial(t, listeners.GRPC.Addr().String(), insecure.NewCredentials()), token)
	// A client that does not trust this environment's CA, or skips TLS, never reaches a handler.
	untrusted := dial(t, tlsAddress, credentials.NewTLS(&tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}))
	if err := createSecret(untrusted, "untrusted", token); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("untrusted CA: %v", err)
	}
	if err := createSecret(dial(t, tlsAddress, insecure.NewCredentials()), "cleartext", token); status.Code(err) != codes.Unavailable {
		t.Fatalf("plaintext to the TLS port: %v", err)
	}
}

func TestGRPCTLSFlagsGoTogether(t *testing.T) {
	for _, args := range [][]string{{"-grpc-tls-listen=127.0.0.1:0"}, {"-grpc-tls-dir=/tmp/x"}} {
		if _, ok := parseServeFlags(args, &bytes.Buffer{}); ok {
			t.Fatalf("%v accepted", args)
		}
	}
}
