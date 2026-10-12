package run

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/identity"
)

// Test subject identity for the revision metadata listener.
const (
	testProjectName   = "demo-project"
	testWorkerSubject = "serviceAccount:worker@demo-project.iam.gserviceaccount.com"
	testWorkerToken   = "gls.revision-metadata-subject-token-for-refresh-tests" //nolint:gosec // synthetic enrollment secret
)

// TestRevisionMetadataServesConfiguredIdentity proves the per-revision metadata
// listener mints tokens for the revision's configured service account only.
func TestRevisionMetadataServesConfiguredIdentity(t *testing.T) {
	subject := testWorkerSubject
	secret := testWorkerToken
	identities, err := identity.New(identity.Config{
		TokenLifetimeSeconds: 300,
		Principals:           []identity.Enrollment{{Subject: subject, Project: testProjectName, SubjectToken: secret}},
	}, nil)
	if err != nil {
		t.Fatalf("build the identity service: %v", err)
	}
	h := newHarness(t)
	h.declareWorker(t)
	h.provider.metadata = identities.Metadata
	h.provider.metabind = func(handler http.Handler) (string, func(), error) {
		var listen net.ListenConfig
		listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return "", nil, fmt.Errorf("bind the test metadata listener: %w", err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
		go func() { _ = server.Serve(listener) }()
		return listener.Addr().String(), func() { _ = server.Close() }, nil
	}
	body := map[string]any{
		keyName: serviceReference,
		"template": map[string]any{
			"containers":     []any{map[string]any{"image": imageReference}},
			"serviceAccount": strings.TrimPrefix(subject, "serviceAccount:"),
		},
	}
	recorder := h.invoke(http.MethodPost, "/v2/projects/demo-project/locations/us-central1/services", body)
	if recorder.Code != 200 {
		t.Fatalf("create status = %d body %s", recorder.Code, recorder.Body.String())
	}
	metadata := boundMetadataHost(h)
	principal := fetchResolvedPrincipal(t, metadata, identities)
	if principal.State != gateway.Authenticated || principal.Subject != subject {
		t.Fatalf("resolved principal = %+v", principal)
	}
}

// boundMetadataHost extracts the revision's metadata listener address.
func boundMetadataHost(h *harness) string {
	for _, entry := range h.runtime.started[0].Env {
		if value, found := strings.CutPrefix(entry, "GCE_METADATA_HOST="); found {
			return value
		}
	}
	h.t.Fatal("no metadata listener bound for the revision")
	return ""
}

// fetchResolvedPrincipal mints and resolves one workload token.
func fetchResolvedPrincipal(t *testing.T, metadata string, identities *identity.Service) gateway.Principal {
	t.Helper()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://"+metadata+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
	request.Header.Set("Metadata-Flavor", "Google")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("metadata token request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, _ := io.ReadAll(response.Body)
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(payload, &token); err != nil {
		t.Fatalf("decode token response %s: %v", payload, err)
	}
	principal, err := identities.Resolve(context.Background(), token.AccessToken)
	if err != nil {
		t.Fatalf("resolve the minted token: %v", err)
	}
	return principal
}
