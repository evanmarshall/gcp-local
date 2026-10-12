//go:build sdkintegration

package identity_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/evanmarshall/gcp-local/internal/runtime"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPythonSDKADCAndMetadataRefresh(t *testing.T) {
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" || os.Getenv("GCP_LOCAL_ISOLATED") != "1" {
		t.Fatal("Run sdkintegration with an explicit Python environment inside scripts/run/isolated.py")
	}
	for _, mode := range []string{"external", "metadata"} {
		t.Run(mode, func(t *testing.T) {
			subject := "serviceAccount:app@synthetic.iam.gserviceaccount.com"
			secret := "gls.synthetic-sdk-subject-token-for-isolated-tests"
			var seconds atomic.Int64
			service, err := identity.New(identity.Config{TokenLifetimeSeconds: 600, Principals: []identity.Enrollment{{Subject: subject, Project: "synthetic", SubjectToken: secret}}}, func() time.Time { return time.Unix(1700000000+seconds.Load(), 0) })
			if err != nil {
				t.Fatal(err)
			}
			registry := gateway.NewRegistry()
			var calls atomic.Int32
			err = registry.Register("storage.buckets.list", func(call gateway.Call, input any) (any, error) {
				if call.Principal.State != gateway.Authenticated {
					return nil, status.Error(codes.Unauthenticated, "Local test caller must authenticate")
				}
				if call.Principal.Subject != subject || call.Principal.Project != "synthetic" || call.Project != "synthetic" {
					t.Errorf("unexpected SDK identity/context: %+v", call)
				}
				calls.Add(1)
				return &gateway.RESTResponse{JSON: map[string]any{"kind": "storage#buckets", "items": []any{map[string]string{"kind": "storage#bucket", "name": "identity-fixture"}}}}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			dispatch := gateway.New(registry, service.Resolve)
			server := core.WithGateway("identity-integration", time.Second, dispatch)
			server.Identity = service
			original := server.HTTP.Handler
			server.HTTP.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Test-only clock control; absent from the production runtime.
				if r.Method == "POST" && r.URL.Path == "/advance-test-clock" {
					seconds.Add(601)
					w.WriteHeader(204)
					return
				}
				original.ServeHTTP(w, r)
			})
			handler, err := service.Metadata(subject)
			if err != nil {
				t.Fatal(err)
			}
			server.Metadata = &http.Server{Handler: handler}
			listen := func() net.Listener {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				return listener
			}
			listeners := core.Listeners{HTTP: listen(), GRPC: listen(), Metadata: listen()}
			apiURL, metadataURL := "http://"+listeners.HTTP.Addr().String(), "http://"+listeners.Metadata.Addr().String()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, listeners) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})

			directory := t.TempDir()
			subjectFile := filepath.Join(directory, "subject.txt")
			os.WriteFile(subjectFile, []byte(secret), 0600)
			profile := map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": apiURL + "/v1/token", "credential_source": map[string]string{"file": subjectFile}}
			encoded, _ := json.Marshal(profile)
			adc := filepath.Join(directory, "adc.json")
			os.WriteFile(adc, encoded, 0600)
			sdkContext, stopSDK := context.WithTimeout(context.Background(), 15*time.Second)
			defer stopSDK()
			command := exec.CommandContext(sdkContext, python, "testdata/sdk.py", mode, apiURL, metadataURL, subject)
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "CLOUDSDK_CONFIG=" + filepath.Join(directory, "empty-cloudsdk"), "GOOGLE_CLOUD_PROJECT=synthetic", "GCE_METADATA_HOST=" + strings.TrimPrefix(metadataURL, "http://"), "GCE_METADATA_IP=" + strings.TrimPrefix(metadataURL, "http://"), "GOOGLE_API_USE_CLIENT_CERTIFICATE=false", "GOOGLE_API_USE_MTLS_ENDPOINT=never"}
			if mode == "external" {
				command.Env = append(command.Env, "GOOGLE_APPLICATION_CREDENTIALS="+adc)
			}
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("SDK flow %s: %v\n%s", mode, err, output)
			}
			if calls.Load() != 2 {
				t.Fatalf("expected two authenticated Storage stub calls, got %d", calls.Load())
			}
		})
	}
}
