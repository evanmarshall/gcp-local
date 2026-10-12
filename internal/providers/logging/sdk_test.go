//go:build sdkintegration

package logging

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// TestCloudLoggingSDK runs the pinned google-cloud-logging SDK over REST and gRPC with
// local external-account ADC inside the loopback-only runner.
func TestCloudLoggingSDK(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil || os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run SDK tests in the loopback-only runner (scripts/check_logging.sh)")
	}
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	const owner, other = "serviceAccount:app@project-a.iam.gserviceaccount.com", "serviceAccount:peer@project-a.iam.gserviceaccount.com"
	const secret = "gls.synthetic-logging-sdk-subject-token"
	registry, index := gateway.NewRegistry(), state.NewIndex()
	project := "//cloudresourcemanager.googleapis.com/projects/project-a"
	if err := index.Put("resourcemanager", state.Resource{Name: project, Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: "project-a", Location: state.Global}}); err != nil {
		t.Fatal(err)
	}
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{{Role: "roles/logging.admin", Members: []string{owner}}}}); err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: owner, Project: "project-a", SubjectToken: secret},
		{Subject: other, Project: "project-a", SubjectToken: secret + "-peer"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("logging-sdk", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), registry, server.Backends, index, engine); err != nil {
		t.Fatal(err)
	}
	listen := func() net.Listener {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	listeners := core.Listeners{HTTP: listen(), GRPC: listen()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listeners) }()
	t.Cleanup(func() { cancel(); <-done })
	endpoint := "http://" + listeners.HTTP.Addr().String()

	directory := t.TempDir()
	adc := func(name, token string) string {
		subject := filepath.Join(directory, name+".txt")
		if err := os.WriteFile(subject, []byte(token), 0o600); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": endpoint + "/v1/token", "credential_source": map[string]string{"file": subject}})
		path := filepath.Join(directory, name+"-adc.json")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	run, cancelRun := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelRun()
	command := exec.CommandContext(run, python, "testdata/sdk.py", endpoint, listeners.GRPC.Addr().String())
	command.Env = []string{"PATH=/usr/bin:/bin", "GOOGLE_APPLICATION_CREDENTIALS=" + adc("owner", secret), "GCP_LOCAL_DENIED_ADC=" + adc("peer", secret+"-peer"),
		"GOOGLE_CLOUD_PROJECT=project-a", "CLOUDSDK_CONFIG=" + filepath.Join(directory, "empty-cloudsdk"), "GOOGLE_API_USE_CLIENT_CERTIFICATE=false",
		"GOOGLE_API_USE_MTLS_ENDPOINT=never", "GCP_LOCAL_ISOLATED=1"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("google-cloud-logging SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
}
