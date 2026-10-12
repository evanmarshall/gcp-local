//go:build sdkintegration

package run

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestCloudRunPythonSDK runs the pinned google-cloud-run SDK over its default
// gRPC transport with local external-account ADC inside the loopback-only runner.
func TestCloudRunPythonSDK(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil || os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run SDK tests in the loopback-only runner (scripts/check/sdk/cloudrun.sh)")
	}
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	g := newGRPCHarness(t)
	run, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	command := exec.CommandContext(run, python, "testdata/sdk.py", g.endpoint)
	directory := t.TempDir()
	command.Env = []string{"PATH=/usr/bin:/bin", "GOOGLE_APPLICATION_CREDENTIALS=" + g.adc[ownerSubject], "GCP_LOCAL_DENIED_ADC=" + g.adc[peerSubject],
		"GOOGLE_CLOUD_PROJECT=demo-project", "CLOUDSDK_CONFIG=" + filepath.Join(directory, "empty-cloudsdk"), "GOOGLE_API_USE_CLIENT_CERTIFICATE=false",
		"GOOGLE_API_USE_MTLS_ENDPOINT=never", "GCP_LOCAL_ISOLATED=1"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("google-cloud-run SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
}
