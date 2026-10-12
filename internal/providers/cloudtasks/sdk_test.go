//go:build sdkintegration

package cloudtasks

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestCloudTasksPythonSDK runs the pinned google-cloud-tasks client (v2) at
// its default gRPC transport with local external-account ADC inside the
// loopback-only runner (scripts/check_cloudtasks.sh).
func TestCloudTasksPythonSDK(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil || os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run SDK tests in the loopback-only runner (scripts/check_cloudtasks.sh)")
	}
	python := os.Getenv("GCP_LOCAL_TEST_PYTHON")
	if python == "" {
		t.Fatal("Explicit pinned Python environment is required")
	}
	const subject = "gls.synthetic-cloudtasks-sdk-subject"
	listeners := startRuntime(t, subject)
	sts := "http://" + listeners.HTTP.Addr().String()
	run, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(run, python, "testdata/sdk.py", listeners.GRPC.Addr().String())
	command.Env = []string{"PATH=/usr/bin:/bin", "GOOGLE_APPLICATION_CREDENTIALS=" + writeADC(t, sts, subject), "GCP_LOCAL_DENIED_ADC=" + writeADC(t, sts, subject+"-peer"),
		"GOOGLE_CLOUD_PROJECT=project-a", "CLOUDSDK_CONFIG=" + filepath.Join(t.TempDir(), "empty-cloudsdk"), "GOOGLE_API_USE_CLIENT_CERTIFICATE=false",
		"GOOGLE_API_USE_MTLS_ENDPOINT=never", "GCP_LOCAL_ISOLATED=1"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("google-cloud-tasks SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
}
