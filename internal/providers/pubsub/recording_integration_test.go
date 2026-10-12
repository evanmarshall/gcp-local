//go:build pubsubintegration

package pubsub

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// recorderRun starts the provider with the scenario's fixture, allowed, and
// denied identities and writes their local ADC bundle. Only fixture is a
// project-level Pub/Sub admin.
func recorderRun(t *testing.T) (*integration, string, string) {
	t.Helper()
	var enrollments []identity.Enrollment
	for _, actor := range []string{"fixture", "allowed", "denied"} {
		enrollments = append(enrollments, identity.Enrollment{Subject: "serviceAccount:" + actor + "@project-a.iam.gserviceaccount.com", Project: "project-a", SubjectToken: subjectSecret + "-recorder-" + actor})
	}
	f := running(t, enrollments...)
	_, err := f.p.iam.SetPolicy(provider.ProjectResource("project-a"), iam.Policy{Bindings: []iam.Binding{{Role: "roles/pubsub.admin", Members: []string{owner, enrollments[0].Subject}}}})
	must(t, err)
	directory := t.TempDir()
	profiles := map[string]any{}
	for i, actor := range []string{"fixture", "allowed", "denied"} {
		home := filepath.Join(directory, actor)
		must(t, os.Mkdir(home, 0700))
		subject := filepath.Join(home, "subject.txt")
		must(t, os.WriteFile(subject, []byte(enrollments[i].SubjectToken), 0600))
		profile := map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": f.http + "/v1/token", "credential_source": map[string]string{"file": subject}}
		raw, err := json.Marshal(profile)
		must(t, err)
		adc := filepath.Join(home, "adc.json")
		must(t, os.WriteFile(adc, raw, 0600))
		profiles[actor] = map[string]string{"email": actor + "@project-a.iam.gserviceaccount.com", "adc": adc}
	}
	raw, err := json.Marshal(map[string]any{"project": "project-a", "identities": profiles})
	must(t, err)
	path := filepath.Join(directory, "identities.json")
	must(t, os.WriteFile(path, raw, 0600))
	return f, path, directory
}

// runScenarioScript runs one scenario driver from testdata with the repository on PYTHONPATH.
func runScenarioScript(t *testing.T, f *integration, script string, args ...string) {
	t.Helper()
	root, err := filepath.Abs("../../..")
	must(t, err)
	command := exec.CommandContext(f.owner, os.Getenv("GCP_LOCAL_TEST_PYTHON"), append([]string{script, "http://" + f.endpoint}, args...)...)
	command.Env = []string{"PATH=/usr/bin:/bin", "PYTHONPATH=" + root, "GCP_LOCAL_ISOLATED=1", "GOOGLE_API_USE_CLIENT_CERTIFICATE=false", "GOOGLE_API_USE_MTLS_ENDPOINT=never"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", script, err, output)
	}
	t.Log(string(output))
}

func TestBoundedRecorderThroughProductionLocalADC(t *testing.T) {
	f, identities, directory := recorderRun(t)
	runScenarioScript(t, f, "testdata/recording.py", identities, filepath.Join(directory, "state"))
}

// TestDeliveryScenarioThroughProductionLocalADC runs the
// pubsub-delivery-features scenario locally and compares it with its GCP
// recording.
func TestDeliveryScenarioThroughProductionLocalADC(t *testing.T) {
	f, identities, directory := recorderRun(t)
	runScenarioScript(t, f, "testdata/delivery_recording.py", identities, filepath.Join(directory, "state"), "123456")
}
