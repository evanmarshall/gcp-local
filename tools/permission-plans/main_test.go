package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestRunPrintsSecretManagerPlans checks the export the drift report reads.
func TestRunPrintsSecretManagerPlans(t *testing.T) {
	var output, errors bytes.Buffer
	if code := run([]string{"-service", "secretmanager"}, &output, &errors); code != 0 {
		t.Fatalf("exit %d: %s", code, errors.String())
	}
	var value struct {
		Plans map[string]plan `json:"plans"`
	}
	if err := json.Unmarshal(output.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	access := value.Plans["secretmanager.versions.access"]
	if len(access.Baseline) != 1 || access.Baseline[0] != "secretmanager.versions.access" {
		t.Fatalf("unexpected access plan: %+v", access)
	}
	if code := run([]string{"-service", "storage"}, &output, &errors); code != 2 {
		t.Fatalf("unsupported service exit %d", code)
	}
}
