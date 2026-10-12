package cloudscheduler

import (
	"net/http"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

// A recreated job starts with no policy of its own: deleting the job removes
// its index entry, which drops anything bound to it, and the index entry
// leaves together with the job.
func TestRecreatedJobStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t, nil)
	job := func() map[string]any {
		return map[string]any{"schedule": "*/5 * * * *", "pubsubTarget": map[string]any{"topicName": "projects/" + testProject + "/topics/t", "attributes": map[string]any{"a": "b"}}}
	}
	h.create("nightly", job())
	name := jobResource(testProject, testLocation, "nightly")
	_, err := h.engine.SetPolicy(name, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/cloudscheduler.jobRunner", Members: []string{"serviceAccount:" + email("viewer")}},
	}})
	must(t, err)
	if code, body := h.call("actor", http.MethodDelete, basePath+"/nightly", nil); code != http.StatusOK {
		t.Fatalf("delete = %d %v", code, body)
	}
	if _, indexed := h.index.Get(name); indexed {
		t.Fatal("the deleted job is still indexed")
	}
	h.create("nightly", job())
	policy, err := h.engine.GetPolicy(name)
	must(t, err)
	if len(policy.Bindings) != 0 {
		t.Fatalf("the recreated job kept the deleted job's grants: %v", policy.Bindings)
	}
}
