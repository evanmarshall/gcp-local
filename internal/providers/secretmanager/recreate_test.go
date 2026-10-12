package secretmanager

import (
	"net/http"
	"strings"
	"testing"
)

// A deleted secret's grants must not reach a new secret of the same name: GCP
// starts every created resource with an empty policy.
func TestRecreatedSecretStartsWithEmptyPolicy(t *testing.T) {
	h := newHarness(t)
	h.create(replicationAuto())
	grantAccessor(t, h)
	if recorder := h.invoke(http.MethodDelete, "/v1/projects/"+testProject+"/secrets/"+testSecret, nil, "actor"); recorder.Code != 200 {
		t.Fatalf("delete status = %d body %s", recorder.Code, recorder.Body.String())
	}
	h.create(replicationAuto())
	h.addVersion(t, "fresh")
	policy := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+":getIamPolicy", nil, "actor")
	if policy.Code != 200 || strings.Contains(policy.Body.String(), "secretAccessor") {
		t.Fatalf("recreated secret policy = %d %s", policy.Code, policy.Body.String())
	}
	if recorder := h.invoke(http.MethodGet, "/v1/projects/"+testProject+"/secrets/"+testSecret+"/versions/1:access", nil, testAccessor); recorder.Code != 403 {
		t.Fatalf("former accessor reads the recreated secret: %d %s", recorder.Code, recorder.Body.String())
	}
}
