package cloudkms

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// TestPatchIsAtomic rejects a mask with one bad path without applying the
// good one.
func TestPatchIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.ok(h.invoke(http.MethodPost, testRingsPath+"?keyRingId="+testRing, map[string]any{}), "ring create")
	h.ok(h.invoke(http.MethodPost, testKeysPath+"?cryptoKeyId="+testKey, map[string]any{"labels": map[string]string{"phase": "before"}}), "key create")
	recorder := h.invoke(http.MethodPatch, testKeyPath+"?updateMask=labels,bogus", map[string]any{"labels": map[string]string{"phase": "after"}})
	if recorder.Code != 400 {
		t.Fatalf("bad-mask patch status = %d", recorder.Code)
	}
	if body := h.invoke(http.MethodGet, testKeyPath, nil).Body.String(); !strings.Contains(body, `"phase":"before"`) {
		t.Fatalf("a rejected patch changed the key: %s", body)
	}
}

// TestConcurrentKeyRequests races key ring and crypto key creation, key
// patches, version creation and state changes, and encryption on one key;
// -race fails it if a handler touches a record outside the scope lock.
func TestConcurrentKeyRequests(t *testing.T) {
	h := newHarness(t)
	if _, err := h.provider.engine.SetPolicy(provider.ProjectResource(testProject), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/cloudkms.admin", Members: []string{member(testFixture)}},
		{Role: testEncrypter, Members: []string{member(testFixture)}},
	}}); err != nil {
		t.Fatalf("grant the encrypter role: %v", err)
	}
	version := testVersions + "/1"
	plaintext := base64.StdEncoding.EncodeToString([]byte(testPlaintext))
	providertest.Hammer(t, h.gate, testFixture, 8, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testRingsPath + "?keyRingId=" + testRing, Body: map[string]any{}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testRingPath, Want: []int{200}},
			{Method: http.MethodPost, Path: testKeysPath + "?cryptoKeyId=" + testKey, Body: map[string]any{}, Want: []int{200, 409}},
			{Method: http.MethodGet, Path: testKeyPath, Want: []int{200}},
			{Method: http.MethodGet, Path: testKeysPath, Want: []int{200}},
			{Method: http.MethodPatch, Path: testKeyPath + "?updateMask=labels", Body: map[string]any{"labels": map[string]string{"worker": "w"}}, Want: []int{200}},
			{Method: http.MethodPost, Path: testVersions, Body: map[string]any{}, Want: []int{200}},
			{Method: http.MethodGet, Path: testVersions, Want: []int{200}},
			{Method: http.MethodPatch, Path: version + "?updateMask=state", Body: map[string]any{"state": "DISABLED"}, Want: []int{200, 400}},
			{Method: http.MethodPost, Path: testKeyPath + ":encrypt", Body: map[string]any{"plaintext": plaintext}, Want: []int{200, 400}},
			{Method: http.MethodPatch, Path: version + "?updateMask=state", Body: map[string]any{"state": "ENABLED"}, Want: []int{200, 400}},
			{Method: http.MethodPost, Path: version + ":destroy", Body: map[string]any{}, Want: []int{200, 400}},
			{Method: http.MethodPost, Path: version + ":restore", Body: map[string]any{}, Want: []int{200, 400}},
			{Method: http.MethodGet, Path: version, Want: []int{200}},
		}
	})
	// Fresh keys race their own creation against patches and new versions,
	// so a create that renders its record after unlocking is caught.
	providertest.Hammer(t, h.gate, testFixture, 8, 1, func(int) []providertest.Request {
		requests := make([]providertest.Request, 0, 60)
		for round := range 20 {
			key := testKeysPath + "/fresh-" + strconv.Itoa(round)
			requests = append(requests,
				providertest.Request{Method: http.MethodPost, Path: testKeysPath + "?cryptoKeyId=fresh-" + strconv.Itoa(round), Body: map[string]any{}, Want: []int{200, 409}},
				providertest.Request{Method: http.MethodPatch, Path: key + "?updateMask=labels", Body: map[string]any{"labels": map[string]string{"round": "r"}}, Want: []int{200}},
				providertest.Request{Method: http.MethodPost, Path: key + "/cryptoKeyVersions", Body: map[string]any{}, Want: []int{200}},
			)
		}
		return requests
	})
}
