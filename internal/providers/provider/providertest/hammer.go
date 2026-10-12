// Package providertest drives a provider through its gateway from many
// concurrent clients. Run it under -race: a handler that touches store state
// outside its lock fails the test even when every response looks right.
package providertest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
)

// Request is one REST call. Want lists every status a correct provider may
// return while other workers race on the same resource (for example 200 or
// 404 for a get that can land before the create or after the delete).
type Request struct {
	Method string
	Path   string
	Body   any // JSON-encoded when non-nil
	Want   []int
}

// Hammer runs workers goroutines that each issue script(worker) rounds times
// against handler with a bearer token. All workers are released together so
// creates, reads, updates, and deletes of the same resource overlap. A status
// outside a request's Want fails the test.
func Hammer(t *testing.T, handler http.Handler, token string, workers, rounds int, script func(worker int) []Request) {
	t.Helper()
	start := make(chan struct{})
	var group sync.WaitGroup
	for worker := range workers {
		requests := script(worker)
		group.Go(func() {
			<-start
			for range rounds {
				for _, request := range requests {
					issue(t, handler, token, request)
				}
			}
		})
	}
	close(start)
	group.Wait()
}

// issue sends one request and checks its status. It reports failures with
// Errorf because it runs off the test goroutine.
func issue(t *testing.T, handler http.Handler, token string, spec Request) {
	t.Helper()
	var body []byte
	if spec.Body != nil {
		encoded, err := json.Marshal(spec.Body)
		if err != nil {
			t.Errorf("encode %s %s: %v", spec.Method, spec.Path, err)
			return
		}
		body = encoded
	}
	request, err := http.NewRequestWithContext(context.Background(), spec.Method, spec.Path, bytes.NewReader(body))
	if err != nil {
		t.Errorf("build %s %s: %v", spec.Method, spec.Path, err)
		return
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if !slices.Contains(spec.Want, recorder.Code) {
		t.Errorf("%s %s = %d, want one of %v: %s", spec.Method, spec.Path, recorder.Code, spec.Want, recorder.Body.String())
	}
}
