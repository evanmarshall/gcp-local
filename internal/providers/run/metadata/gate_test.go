package metadata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// otherPeerIP is another workload's address on the revision network.
const otherPeerIP = "172.30.0.5"

func TestMetadataGateServesOnlyTheRevisionPeer(t *testing.T) {
	served := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("token")) })
	for peer, want := range map[string]int{"127.0.0.1": http.StatusOK, otherPeerIP: http.StatusForbidden, "": http.StatusForbidden} {
		gate := NewPeerGate(served)
		gate.Allow(peer)
		request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/computeMetadata/v1/", nil)
		request.RemoteAddr = "127.0.0.1:40000"
		recorder := httptest.NewRecorder()
		gate.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Errorf("peer %q: status %d, want %d", peer, recorder.Code, want)
		}
	}
}
