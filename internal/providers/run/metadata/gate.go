// Package metadata gates a revision's metadata listener to that revision.
package metadata

import (
	"net"
	"net/http"
	"time"
)

// peerWait bounds how long a metadata request waits for its revision's
// address; the listener is bound before the container starts.
const peerWait = 10 * time.Second

// PeerGate serves one revision's metadata handler only to that revision. Each
// revision gets its own listener, but every workload on the network can reach
// every listener, so the gate checks the caller's address against the peer
// the runtime reported after start.
type PeerGate struct {
	next  http.Handler
	ready chan struct{}
	peer  net.IP
}

// NewPeerGate wraps one revision's metadata handler; it waits for Allow.
func NewPeerGate(next http.Handler) *PeerGate {
	return &PeerGate{next: next, ready: make(chan struct{})}
}

// Allow records the revision's address and releases waiting requests. An
// unparseable peer refuses every request.
func (g *PeerGate) Allow(peer string) {
	g.peer = net.ParseIP(peer)
	close(g.ready)
}

func (g *PeerGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := time.NewTimer(peerWait)
	defer timer.Stop()
	select {
	case <-g.ready:
	case <-r.Context().Done():
		return
	case <-timer.C:
		http.Error(w, "The revision is not running", http.StatusServiceUnavailable)
		return
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || g.peer == nil || !g.peer.Equal(net.ParseIP(host)) {
		http.Error(w, "This metadata server belongs to another revision", http.StatusForbidden)
		return
	}
	g.next.ServeHTTP(w, r)
}
