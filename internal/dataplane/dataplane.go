// Package dataplane publishes protocol endpoints (Redis, PostgreSQL) of
// provider-owned servers on a bounded, operator-configured port range, so a
// client outside the emulator's process or container can reach them at the
// address the API reports. The listeners only forward bytes: each provider
// keeps its own protocol authentication (Redis AUTH, PostgreSQL SCRAM).
package dataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

// AdvertisedHost is the address clients use: the host's loopback, where the
// packaged environment publishes the range.
const AdvertisedHost = "127.0.0.1"

// maxPorts bounds the range so a misconfiguration cannot claim many ports.
const maxPorts = 256

// Pool sentinels.
var (
	ErrRange     = errors.New("data-ports expects FIRST-LAST with 1024 <= FIRST <= LAST <= 65535 and at most 256 ports")
	ErrListen    = errors.New("data-listen expects a numeric IP address")
	ErrExhausted = errors.New("every port in the data-plane range is in use")
)

// Pool hands out ports from one range and binds forwarding listeners on them.
// A nil *Pool means publishing is disabled.
type Pool struct {
	host        string
	first, last int
	mu          sync.Mutex
	used        map[int]bool
}

// Parse builds a pool from the -data-ports and -data-listen flag values. An
// empty range disables publishing and returns a nil pool.
func Parse(ports, listen string) (*Pool, error) {
	if ports == "" {
		return nil, nil //nolint:nilnil // no range configured is the valid disabled case
	}
	first, last, found := strings.Cut(ports, "-")
	low, lowErr := strconv.Atoi(first)
	high, highErr := strconv.Atoi(last)
	if !found || lowErr != nil || highErr != nil || low < 1024 || high > 65535 || low > high || high-low >= maxPorts {
		return nil, ErrRange
	}
	if net.ParseIP(listen) == nil {
		return nil, ErrListen
	}
	return &Pool{host: listen, first: low, last: high, used: map[int]bool{}}, nil
}

// Listener is one published port forwarding to a provider-owned server.
type Listener struct {
	pool     *Pool
	port     int
	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   bool
	cancel   context.CancelFunc
	wait     sync.WaitGroup
	once     sync.Once
}

// Port is the published port clients connect to.
func (l *Listener) Port() int { return l.port }

// Serve handles one accepted connection; ctx ends when the listener closes.
type Serve func(ctx context.Context, conn net.Conn)

// Open binds the lowest free port in the range and calls serve for every
// accepted connection until Close. Ports taken by other processes are
// skipped. Connections outlive ctx; only Close ends them.
func (p *Pool) Open(ctx context.Context, serve Serve) (*Listener, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var listen net.ListenConfig
	for port := p.first; port <= p.last; port++ {
		if p.used[port] {
			continue
		}
		bound, err := listen.Listen(ctx, "tcp", net.JoinHostPort(p.host, strconv.Itoa(port)))
		if err != nil {
			continue
		}
		p.used[port] = true
		lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
		l := &Listener{pool: p, port: port, listener: bound, conns: map[net.Conn]struct{}{}, cancel: cancel}
		l.wait.Go(func() { l.accept(lifetime, serve) })
		return l, nil
	}
	return nil, fmt.Errorf("%w (%d-%d)", ErrExhausted, p.first, p.last)
}

// accept runs serve per connection and tracks it so Close can end it.
func (l *Listener) accept(ctx context.Context, serve Serve) {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			_ = conn.Close()
			return
		}
		l.conns[conn] = struct{}{}
		l.mu.Unlock()
		l.wait.Go(func() {
			defer func() {
				l.mu.Lock()
				delete(l.conns, conn)
				l.mu.Unlock()
				_ = conn.Close()
			}()
			serve(ctx, conn)
		})
	}
}

// Close stops accepting, closes every forwarded connection, waits for them to
// end, and returns the port to the pool.
func (l *Listener) Close() {
	l.once.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.cancel()
		_ = l.listener.Close()
		for conn := range l.conns {
			_ = conn.Close()
		}
		l.mu.Unlock()
		l.wait.Wait()
		l.pool.mu.Lock()
		delete(l.pool.used, l.port)
		l.pool.mu.Unlock()
	})
}

// Pipe copies both directions between client and server until either side
// ends, then closes both.
func Pipe(client io.ReadWriteCloser, fromClient io.Reader, server io.ReadWriteCloser) {
	finished := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, fromClient); finished <- struct{}{} }()
	go func() { _, _ = io.Copy(client, server); finished <- struct{}{} }()
	<-finished
	_ = client.Close()
	_ = server.Close()
	<-finished
}

// Forward returns a serve function that pipes each connection to a fixed TCP address.
func Forward(address string) Serve {
	return func(ctx context.Context, client net.Conn) {
		var dialer net.Dialer
		server, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return
		}
		Pipe(client, client, server)
	}
}
