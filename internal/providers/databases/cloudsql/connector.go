package cloudsql

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Connector sentinels.
var (
	errConnectorAddress   = errors.New("the Cloud SQL connector requires a numeric loopback listen address")
	errConnectorName      = errors.New("the Cloud SQL connector requires a project:region:instance connection name")
	errConnectorPrincipal = errors.New("the Cloud SQL connector requires a serviceAccount: or user: principal")
)

// startupTimeout bounds the unauthenticated startup phase of a connection.
const startupTimeout = 10 * time.Second

// ConnectorSpec binds one loopback listener to one instance and one principal,
// as the Cloud SQL Auth Proxy binds a listener to its credential. Every
// accepted connection is authorized for that principal before any byte
// reaches PostgreSQL.
type ConnectorSpec struct {
	ConnectionName, Principal, Address string
}

// Connector is one running listener.
type Connector struct {
	listener net.Listener
	done     chan struct{}
	once     sync.Once
	wait     sync.WaitGroup
}

// Close stops accepting connections and waits for active sessions to end.
func (c *Connector) Close() {
	c.once.Do(func() { close(c.done) })
	_ = c.listener.Close()
	c.wait.Wait()
}

// connectorTarget is a parsed connection name.
type connectorTarget struct {
	project, region, instance string
	principal                 gateway.Principal
}

// parseSpec validates a connector specification.
func parseSpec(spec ConnectorSpec) (connectorTarget, error) {
	host, _, err := net.SplitHostPort(spec.Address)
	if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() {
		return connectorTarget{}, errConnectorAddress
	}
	parts := strings.Split(spec.ConnectionName, ":")
	if len(parts) != 3 || provider.ProjectScope(parts[0]).Validate() != nil || !regionPattern.MatchString(parts[1]) || !instanceNamePattern.MatchString(parts[2]) {
		return connectorTarget{}, errConnectorName
	}
	if !strings.HasPrefix(spec.Principal, "serviceAccount:") && !strings.HasPrefix(spec.Principal, "user:") {
		return connectorTarget{}, errConnectorPrincipal
	}
	principal := gateway.Principal{State: gateway.Authenticated, Subject: spec.Principal, Scopes: []string{cloudPlatform}}
	return connectorTarget{project: parts[0], region: parts[1], instance: parts[2], principal: principal}, nil
}

// OpenConnector starts a connector that runs until ctx ends or Close.
func (p *Provider) OpenConnector(ctx context.Context, spec ConnectorSpec) (*Connector, error) {
	target, err := parseSpec(spec)
	if err != nil {
		return nil, err
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", spec.Address)
	if err != nil {
		return nil, fmt.Errorf("bind the Cloud SQL connector: %w", err)
	}
	connector := &Connector{listener: listener, done: make(chan struct{})}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, connector.Close)
	connector.wait.Go(func() {
		<-connector.done
		cancel()
		stop()
	})
	connector.wait.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connector.wait.Go(func() { p.serve(lifetime, target, conn, p.admit) })
		}
	})
	return connector, nil
}

// admission decides whether one connection may reach the instance; it
// returns a SQLSTATE and message on refusal.
type admission func(ctx context.Context, target connectorTarget, user string) (string, string)

// serve admits and forwards one client connection.
func (p *Provider) serve(ctx context.Context, target connectorTarget, client net.Conn, admit admission) {
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(startupTimeout))
	if mysql, _ := p.engineOf(target.project, target.instance); mysql {
		p.serveMySQL(ctx, target, client, admit)
		return
	}
	reader := bufio.NewReader(client)
	startup, user, err := pgwire.ReadStartup(reader, client)
	if err != nil {
		return
	}
	if code, message := admit(ctx, target, user); code != "" {
		_, _ = client.Write(pgwire.ErrorResponse(code, message))
		return
	}
	_ = client.SetDeadline(time.Time{})
	err = p.session(ctx, target, func(ctx context.Context, server net.Conn, removed <-chan struct{}) error {
		if _, err := server.Write(startup); err != nil {
			return fmt.Errorf("forward the startup packet: %w", err)
		}
		pipe(ctx, removed, client, reader, server)
		return nil
	})
	if err != nil {
		_, _ = client.Write(pgwire.ErrorResponse("08006", unavailable))
	}
}

// unavailable is the refusal when the instance's server cannot be reached.
const unavailable = "gcp-local: the Cloud SQL instance is unavailable"

// session dials the instance's private socket inside a backend session;
// forward receives the session context, which reset and shutdown cancel.
func (p *Provider) session(ctx context.Context, target connectorTarget, forward func(context.Context, net.Conn, <-chan struct{}) error) error {
	key := backend.Key{Service: serviceName, Scope: provider.ProjectScope(target.project)}
	err := p.manager.Session(ctx, key, func(ctx context.Context, active backend.Backend) error {
		fleet, err := driverOf(active)
		if err != nil {
			return err
		}
		socket, removed, err := fleet.endpoint(target.instance)
		if err != nil {
			return err
		}
		var dialer net.Dialer
		server, err := dialer.DialContext(ctx, "unix", socket)
		if err != nil {
			return fmt.Errorf("dial the instance socket: %w", err)
		}
		defer server.Close()
		return forward(ctx, server, removed)
	})
	if err != nil {
		return fmt.Errorf("cloud SQL session: %w", err)
	}
	return nil
}

// serveMySQL admits and forwards one MySQL client connection. MySQL servers
// speak first, so the instance is authorized before its socket is dialed;
// the server's greeting is forwarded without its TLS capability, and the
// client's handshake response is read for its user before any credential
// reaches the server.
func (p *Provider) serveMySQL(ctx context.Context, target connectorTarget, client net.Conn, admit admission) {
	if code, message := admit(ctx, target, ""); code != "" {
		_, _ = client.Write(mysqlwire.ErrorPacket(0, mysqlRefusal(code), code, message))
		return
	}
	sent := false
	err := p.session(ctx, target, func(ctx context.Context, server net.Conn, removed <-chan struct{}) error {
		reader, forwarded, err := relayMySQLHandshake(client, server, &sent)
		if err != nil || !forwarded {
			return err
		}
		_ = client.SetDeadline(time.Time{})
		_ = server.SetDeadline(time.Time{})
		pipe(ctx, removed, client, reader, server)
		return nil
	})
	if err != nil && !sent {
		_, _ = client.Write(mysqlwire.ErrorPacket(0, mysqlwire.HostRefused, "08006", unavailable))
	}
}

// relayMySQLHandshake forwards the server greeting without its TLS capability,
// reads the client's handshake response, and forwards it unless it is
// malformed or names the administrative account, which it refuses. sent
// reports whether the greeting reached the client.
func relayMySQLHandshake(client, server net.Conn, sent *bool) (*bufio.Reader, bool, error) {
	_ = server.SetDeadline(time.Now().Add(startupTimeout))
	hello, _, err := mysqlwire.ReadPacket(server)
	if err != nil {
		return nil, false, fmt.Errorf("read the MySQL greeting: %w", err)
	}
	if !mysqlwire.WithoutTLS(hello) {
		return nil, false, mysqlwire.ErrPacket
	}
	if _, err := client.Write(mysqlwire.Packet(0, hello)); err != nil {
		return nil, false, nil //nolint:nilerr // the client went away; nothing to report
	}
	*sent = true
	reader := bufio.NewReader(client)
	response, sequence, err := mysqlwire.ReadPacket(reader)
	if err != nil {
		return nil, false, nil //nolint:nilerr // the client went away; nothing to report
	}
	if user, ok := mysqlwire.HandshakeUser(response); !ok || user == adminRole {
		message := "gcp-local: the Cloud SQL connector requires a MySQL 4.1 handshake without TLS"
		if ok {
			message = "gcp-local: the internal administrative role is not reachable through the connector"
		}
		_, _ = client.Write(mysqlwire.ErrorPacket(sequence+1, mysqlwire.AccessDenied, "28000", message))
		return nil, false, nil
	}
	if _, err := server.Write(mysqlwire.Packet(sequence, response)); err != nil {
		return nil, false, fmt.Errorf("forward the handshake response: %w", err)
	}
	return reader, true, nil
}

// mysqlRefusal maps a connector refusal's SQLSTATE onto a MySQL error number.
func mysqlRefusal(state string) uint16 {
	if state == "28000" {
		return mysqlwire.AccessDenied
	}
	return mysqlwire.HostRefused
}

// admit checks the bound principal's cloudsql.instances.connect permission and
// the instance's existence and region. It returns a SQLSTATE and message on refusal.
func (p *Provider) admit(ctx context.Context, target connectorTarget, user string) (string, string) {
	call := gateway.Call{Context: ctx, Method: methodConnect, Project: target.project, Location: target.region,
		Principal: target.principal, TraceID: hex.EncodeToString(randomBytes(16))}
	// The connector reports the engine's own reason, so it picks the target
	// as provider.Nearest does, under the project lock, and asks the engine
	// directly.
	var decision iam.Decision
	var err error
	scopeErr := p.catalog.With(provider.ProjectScope(target.project), func(*catalog) error {
		candidates := p.candidates(target.project, target.instance)
		chosen := iam.Target{Name: candidates[len(candidates)-1]}
		if _, indexed := p.index.Get(candidates[0]); indexed {
			chosen = iam.Target{Name: candidates[0], Attributes: attributes(candidates[0])}
		}
		decision, err = p.engine.Authorize(call, chosen, nil)
		return nil
	})
	if err == nil {
		err = scopeErr
	}
	if err != nil {
		return "28000", "gcp-local: " + target.principal.Subject + " is not authorized to connect (decision " + decision.ID + "): " + iamReason(err)
	}
	return p.reachable(ctx, target, user)
}

// reachable checks the instance's existence and region and refuses the
// internal administrative role. PostgreSQL's own password authentication
// follows for every other user.
func (p *Provider) reachable(_ context.Context, target connectorTarget, user string) (string, string) {
	record, ok := p.lookup(target.project, target.instance)
	if !ok || record.Region != target.region {
		return "08004", "gcp-local: the Cloud SQL instance " + target.project + ":" + target.region + ":" + target.instance + " does not exist"
	}
	if user == adminRole {
		return "28000", "gcp-local: the internal administrative role is not reachable through the connector"
	}
	return "", ""
}

// iamReason keeps the engine's reason without its gRPC prefix.
func iamReason(err error) string {
	text := err.Error()
	if _, reason, ok := strings.Cut(text, "desc = "); ok {
		return reason
	}
	return text
}

// pipe copies both directions until either side ends, the instance is
// removed, or the session is cancelled by reset or shutdown.
func pipe(ctx context.Context, removed <-chan struct{}, client net.Conn, buffered io.Reader, server net.Conn) {
	finished := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, buffered); finished <- struct{}{} }()
	go func() { _, _ = io.Copy(client, server); finished <- struct{}{} }()
	pending := 2
	select {
	case <-finished:
		pending--
	case <-removed:
	case <-ctx.Done():
	}
	_ = client.Close()
	_ = server.Close()
	for range pending {
		<-finished
	}
}

// publicKey names one instance's published listener.
func publicKey(project, instance string) string { return project + "/" + instance }

// openPublic gives a public-IP instance a port from the data-plane range,
// the local stand-in for its public address. Connections reach PostgreSQL's
// own password authentication with no IAM check, as public-IP connections
// in Cloud SQL do; the internal administrative role is refused. It returns
// nil when publishing is disabled or the instance has no public IP.
func (p *Provider) openPublic(ctx context.Context, project string, record instanceRecord) (*dataplane.Listener, error) {
	if p.pool == nil || !record.IPv4 {
		return nil, nil //nolint:nilnil // nothing to publish is the valid empty case
	}
	target := connectorTarget{project: project, region: record.Region, instance: record.Name}
	listener, err := p.pool.Open(ctx, func(ctx context.Context, conn net.Conn) {
		p.serve(ctx, target, conn, p.reachable)
	})
	if err != nil {
		return nil, fmt.Errorf("publish the instance address: %w", err)
	}
	return listener, nil
}

// trackPublic records an instance's listener so delete and reset can close it.
func (p *Provider) trackPublic(project, instance string, listener *dataplane.Listener) {
	if listener == nil {
		return
	}
	p.publicMu.Lock()
	defer p.publicMu.Unlock()
	p.public[publicKey(project, instance)] = listener
}

// closePublic closes one instance's listener and its forwarded sessions.
func (p *Provider) closePublic(project, instance string) {
	p.publicMu.Lock()
	listener := p.public[publicKey(project, instance)]
	delete(p.public, publicKey(project, instance))
	p.publicMu.Unlock()
	if listener != nil {
		listener.Close()
	}
}

// closeProjectPublic closes every listener of one project after its reset.
func (p *Provider) closeProjectPublic(project string) {
	p.publicMu.Lock()
	var closing []*dataplane.Listener
	for key, listener := range p.public {
		if len(key) > len(project) && key[:len(project)+1] == project+"/" {
			closing = append(closing, listener)
			delete(p.public, key)
		}
	}
	p.publicMu.Unlock()
	for _, listener := range closing {
		listener.Close()
	}
}
