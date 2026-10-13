package cloudsql

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
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
			connector.wait.Go(func() { p.serve(lifetime, target, conn) })
		}
	})
	return connector, nil
}

// admission decides whether one connection may reach the instance; it
// returns a SQLSTATE and message on refusal.
type admission func(ctx context.Context, target connectorTarget, user string) (string, string)

// serve admits and forwards one connector connection: IAM is checked and
// TLS is declined, as the Cloud SQL Auth Proxy's local listener does.
func (p *Provider) serve(ctx context.Context, target connectorTarget, client net.Conn) {
	p.forward(ctx, target, client, p.admit, nil)
}

// forward admits and forwards one client connection. With a policy (the
// published port) it offers TLS and enforces the instance's TLS mode.
func (p *Provider) forward(ctx context.Context, target connectorTarget, client net.Conn, admit admission, policy *tlsPolicy) {
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(startupTimeout))
	if mysql, _ := p.engineOf(target.project, target.instance); mysql {
		p.serveMySQL(ctx, target, client, admit, policy)
		return
	}
	conn, reader, startup, user, ok := readPostgresStartup(ctx, client, policy)
	if !ok {
		return
	}
	if code, message := admit(ctx, target, user); code != "" {
		_, _ = conn.Write(pgwire.ErrorResponse(code, message))
		return
	}
	_ = client.SetDeadline(time.Time{})
	err := p.session(ctx, target, func(ctx context.Context, server net.Conn, removed <-chan struct{}) error {
		if _, err := server.Write(startup); err != nil {
			return fmt.Errorf("forward the startup packet: %w", err)
		}
		pipe(ctx, removed, conn, reader, server)
		return nil
	})
	if err != nil {
		_, _ = conn.Write(pgwire.ErrorResponse("08006", unavailable))
	}
}

// readPostgresStartup reads a PostgreSQL startup packet, inside TLS when the
// client asks for it on the published port, and refuses a cleartext startup
// the instance's TLS mode forbids. ok is false when the connection ends.
func readPostgresStartup(ctx context.Context, client net.Conn, policy *tlsPolicy) (net.Conn, *bufio.Reader, []byte, string, bool) {
	conn := client
	reader := bufio.NewReader(conn)
	startup, user, encrypt, err := pgwire.ReadStartup(reader, conn, policy != nil)
	if err == nil && encrypt {
		secure := tls.Server(client, policy.config)
		if err := secure.HandshakeContext(ctx); err != nil {
			return nil, nil, nil, "", false
		}
		conn, reader = secure, bufio.NewReader(secure)
		startup, user, _, err = pgwire.ReadStartup(reader, conn, false)
	}
	if err != nil {
		return nil, nil, nil, "", false
	}
	if policy != nil && policy.encrypted && !encrypt {
		_, _ = conn.Write(pgwire.ErrorResponse("28000", "gcp-local: the instance's sslMode "+policy.mode+" requires TLS (sslmode=require)"))
		return nil, nil, nil, "", false
	}
	return conn, reader, startup, user, true
}

// tlsPolicy is the published listener's TLS for one connection: the
// instance's server certificate and client CA, and whether its sslMode
// requires encryption (ENCRYPTED_ONLY or TRUSTED_CLIENT_CERTIFICATE_REQUIRED,
// which also requires a client certificate from sslCerts).
type tlsPolicy struct {
	config    *tls.Config
	mode      string
	encrypted bool
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
		socket, removed, err := fleet.Endpoint(target.instance)
		if err != nil {
			return err //nolint:wrapcheck // the fleet's errors are descriptive
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
// speak first, so the instance is authorized before its socket is dialed.
// The connector forwards the greeting without its TLS capability; the
// published port offers TLS and terminates it here. Either way the client's
// handshake response is read for its user before any credential reaches
// the server.
func (p *Provider) serveMySQL(ctx context.Context, target connectorTarget, client net.Conn, admit admission, policy *tlsPolicy) {
	if code, message := admit(ctx, target, ""); code != "" {
		_, _ = client.Write(mysqlwire.ErrorPacket(0, mysqlRefusal(code), code, message))
		return
	}
	sent := false
	err := p.session(ctx, target, func(ctx context.Context, server net.Conn, removed <-chan struct{}) error {
		relay := mysqlRelay{client: client, server: server, policy: policy, sent: &sent}
		conn, reader, forwarded, err := relay.handshake(ctx)
		if err != nil || !forwarded {
			return err
		}
		_ = client.SetDeadline(time.Time{})
		_ = server.SetDeadline(time.Time{})
		pipe(ctx, removed, conn, reader, server)
		return nil
	})
	if err != nil && !sent {
		_, _ = client.Write(mysqlwire.ErrorPacket(0, mysqlwire.HostRefused, "08006", unavailable))
	}
}

// mysqlRelay carries one MySQL connection through its handshake.
type mysqlRelay struct {
	client, server net.Conn
	policy         *tlsPolicy
	// sent reports whether the greeting reached the client.
	sent *bool
}

// errMySQLAuthRelay reports an authentication exchange the relay can't follow.
var errMySQLAuthRelay = errors.New("unexpected MySQL authentication packet")

// handshake forwards the server greeting, offering TLS on the published
// port, reads the client's handshake response (inside TLS when the client
// asked for it), and forwards it unless it is malformed, names the
// administrative account, or skips TLS the instance requires. Inside TLS
// the client's sequence numbers run one ahead of the server's, which never
// saw the SSLRequest, so the authentication exchange is renumbered. It
// returns the client side to pipe, which is the TLS stream when one started.
func (r mysqlRelay) handshake(ctx context.Context) (net.Conn, *bufio.Reader, bool, error) {
	_ = r.server.SetDeadline(time.Now().Add(startupTimeout))
	hello, _, err := mysqlwire.ReadPacket(r.server)
	if err != nil {
		return nil, nil, false, fmt.Errorf("read the MySQL greeting: %w", err)
	}
	if !mysqlwire.SetTLS(hello, r.policy != nil) {
		return nil, nil, false, mysqlwire.ErrPacket
	}
	if _, err := r.client.Write(mysqlwire.Packet(0, hello)); err != nil {
		return nil, nil, false, nil //nolint:nilerr // the client went away; nothing to report
	}
	*r.sent = true
	conn, reader, response, sequence, encrypted, ok := r.readResponse(ctx)
	if !ok {
		return nil, nil, false, nil
	}
	if refused := r.refusal(response, encrypted); refused != nil {
		_, _ = conn.Write(mysqlwire.ErrorPacket(sequence+1, refused.Code, refused.State, refused.Message))
		return nil, nil, false, nil //nolint:nilerr // the refusal is the client's answer, not a failure
	}
	shift := byte(0)
	if encrypted {
		shift = 1
	}
	if _, err := r.server.Write(mysqlwire.Packet(sequence-shift, response)); err != nil {
		return nil, nil, false, fmt.Errorf("forward the handshake response: %w", err)
	}
	if shift == 0 {
		return conn, reader, true, nil
	}
	return conn, reader, true, renumberAuth(r.server, conn, reader)
}

// bufferedConn reads what a bufio.Reader already took from Conn first.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (b bufferedConn) Read(p []byte) (int, error) { return b.reader.Read(p) } //nolint:wrapcheck // a transparent reader

// readResponse reads the client's handshake response, inside TLS when the
// client sends an SSLRequest the published port accepts. ok is false when
// the client went away or its TLS handshake failed; neither has a reply.
func (r mysqlRelay) readResponse(ctx context.Context) (net.Conn, *bufio.Reader, []byte, byte, bool, bool) {
	conn := r.client
	reader := bufio.NewReader(conn)
	response, sequence, err := mysqlwire.ReadPacket(reader)
	if err != nil {
		return nil, nil, nil, 0, false, false
	}
	if r.policy == nil || !mysqlwire.IsSSLRequest(response) {
		return conn, reader, response, sequence, false, true
	}
	// A MySQL client starts TLS right after its SSLRequest, so the
	// handshake may already sit in reader's buffer.
	secure := tls.Server(bufferedConn{Conn: r.client, reader: reader}, r.policy.config)
	if err := secure.HandshakeContext(ctx); err != nil {
		return nil, nil, nil, 0, false, false
	}
	reader = bufio.NewReader(secure)
	if response, sequence, err = mysqlwire.ReadPacket(reader); err != nil {
		return nil, nil, nil, 0, false, false
	}
	return secure, reader, mysqlwire.WithoutSSLFlag(response), sequence, true, true
}

// secureTransportRequired is MySQL's ER_SECURE_TRANSPORT_REQUIRED, which
// Cloud SQL's require_secure_transport answers a cleartext login with.
const secureTransportRequired uint16 = 3159

// refusal returns why a handshake response may not reach the server, or nil.
func (r mysqlRelay) refusal(response []byte, encrypted bool) *mysqlwire.Error {
	user, ok := mysqlwire.HandshakeUser(response)
	switch {
	case !ok:
		return &mysqlwire.Error{Code: mysqlwire.AccessDenied, State: "28000", Message: "gcp-local: the Cloud SQL connector requires a MySQL 4.1 handshake"}
	case user == adminRole:
		return &mysqlwire.Error{Code: mysqlwire.AccessDenied, State: "28000",
			Message: "gcp-local: the internal administrative role is not reachable through the connector"}
	case r.policy != nil && r.policy.encrypted && !encrypted:
		return &mysqlwire.Error{Code: secureTransportRequired, State: "HY000",
			Message: "Connections using insecure transport are prohibited while --require_secure_transport=ON."}
	default:
		return nil
	}
}

// renumberAuth relays the authentication exchange after a TLS handshake,
// one packet at a time with the sequence numbers shifted, until the server's
// OK or ERR; afterwards every command restarts its own numbering.
func renumberAuth(server net.Conn, client net.Conn, reader *bufio.Reader) error {
	for {
		payload, sequence, err := mysqlwire.ReadPacket(server)
		if err != nil {
			return fmt.Errorf("read the MySQL authentication reply: %w", err)
		}
		if _, err := client.Write(mysqlwire.Packet(sequence+1, payload)); err != nil {
			return nil //nolint:nilerr // the client went away; nothing to report
		}
		done, answer, err := authDone(payload)
		if done {
			return err
		}
		if !answer {
			continue
		}
		answerPacket, sequence, err := mysqlwire.ReadPacket(reader)
		if err != nil {
			return nil //nolint:nilerr // the client went away; nothing to report
		}
		if _, err := server.Write(mysqlwire.Packet(sequence-1, answerPacket)); err != nil {
			return fmt.Errorf("forward the authentication answer: %w", err)
		}
	}
}

// authDone classifies a server authentication packet: done at OK or ERR,
// and answer when the client replies to it. caching_sha2_password's fast
// authentication (0x01 0x03) is followed by OK without a reply.
func authDone(payload []byte) (bool, bool, error) {
	switch {
	case len(payload) == 0:
		return true, false, errMySQLAuthRelay
	case payload[0] == 0x00, payload[0] == 0xff:
		return true, false, nil
	case payload[0] == 0x01 && len(payload) == 2 && payload[1] == 3:
		return false, false, nil
	default:
		return false, true, nil
	}
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
// the local stand-in for its public address. Connections reach the server's
// own password authentication with no IAM check, as public-IP connections in
// Cloud SQL do; the listener enforces the instance's authorized networks and
// TLS mode first, and the internal administrative role is refused. It
// returns nil when publishing is disabled or the instance has no public IP.
func (p *Provider) openPublic(ctx context.Context, project string, record instanceRecord) (*dataplane.Listener, error) {
	if p.pool == nil || !record.IPv4 {
		return nil, nil //nolint:nilnil // nothing to publish is the valid empty case
	}
	target := connectorTarget{project: project, region: record.Region, instance: record.Name}
	listener, err := p.pool.Open(ctx, func(ctx context.Context, conn net.Conn) {
		p.servePublic(ctx, target, conn)
	})
	if err != nil {
		return nil, fmt.Errorf("publish the instance address: %w", err)
	}
	return listener, nil
}

// servePublic admits one published-port connection by source address,
// then forwards it with the instance's current TLS policy.
func (p *Provider) servePublic(ctx context.Context, target connectorTarget, conn net.Conn) {
	record, ok := p.lookup(target.project, target.instance)
	if !ok {
		p.forward(ctx, target, conn, p.reachable, nil)
		return
	}
	if !p.authorized(record, conn.RemoteAddr()) {
		p.refuseNetwork(conn, record, conn.RemoteAddr())
		return
	}
	// sslMode wins over requireSsl, which applySettings folds into it.
	mode := record.SSLMode
	encrypted := mode == sslEncrypted || mode == sslTrustedCerts
	config, err := p.serverConfig(target.project, target.instance, mode == sslTrustedCerts)
	if err != nil {
		_ = conn.Close()
		return
	}
	p.forward(ctx, target, conn, p.reachable, &tlsPolicy{config: config, mode: mode, encrypted: encrypted})
}

// authorized reports whether a connection's source address may reach the
// instance: every address when it has no authorized networks (a local
// divergence: GCP then admits no direct public-IP connection), otherwise
// only addresses inside an unexpired entry.
func (p *Provider) authorized(record instanceRecord, remote net.Addr) bool {
	if len(record.Networks) == 0 {
		return true
	}
	address, err := netip.ParseAddrPort(remote.String())
	if err != nil {
		return false
	}
	now := p.clock()
	return slices.ContainsFunc(record.Networks, func(entry aclEntry) bool { return entry.admits(address.Addr(), now) })
}

// refuseNetwork answers a connection from an unauthorized address in the
// engine's protocol, before any server is dialed: GCP's firewall drops it
// instead, so the client would time out.
func (p *Provider) refuseNetwork(conn net.Conn, record instanceRecord, remote net.Addr) {
	defer conn.Close()
	host, _, _ := net.SplitHostPort(remote.String())
	message := "gcp-local: " + host + " is not in the authorized networks of instance " + record.Name
	if isMySQL(record.Version) {
		_, _ = conn.Write(mysqlwire.ErrorPacket(0, mysqlwire.HostRefused, "HY000", message))
		return
	}
	_ = conn.SetDeadline(time.Now().Add(startupTimeout))
	reader := bufio.NewReader(conn)
	if _, _, _, err := pgwire.ReadStartup(reader, conn, false); err == nil {
		_, _ = conn.Write(pgwire.ErrorResponse("28000", message))
	}
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
