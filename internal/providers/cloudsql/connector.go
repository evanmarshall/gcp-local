package cloudsql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
)

// Connector sentinels.
var (
	errConnectorAddress   = errors.New("the Cloud SQL connector requires a numeric loopback listen address")
	errConnectorName      = errors.New("the Cloud SQL connector requires a project:region:instance connection name")
	errConnectorPrincipal = errors.New("the Cloud SQL connector requires a serviceAccount: or user: principal")
	errStartupPacket      = errors.New("malformed PostgreSQL startup packet")
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
	if len(parts) != 3 || scopeOf(parts[0]).Validate() != nil || !regionPattern.MatchString(parts[1]) || !instanceNamePattern.MatchString(parts[2]) {
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

// serve authorizes and forwards one client connection.
func (p *Provider) serve(ctx context.Context, target connectorTarget, client net.Conn) {
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(startupTimeout))
	reader := bufio.NewReader(client)
	startup, user, err := readStartup(reader, client)
	if err != nil {
		return
	}
	if code, message := p.admit(ctx, target, user); code != "" {
		_, _ = client.Write(errorResponse(code, message))
		return
	}
	_ = client.SetDeadline(time.Time{})
	key := backend.Key{Service: serviceName, Scope: scopeOf(target.project)}
	err = p.manager.Session(ctx, key, func(ctx context.Context, active backend.Backend) error {
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
		if _, err := server.Write(startup); err != nil {
			return fmt.Errorf("forward the startup packet: %w", err)
		}
		pipe(ctx, removed, client, reader, server)
		return nil
	})
	if err != nil {
		_, _ = client.Write(errorResponse("08006", "gcp-local: the Cloud SQL instance is unavailable"))
	}
}

// admit checks the bound principal's cloudsql.instances.connect permission and
// the instance's existence and region. It returns a SQLSTATE and message on refusal.
func (p *Provider) admit(ctx context.Context, target connectorTarget, user string) (string, string) {
	call := gateway.Call{Context: ctx, Method: methodConnect, Project: target.project, Location: target.region,
		Principal: target.principal, TraceID: hex.EncodeToString(randomBytes(16))}
	decision, err := p.engine.Authorize(call, p.target(target.project, target.instance), nil)
	if err != nil {
		return "28000", "gcp-local: " + target.principal.Subject + " is not authorized to connect (decision " + decision.ID + "): " + iamReason(err)
	}
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

// readStartup reads the client's startup packet, declining TLS and GSS
// encryption as the Cloud SQL Auth Proxy's local listener does. It returns the
// raw packet to forward and the requested user. A cancel request is forwarded
// as-is after the same authorization.
func readStartup(reader *bufio.Reader, client net.Conn) ([]byte, string, error) {
	for range 3 {
		packet, code, err := readPacket(reader)
		if err != nil {
			return nil, "", err
		}
		switch code {
		case sslRequestCode, gssRequestCode:
			if _, err := client.Write([]byte{'N'}); err != nil {
				return nil, "", fmt.Errorf("decline encryption: %w", err)
			}
		case cancelRequestCode:
			return packet, "", nil
		case protocolVersion:
			return packet, startupUser(packet[8:]), nil
		default:
			return nil, "", errStartupPacket
		}
	}
	return nil, "", errStartupPacket
}

// readPacket reads one untyped startup-phase packet and its request code.
func readPacket(reader *bufio.Reader) ([]byte, uint32, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, fmt.Errorf("read the startup packet: %w", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length < 8 || length > 10000 {
		return nil, 0, errStartupPacket
	}
	packet := make([]byte, length)
	copy(packet, header)
	if _, err := io.ReadFull(reader, packet[8:]); err != nil {
		return nil, 0, fmt.Errorf("read the startup packet: %w", err)
	}
	return packet, binary.BigEndian.Uint32(header[4:]), nil
}

// startupUser extracts the user parameter from a startup packet body.
func startupUser(body []byte) string {
	fields := bytes.Split(body, []byte{0})
	for i := 0; i+1 < len(fields); i += 2 {
		if string(fields[i]) == "user" {
			return string(fields[i+1])
		}
	}
	return ""
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
