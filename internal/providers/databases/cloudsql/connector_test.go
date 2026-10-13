package cloudsql

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// startupPacket builds a protocol 3.0 startup packet.
func startupPacket(user string) []byte {
	params := []byte("user\x00" + user + "\x00database\x00postgres\x00\x00")
	packet := binary.BigEndian.AppendUint32(nil, uint32(8+len(params))) //nolint:gosec // test packet
	packet = binary.BigEndian.AppendUint32(packet, pgwire.ProtocolVersion)
	return append(packet, params...)
}

// loopbackAny binds an ephemeral loopback port.
const loopbackAny = "127.0.0.1:0"

// dialConnector sends an SSL request and a startup packet and returns what
// the connector answers.
func dialConnector(t *testing.T, connector *Connector, user string) []byte {
	t.Helper()
	return dialStartup(t, connector.listener.Addr().String(), user, true)
}

// dialStartup sends a startup packet to address, after an SSL request the
// connector declines when ssl, and returns the answer.
func dialStartup(t *testing.T, address, user string, ssl bool) []byte {
	t.Helper()
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", address)
	must(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if ssl {
		request := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), pgwire.SSLRequestCode)
		_, err = conn.Write(request)
		must(t, err)
		answer := make([]byte, 1)
		_, err = io.ReadFull(conn, answer)
		must(t, err)
		if answer[0] != 'N' {
			t.Fatalf("SSL answer = %q", answer)
		}
	}
	_, err = conn.Write(startupPacket(user))
	must(t, err)
	reply, _ := io.ReadAll(conn)
	return reply
}

// expectReply asserts that a connection was refused with a message fragment.
func expectReply(t *testing.T, connector *Connector, user, fragment string) {
	t.Helper()
	if reply := dialConnector(t, connector, user); len(reply) == 0 || reply[0] != 'E' || !bytes.Contains(reply, []byte(fragment)) {
		t.Fatalf("reply = %q, want an error containing %q", reply, fragment)
	}
}

// openConnector binds a connector for one principal and connection name.
func (h *harness) openConnector(token, name string) *Connector {
	h.t.Helper()
	connector, err := h.provider.OpenConnector(h.t.Context(), ConnectorSpec{ConnectionName: name, Principal: principal(token), Address: loopbackAny})
	must(h.t, err)
	h.t.Cleanup(connector.Close)
	return connector
}

func TestConnectorAuthorizesEachConnection(t *testing.T) {
	h := newHarness(t)
	name := testProject + ":us-central1:" + testInstance
	allowed, viewer, peer := h.openConnector(clientToken, name), h.openConnector(viewerToken, name), h.openConnector(peerToken, name)
	expectReply(t, allowed, defaultUser, "does not exist")
	if h.fleet() != nil {
		t.Fatal("a connection to a missing instance started a backend")
	}
	h.createInstance()
	server := h.fleet().server(testInstance)
	expectReply(t, viewer, defaultUser, "cloudsql.instances.connect")
	expectReply(t, peer, defaultUser, "28000")
	expectReply(t, h.openConnector(clientToken, testProject+":europe-west1:"+testInstance), defaultUser, "does not exist")
	expectReply(t, allowed, adminRole, "administrative role")
	select {
	case got := <-server.received:
		t.Fatalf("a refused connection reached PostgreSQL: %q", got)
	default:
	}
	dialConnector(t, allowed, defaultUser)
	select {
	case got := <-server.received:
		if !bytes.Equal(got, startupPacket(defaultUser)) {
			t.Fatalf("forwarded = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the authorized startup packet was not forwarded")
	}
}

func TestConnectorSpecValidation(t *testing.T) {
	const name, account = "p:us-central1:i", "serviceAccount:a@p.iam.gserviceaccount.com"
	for _, spec := range []ConnectorSpec{
		{ConnectionName: name, Principal: account, Address: "0.0.0.0:0"},
		{ConnectionName: "p:i", Principal: account, Address: loopbackAny},
		{ConnectionName: name, Principal: "allUsers", Address: loopbackAny},
	} {
		if _, err := parseSpec(spec); err == nil {
			t.Fatalf("accepted %+v", spec)
		}
	}
	target, err := parseSpec(ConnectorSpec{ConnectionName: name, Principal: "user:a@example.com", Address: "[::1]:5432"})
	if err != nil || target.instance != "i" || !strings.HasPrefix(target.principal.Subject, "user:") {
		t.Fatalf("target = %+v, %v", target, err)
	}
}

// onePortPool returns a pool over one currently free loopback port.
func onePortPool(t *testing.T) *dataplane.Pool {
	t.Helper()
	var listen net.ListenConfig
	probe, err := listen.Listen(t.Context(), "tcp", loopbackAny)
	must(t, err)
	address, ok := probe.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address = %v", probe.Addr())
	}
	port := strconv.Itoa(address.Port)
	must(t, probe.Close())
	pool, err := dataplane.Parse(port+"-"+port, "127.0.0.1")
	must(t, err)
	return pool
}

// publishedAddress reads the PRIMARY address and its gcp-local port extension.
func (h *harness) publishedAddress(name string) string {
	h.t.Helper()
	instance := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+name, nil), http.StatusOK)
	addresses, _ := instance["ipAddresses"].([]any)
	primary := object(h.t, addresses[0])
	port, _ := primary["port"].(float64)
	if primary["type"] != "PRIMARY" || primary["ipAddress"] != dataplane.AdvertisedHost || port == 0 {
		h.t.Fatalf("primary address = %v", primary)
	}
	return net.JoinHostPort(dataplane.AdvertisedHost, strconv.Itoa(int(port)))
}

func expectRefused(t *testing.T, address string) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("%s still accepts connections", address)
	}
}

func TestPublicAddressForwardsWithoutIAM(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	h.createInstance()
	address := h.publishedAddress(testInstance)
	server := h.fleet().server(testInstance)

	if reply := dialStartup(t, address, adminRole, false); len(reply) == 0 || reply[0] != 'E' || !bytes.Contains(reply, []byte("administrative role")) {
		t.Fatalf("admin role reply = %q", reply)
	}
	dialStartup(t, address, defaultUser, false)
	select {
	case got := <-server.received:
		if !bytes.Equal(got, startupPacket(defaultUser)) {
			t.Fatalf("forwarded = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the startup packet was not forwarded to PostgreSQL")
	}

	private := insertBody()
	private[keyName] = "private-pg"
	object(t, private[keySettings])["ipConfiguration"] = map[string]any{"ipv4Enabled": false, "privateNetwork": "projects/demo-sql/global/networks/default"}
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, private), http.StatusOK)
	instance := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/private-pg", nil), http.StatusOK)
	addresses, _ := instance["ipAddresses"].([]any)
	if _, published := object(t, addresses[0])["port"]; published || h.fleet().server("private-pg") == nil {
		t.Fatalf("a private-IP instance was published or has no server: %v", addresses)
	}

	second := insertBody()
	second[keyName] = "second-pg"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, second), http.StatusTooManyRequests)
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/second-pg", nil), http.StatusNotFound)

	h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusOK)
	expectRefused(t, address)
	h.createInstance()
	if again := h.publishedAddress(testInstance); again != address {
		t.Fatalf("the released port was not reused: %s, want %s", again, address)
	}
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: provider.ProjectScope(testProject)}))
	expectRefused(t, address)
}

func mysqlHandshakeResponse(user string, flags uint32) []byte {
	response := binary.LittleEndian.AppendUint32(nil, flags)
	response = binary.LittleEndian.AppendUint32(response, mysqlwire.MaxPacket)
	response = append(response, mysqlwire.CharsetUTF8MB4)
	response = append(response, make([]byte, 23)...)
	return append(append(append(response, user...), 0, 0), mysqlwire.NativePassword+"\x00"...)
}

const clientFlags = mysqlwire.CapProtocol41 | mysqlwire.CapSecureConnection | mysqlwire.CapPluginAuth

func dialMySQLHandshake(t *testing.T, address, user string) ([]byte, []byte) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	must(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	hello, _, err := mysqlwire.ReadPacket(conn)
	must(t, err)
	if hello[0] == 0xff {
		return hello, nil
	}
	_, err = conn.Write(mysqlwire.Packet(1, mysqlHandshakeResponse(user, clientFlags)))
	must(t, err)
	reply, _ := io.ReadAll(conn)
	return hello, reply
}

func expectForwarded(t *testing.T, server *fakeServer, want []byte) {
	t.Helper()
	select {
	case got := <-server.received:
		if !bytes.Equal(got, want) {
			t.Fatalf("forwarded = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was forwarded to MySQL")
	}
}

func TestMySQLPublicAddress(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	h.createMySQL()
	address := h.publishedAddress(testMySQL)
	server := h.fleet().server(testMySQL)

	hello, reply := dialMySQLHandshake(t, address, adminRole)
	// The published port forwards the server greeting and offers the TLS it terminates.
	if flags := binary.LittleEndian.Uint16(hello[bytes.IndexByte(hello, 0)+14:]); flags&mysqlwire.CapSSL == 0 || !bytes.Equal(hello, fakeGreeting()) {
		t.Fatalf("greeting = %x", hello)
	}
	if !bytes.Equal(reply, mysqlwire.ErrorPacket(2, mysqlwire.AccessDenied, "28000", "gcp-local: the internal administrative role is not reachable through the connector")) {
		t.Fatalf("admin role reply = %q", reply)
	}
	_, reply = dialMySQLHandshake(t, address, mysqlRootUser)
	expectForwarded(t, server, mysqlwire.Packet(1, mysqlHandshakeResponse(mysqlRootUser, clientFlags)))
	if len(reply) != 0 {
		t.Fatalf("reply = %q", reply)
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testMySQLPath, nil), http.StatusOK)
	expectRefused(t, address)
}

func TestMySQLConnectorRefusesBeforeGreeting(t *testing.T) {
	h := newHarness(t)
	viewer := h.openConnector(viewerToken, testProject+":us-central1:"+testMySQL)
	h.createMySQL()
	refused, _ := dialMySQLHandshake(t, viewer.listener.Addr().String(), mysqlRootUser)
	failure := mysqlwire.ParseError(refused)
	if !strings.HasPrefix(failure.Error(), "ERROR 1045 (28000): gcp-local: "+principal(viewerToken)+" is not authorized to connect") ||
		!strings.Contains(failure.Error(), "cloudsql.instances.connect") {
		t.Fatalf("viewer refusal = %v", failure)
	}
}

func TestMySQLHandshakeUser(t *testing.T) {
	if user, ok := mysqlwire.HandshakeUser(mysqlHandshakeResponse("app", mysqlwire.CapProtocol41)); !ok || user != "app" {
		t.Fatalf("user = %q", user)
	}
	if _, ok := mysqlwire.HandshakeUser(mysqlHandshakeResponse("app", mysqlwire.CapProtocol41|mysqlwire.CapSSL)); ok {
		t.Fatal("accepted an SSLRequest")
	}
	if _, ok := mysqlwire.HandshakeUser(mysqlHandshakeResponse("app", 0)); ok {
		t.Fatal("accepted a pre-4.1 handshake")
	}
}
