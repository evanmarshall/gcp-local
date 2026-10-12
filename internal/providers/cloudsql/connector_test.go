package cloudsql

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// startupPacket builds a protocol 3.0 startup packet.
func startupPacket(user string) []byte {
	params := []byte("user\x00" + user + "\x00database\x00postgres\x00\x00")
	packet := binary.BigEndian.AppendUint32(nil, uint32(8+len(params))) //nolint:gosec // test packet
	packet = binary.BigEndian.AppendUint32(packet, protocolVersion)
	return append(packet, params...)
}

// loopbackAny binds an ephemeral loopback port.
const loopbackAny = "127.0.0.1:0"

// dialConnector sends an SSL request and a startup packet and returns what
// the connector answers.
func dialConnector(t *testing.T, connector *Connector, user string) []byte {
	t.Helper()
	return dialStartup(t, connector.listener.Addr().String(), user)
}

// dialStartup sends an SSL request and a startup packet to address and
// returns the answer.
func dialStartup(t *testing.T, address, user string) []byte {
	t.Helper()
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", address)
	must(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	ssl := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), sslRequestCode)
	_, err = conn.Write(ssl)
	must(t, err)
	answer := make([]byte, 1)
	_, err = io.ReadFull(conn, answer)
	must(t, err)
	if answer[0] != 'N' {
		t.Fatalf("SSL answer = %q", answer)
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
