package cloudsql

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

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

	if reply := dialStartup(t, address, adminRole); len(reply) == 0 || reply[0] != 'E' || !bytes.Contains(reply, []byte("administrative role")) {
		t.Fatalf("admin role reply = %q", reply)
	}
	dialStartup(t, address, defaultUser)
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
