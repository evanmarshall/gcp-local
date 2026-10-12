//go:build redisintegration

package memorystore

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// TestRealRedisLifecycle drives the pinned redis-server through the provider:
// a denied call starts nothing, AUTH is enforced on the protocol endpoint,
// documented blocked commands answer NOPERM, resize and delete reach the
// process, and reset stops only the selected scope's servers. Authorization
// uses the test authorizer because the roles/redis.* snapshots are unrecorded.
func TestRealRedisLifecycle(t *testing.T) {
	interfaces, err := net.Interfaces()
	must(t, err)
	if os.Getenv("GCP_LOCAL_ISOLATED") != "1" || len(interfaces) != 1 || interfaces[0].Name != "lo" {
		t.Fatal("Run actual Redis tests in the loopback-only runner")
	}
	config := Config{Server: os.Getenv("GCP_LOCAL_REDIS_SERVER")}
	must(t, config.validate(context.Background()))
	h := &harness{t: t, index: state.NewIndex(), auth: newFakeAuthorizer(), fakes: map[state.Scope]*fakeDriver{}}
	h.auth.grant(testProject, principal(adminToken), adminPermissions)
	h.auth.grant(peerProject, principal(peerToken), adminPermissions)
	h.manager = backend.NewManager(30*time.Second, 10*time.Second)
	t.Cleanup(func() { must(t, h.manager.Shutdown(context.Background())) })
	registry := gateway.NewRegistry()
	h.provider, err = newProvider(registry, h.manager, h.index, h.auth, func(state.Scope) (backend.Backend, error) {
		return newFleet(config), nil
	})
	must(t, err)
	h.gate = gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: principal(token),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})

	h.expect(h.create(peerToken, testProject, testRegion, testInstance, basic(nil)), http.StatusForbidden)
	if status := h.manager.Status(); len(status) != 0 {
		t.Fatalf("a denied request started a backend: %+v", status)
	}
	secured := h.expect(h.create(adminToken, testProject, testRegion, testInstance, basic(map[string]any{keyAuthEnabled: true})), http.StatusOK)
	address := endpoint(t, secured)
	auth, _ := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+testInstance+"/authString", nil), http.StatusOK)["authString"].(string)
	exerciseAuth(t, address, auth)
	open := h.expect(h.create(adminToken, testProject, testRegion, "open", basic(nil)), http.StatusOK)
	openAddress := endpoint(t, open)
	expectReply(t, client(t, openAddress), "PONG", "PING")

	h.expect(h.invoke(adminToken, http.MethodPatch, testInstances+"/"+testInstance+"?updateMask=memorySizeGb", map[string]any{fieldMemory: 2}), http.StatusOK)
	conn := client(t, address)
	expectReply(t, conn, "OK", "AUTH", auth)
	info, err := conn.do("INFO", "memory")
	must(t, err)
	if !strings.Contains(info, "maxmemory:"+strconv.FormatInt(2<<30, 10)+"\r\n") {
		t.Fatalf("resize did not reach the server: %s", info)
	}
	expectReply(t, conn, "kept", "GET", "greeting")

	h.expect(h.invoke(adminToken, http.MethodDelete, testInstances+"/"+testInstance, nil), http.StatusOK)
	expectClosed(t, address)
	peer := h.expect(h.create(peerToken, peerProject, testRegion, testInstance, basic(nil)), http.StatusOK)
	must(t, h.manager.Reset(context.Background(), backend.Key{Service: serviceName, Scope: scopeOf(testProject, testRegion)}))
	expectClosed(t, openAddress)
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/open", nil), http.StatusNotFound)
	expectReply(t, client(t, endpoint(t, peer)), "PONG", "PING")
}

// exerciseAuth checks AUTH enforcement and the documented blocked commands.
func exerciseAuth(t *testing.T, address, auth string) {
	t.Helper()
	conn := client(t, address)
	expectError(t, conn, "NOAUTH", "PING")
	expectError(t, conn, "WRONGPASS", "AUTH", "not-the-auth-string")
	expectError(t, conn, "NOAUTH", "GET", "greeting")
	expectReply(t, conn, "OK", "AUTH", auth)
	expectReply(t, conn, "OK", "SET", "greeting", "kept")
	expectReply(t, conn, "kept", "GET", "greeting")
	for _, blocked := range [][]string{{"CONFIG", "GET", "maxmemory"}, {"SHUTDOWN"}, {"ACL", "WHOAMI"}, {"CLIENT", "LIST"}, {"BGSAVE"}} {
		expectError(t, conn, "NOPERM", blocked...)
	}
}

func endpoint(t *testing.T, operation map[string]any) string {
	t.Helper()
	response, _ := operation["response"].(map[string]any)
	host, _ := response["host"].(string)
	port, _ := response["port"].(float64)
	if host != "127.0.0.1" || port == 0 {
		t.Fatalf("instance endpoint = %v", response)
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

func client(t *testing.T, address string) *respConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, err := dialRESP(ctx, address)
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func expectReply(t *testing.T, conn *respConn, want string, args ...string) {
	t.Helper()
	if reply, err := conn.do(args...); err != nil || reply != want {
		t.Fatalf("%v = %q, %v; want %q", args[0], reply, err, want)
	}
}

func expectError(t *testing.T, conn *respConn, prefix string, args ...string) {
	t.Helper()
	_, err := conn.do(args...)
	if !errors.Is(err, errServerReply) || !strings.Contains(err.Error(), ": "+prefix) {
		t.Fatalf("%v error = %v; want %s", args, err, prefix)
	}
}

func expectClosed(t *testing.T, address string) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).Dial("tcp", address)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("%s still accepts connections", address)
	}
}
