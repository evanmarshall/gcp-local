package dataplane

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"
)

// freeRange finds n consecutive loopback ports that are currently free.
func freeRange(t *testing.T, n int) int {
	t.Helper()
	var listen net.ListenConfig
	for range 50 {
		probe, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address, isTCP := probe.Addr().(*net.TCPAddr)
		_ = probe.Close()
		if !isTCP || address.Port+n > 65535 {
			continue
		}
		first := address.Port
		ok := true
		for port := first; port < first+n; port++ {
			l, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:"+strconv.Itoa(port))
			if err != nil {
				ok = false
				break
			}
			_ = l.Close()
		}
		if ok {
			return first
		}
	}
	t.Fatal("no free port range")
	return 0
}

func TestParse(t *testing.T) {
	pool, err := Parse("", "127.0.0.1")
	if pool != nil || err != nil {
		t.Fatalf("empty range = %v, %v; want disabled", pool, err)
	}
	for _, bad := range []string{"80-90", "5000", "5000-4999", "5000-70000", "5000-5256", "a-b"} {
		if _, err := Parse(bad, "127.0.0.1"); !errors.Is(err, ErrRange) {
			t.Errorf("Parse(%q) = %v; want ErrRange", bad, err)
		}
	}
	if _, err := Parse("5000-5001", "localhost"); !errors.Is(err, ErrListen) {
		t.Errorf("hostname listen = %v; want ErrListen", err)
	}
	if _, err := Parse("5000-5255", "0.0.0.0"); err != nil {
		t.Errorf("256-port range: %v", err)
	}
}

// echo serves one line back to the client.
func echo(_ context.Context, conn net.Conn) {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err == nil {
		_, _ = conn.Write([]byte(line))
	}
}

func TestAllocationReusesAndExhausts(t *testing.T) {
	first := freeRange(t, 2)
	pool, err := Parse(strconv.Itoa(first)+"-"+strconv.Itoa(first+1), "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	a, err := pool.Open(t.Context(), echo)
	if err != nil {
		t.Fatal(err)
	}
	b, err := pool.Open(t.Context(), echo)
	if err != nil {
		t.Fatal(err)
	}
	if a.Port() != first || b.Port() != first+1 {
		t.Fatalf("ports = %d, %d; want %d, %d", a.Port(), b.Port(), first, first+1)
	}
	if _, err := pool.Open(t.Context(), echo); !errors.Is(err, ErrExhausted) {
		t.Fatalf("third open = %v; want ErrExhausted", err)
	}
	a.Close()
	c, err := pool.Open(t.Context(), echo)
	if err != nil || c.Port() != first {
		t.Fatalf("reopen = %v, %v; want the released port %d", c, err, first)
	}
	b.Close()
	c.Close()
}

func TestAllocationSkipsForeignPorts(t *testing.T) {
	first := freeRange(t, 2)
	var listen net.ListenConfig
	foreign, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:"+strconv.Itoa(first))
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	pool, _ := Parse(strconv.Itoa(first)+"-"+strconv.Itoa(first+1), "127.0.0.1")
	l, err := pool.Open(t.Context(), echo)
	if err != nil || l.Port() != first+1 {
		t.Fatalf("open = %v, %v; want port %d", l, err, first+1)
	}
	l.Close()
}

// echoLines answers every line on one connection until it ends.
func echoLines(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		_, _ = conn.Write([]byte("echo " + line))
	}
}

// upstreamServer stands in for a provider-owned server on loopback.
func upstreamServer(t *testing.T) net.Listener {
	t.Helper()
	var listen net.ListenConfig
	upstream, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	go func() {
		for {
			conn, err := upstream.Accept()
			if err != nil {
				return
			}
			go echoLines(conn)
		}
	}()
	return upstream
}

func TestForwardAndCloseEndsSessions(t *testing.T) {
	upstream := upstreamServer(t)
	first := freeRange(t, 1)
	pool, _ := Parse(strconv.Itoa(first)+"-"+strconv.Itoa(first), "127.0.0.1")
	published, err := pool.Open(t.Context(), Forward(upstream.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	var dialer net.Dialer
	client, err := dialer.DialContext(t.Context(), "tcp", net.JoinHostPort(AdvertisedHost, strconv.Itoa(published.Port())))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := bufio.NewReader(client)
	if _, err := client.Write([]byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != "echo hi\n" {
		t.Fatalf("forwarded reply = %q, %v", line, err)
	}
	published.Close()
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := reader.ReadString('\n'); err == nil {
		t.Fatal("an open session survived Close")
	}
	if _, err := dialer.DialContext(t.Context(), "tcp", net.JoinHostPort(AdvertisedHost, strconv.Itoa(published.Port()))); err == nil {
		t.Fatal("the port still accepts connections after Close")
	}
}
