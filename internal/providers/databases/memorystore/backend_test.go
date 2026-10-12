package memorystore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServerConfigEnforcesAuthAndBlockedCommands(t *testing.T) {
	open := serverConfig(16379, "/tmp/x", "adminpw", launchSpec{MemoryGB: 2})
	for _, want := range []string{"bind 127.0.0.1\n", "port 16379\n", "maxmemory 2gb\n", "maxmemory-policy volatile-lru\n",
		"user default on nopass ~* &* +@all -acl -bgrewriteaof -bgsave -client -cluster -command -config -debug -lastsave -migrate -object -replicaof -slaveof -save -shutdown\n",
		"user gcp-local-admin on >adminpw ~* &* +@all\n", `save ""` + "\n"} {
		if !strings.Contains(open, want) {
			t.Fatalf("configuration lacks %q:\n%s", want, open)
		}
	}
	secured := serverConfig(16379, "/tmp/x", "adminpw", launchSpec{AuthString: "auth-string", MemoryGB: 1})
	if !strings.Contains(secured, "user default on >auth-string ~* ") || strings.Contains(secured, "nopass") {
		t.Fatalf("AUTH-enabled configuration allows unauthenticated clients:\n%s", secured)
	}
}

func TestConfigRejectsUnpinnedServers(t *testing.T) {
	ctx := context.Background()
	if err := (Config{Server: "redis-server"}).validate(ctx); !errors.Is(err, errServerPath) {
		t.Fatalf("relative path = %v", err)
	}
	directory := t.TempDir()
	if err := (Config{Server: filepath.Join(directory, "missing")}).validate(ctx); !errors.Is(err, errServerMissing) {
		t.Fatalf("missing binary = %v", err)
	}
	fake := filepath.Join(directory, "redis-server")
	must(t, os.WriteFile(fake, []byte("#!/bin/sh\necho 'Redis server v=6.2.0 sha=0 malloc=libc bits=64'\n"), 0o700)) //nolint:gosec // the fake server must be executable
	if err := (Config{Server: fake}).validate(ctx); !errors.Is(err, errServerVersion) {
		t.Fatalf("unpinned version = %v", err)
	}
	must(t, os.WriteFile(fake, []byte("#!/bin/sh\necho 'Redis server v="+ServerVersion+" sha=0 malloc=libc bits=64'\n"), 0o700)) //nolint:gosec // the fake server must be executable
	must(t, (Config{Server: fake}).validate(ctx))
}

func TestRESPClientReplies(t *testing.T) {
	address := scriptedServer(t, []string{"+PONG\r\n", "-NOPERM blocked\r\n", "$5\r\nhello\r\n", ":7\r\n", "$-1\r\n"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialRESP(ctx, address)
	must(t, err)
	defer conn.Close()
	if reply, err := conn.do("PING"); err != nil || reply != "PONG" {
		t.Fatalf("PING = %q, %v", reply, err)
	}
	if _, err := conn.do("CONFIG", "GET", "*"); !errors.Is(err, errServerReply) || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("error reply = %v", err)
	}
	for _, want := range []string{"hello", "7", ""} {
		if reply, err := conn.do("GET", "k"); err != nil || reply != want {
			t.Fatalf("reply = %q, %v; want %q", reply, err, want)
		}
	}
}

// scriptedServer answers one connection's commands with fixed raw replies.
func scriptedServer(t *testing.T, replies []string) string {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
	must(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for _, reply := range replies {
			if _, err := readCommand(reader); err != nil {
				return
			}
			_, _ = conn.Write([]byte(reply))
		}
	}()
	return listener.Addr().String()
}

// readCommand consumes one RESP array request.
func readCommand(reader *bufio.Reader) ([]string, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	var count int
	for _, r := range strings.TrimSpace(header)[1:] {
		count = count*10 + int(r-'0')
	}
	args := make([]string, 0, count)
	for range count {
		if _, err := reader.ReadString('\n'); err != nil {
			return nil, fmt.Errorf("read length: %w", err)
		}
		value, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read argument: %w", err)
		}
		args = append(args, strings.TrimSuffix(value, "\r\n"))
	}
	return args, nil
}
