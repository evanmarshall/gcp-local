package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
)

func TestVersionAndInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"-version"}, 0}, {[]string{"-shutdown-timeout=0"}, 2}, {[]string{"unexpected"}, 2}, {[]string{"-unknown"}, 2},
		{[]string{"-data-ports=6379-6380", "-version"}, 0}, {[]string{"-data-ports=80-90"}, 2},
		{[]string{"-data-ports=4568-4583", "-data-listen=localhost"}, 2},
	} {
		var output bytes.Buffer
		if code := run(context.Background(), tc.args, &output, &output); code != tc.code {
			t.Fatalf("%v: code %d", tc.args, code)
		}
	}
}

// listenTestAddress opens a loopback listener through the context-aware API.
func listenTestAddress() (net.Listener, error) {
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on loopback: %w", err)
	}
	return listener, nil
}

func TestBindFailureIsReported(t *testing.T) {
	listener, err := listenTestAddress()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-listen", listener.Addr().String()}, &output, &output); code != 1 {
		t.Fatalf("code %d", code)
	}
}

func TestRunBaseURLNamesWildcardListenersByLoopback(t *testing.T) {
	for listen, want := range map[string]string{
		"0.0.0.0:4566":   "http://127.0.0.1:4566",
		":14566":         "http://127.0.0.1:14566",
		"[::]:4566":      "http://127.0.0.1:4566",
		"127.0.0.1:9298": "http://127.0.0.1:9298",
		"[::1]:9298":     "http://[::1]:9298",
	} {
		if got := runBaseURL(listen); got != want {
			t.Fatalf("%s: got %s, want %s", listen, got, want)
		}
	}
}
