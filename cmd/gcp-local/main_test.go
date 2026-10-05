package main

import (
	"bytes"
	"context"
	"net"
	"testing"
)

func TestVersionAndInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"-version"}, 0}, {[]string{"-shutdown-timeout=0"}, 2}, {[]string{"unexpected"}, 2}, {[]string{"-unknown"}, 2},
	} {
		var output bytes.Buffer
		if code := run(context.Background(), tc.args, &output, &output); code != tc.code {
			t.Fatalf("%v: code %d", tc.args, code)
		}
	}
}

func TestBindFailureIsReported(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var output bytes.Buffer
	if code := run(context.Background(), []string{"-listen", listener.Addr().String()}, &output, &output); code != 1 {
		t.Fatalf("code %d", code)
	}
}
