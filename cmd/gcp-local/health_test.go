package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthRequiresReadyLocalRuntime(t *testing.T) {
	for _, tc := range []struct {
		body string
		want int
	}{
		{`{"service":"gcp-local","ready":true}`, 0},
		{`{"service":"gcp-local","ready":false}`, 1},
		{`{"service":"another-server","ready":true}`, 1},
		{`not JSON`, 1},
	} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/_local/health" {
				t.Error("unexpected health path")
			}
			_, _ = w.Write([]byte(tc.body))
		}))
		var output, errors bytes.Buffer
		if got := run(context.Background(), []string{"health", "-endpoint", s.URL}, &output, &errors); got != tc.want {
			t.Fatalf("health(%s) = %d, want %d", tc.body, got, tc.want)
		}
		s.Close()
	}
	var output, errors bytes.Buffer
	if run(context.Background(), []string{"health", "-endpoint", "https://storage.googleapis.com"}, &output, &errors) != 2 {
		t.Fatal("health accepted a cloud endpoint")
	}
}
