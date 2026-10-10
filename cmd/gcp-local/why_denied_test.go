package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWhyDeniedUsesOnlyExplicitLocalDiagnostics(t *testing.T) {
	id := "1234567890abcdef1234567890abcdef"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_local/iam/decisions/"+id {
			t.Error("wrong decision route")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"outcome":"deny","reason":"Missing permission: storage.objects.get"}`))
	}))
	defer server.Close()
	var output bytes.Buffer
	if code := run(t.Context(), []string{"why-denied", endpointFlag, server.URL, id}, &output, &output); code != 0 || !bytes.Contains(output.Bytes(), []byte("storage.objects.get")) {
		t.Fatalf("%d %s", code, output.String())
	}
	for _, args := range [][]string{{endpointFlag, "https://iam.googleapis.com", id}, {endpointFlag, server.URL, "invalid"}, {endpointFlag, server.URL + "/other", id}} {
		output.Reset()
		if code := whyDenied(t.Context(), args, &output, &output); code != 2 {
			t.Fatal("accepted unsafe endpoint or ID")
		}
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://iam.googleapis.com")
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()
	output.Reset()
	if code := whyDenied(t.Context(), []string{endpointFlag, redirect.URL, id}, &output, &output); code != 1 {
		t.Fatal("followed diagnostics redirect")
	}
}
