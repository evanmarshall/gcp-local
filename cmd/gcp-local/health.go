package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

func health(ctx context.Context, args []string, output, errors io.Writer) int {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	flags.SetOutput(errors)
	endpoint := flags.String("endpoint", "http://127.0.0.1:4566", "local health endpoint")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validLoopbackEndpoint(*endpoint) {
		fmt.Fprintln(errors, "Select a numeric loopback HTTP endpoint with an explicit port")
		return 2
	}
	result, err := readHealth(ctx, *endpoint)
	if err != nil {
		fmt.Fprintln(errors, err)
		return 1
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		fmt.Fprintln(errors, "Unable to write the health response:", err)
		return 1
	}
	return 0
}

// health fetch failures reported verbatim to the operator.
var (
	errHealthReady  = errors.New("gcp-local is not ready")
	errHealthReach  = errors.New("unable to reach gcp-local")
	errHealthDecode = errors.New("invalid gcp-local health response")
)

// readHealth reads the local health endpoint and requires the ready gcp-local
// service, rejecting redirects and oversized responses.
func readHealth(ctx context.Context, endpoint string) (map[string]any, error) {
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/_local/health", nil)
	if err != nil {
		return nil, errHealthReady
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errHealthReach
	}
	defer response.Body.Close()
	var result map[string]any
	if response.StatusCode != http.StatusOK {
		return nil, errHealthReady
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result["service"] != "gcp-local" || result["ready"] != true {
		return nil, errHealthDecode
	}
	return result, nil
}
