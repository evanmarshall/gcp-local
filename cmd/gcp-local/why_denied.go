package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

func whyDenied(ctx context.Context, args []string, output, errors io.Writer) int {
	endpoint, decision, ok := parseWhyDenied(args, errors)
	if !ok {
		return 2
	}
	body, err := fetchDecision(ctx, endpoint, decision)
	if err != nil {
		fmt.Fprintln(errors, err)
		return 1
	}
	var formatted bytes.Buffer
	if json.Indent(&formatted, body, "", "  ") != nil {
		fmt.Fprintln(errors, "Invalid diagnostics response")
		return 1
	}
	fmt.Fprintln(output, formatted.String())
	return 0
}

// parseWhyDenied reads the endpoint flag and one decision ID.
func parseWhyDenied(args []string, errors io.Writer) (string, string, bool) {
	flags := flag.NewFlagSet("why-denied", flag.ContinueOnError)
	flags.SetOutput(errors)
	endpoint := flags.String("endpoint", "http://127.0.0.1:4566", "local diagnostics endpoint")
	if flags.Parse(args) != nil || flags.NArg() != 1 || !validLoopbackEndpoint(*endpoint) {
		fmt.Fprintln(errors, "Supply one decision ID and a numeric loopback HTTP endpoint with a port")
		return "", "", false
	}
	id, err := hex.DecodeString(flags.Arg(0))
	if err != nil || len(id) != 16 {
		fmt.Fprintln(errors, "Supply a 32-character hexadecimal decision ID")
		return "", "", false
	}
	return *endpoint, flags.Arg(0), true
}

// validLoopbackEndpoint requires a plain numeric loopback HTTP URL with a port.
func validLoopbackEndpoint(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	ip := net.ParseIP(parsed.Hostname())
	return parsed.Scheme == "http" && ip != nil && ip.IsLoopback() && parsed.Port() != "" &&
		parsed.User == nil && parsed.Path == "" && parsed.RawQuery == "" && parsed.Fragment == ""
}

// Decision fetch failures reported verbatim to the operator.
var (
	errDecisionMissing = errors.New("decision unavailable; it may be unknown or evicted")
	errDecisionContact = errors.New("unable to reach local IAM diagnostics")
	errDecisionRead    = errors.New("unable to read decision")
)

// fetchDecision reads one stored IAM decision from the local diagnostics API.
func fetchDecision(ctx context.Context, endpoint, decision string) ([]byte, error) {
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/_local/iam/decisions/"+decision, nil)
	if err != nil {
		return nil, errDecisionMissing
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errDecisionContact
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errDecisionMissing
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, errDecisionRead
	}
	return raw, nil
}
