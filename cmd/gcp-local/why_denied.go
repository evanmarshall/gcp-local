package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

func whyDenied(args []string, output, errors io.Writer) int {
	flags := flag.NewFlagSet("why-denied", flag.ContinueOnError)
	flags.SetOutput(errors)
	endpoint := flags.String("endpoint", "http://127.0.0.1:4566", "local diagnostics endpoint")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	parsed, err := url.Parse(*endpoint)
	if err != nil {
		fmt.Fprintln(errors, "Invalid local diagnostics endpoint")
		return 2
	}
	ip := net.ParseIP(parsed.Hostname())
	if parsed.Scheme != "http" || ip == nil || !ip.IsLoopback() || parsed.Port() == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || flags.NArg() != 1 {
		fmt.Fprintln(errors, "Supply one decision ID and a numeric loopback HTTP endpoint with a port")
		return 2
	}
	id, err := hex.DecodeString(flags.Arg(0))
	if err != nil || len(id) != 16 {
		fmt.Fprintln(errors, "Supply a 32-character hexadecimal decision ID")
		return 2
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(*endpoint + "/_local/iam/decisions/" + flags.Arg(0))
	if err != nil {
		fmt.Fprintln(errors, "Unable to reach local IAM diagnostics")
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		fmt.Fprintln(errors, "Decision unavailable; it may be unknown or evicted")
		return 1
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		fmt.Fprintln(errors, "Unable to read decision")
		return 1
	}
	var formatted bytes.Buffer
	if json.Indent(&formatted, raw, "", "  ") != nil {
		fmt.Fprintln(errors, "Invalid diagnostics response")
		return 1
	}
	fmt.Fprintln(output, formatted.String())
	return 0
}
