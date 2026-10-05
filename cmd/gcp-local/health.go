package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

func health(args []string, output, errors io.Writer) int {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	flags.SetOutput(errors)
	endpoint := flags.String("endpoint", "http://127.0.0.1:4566", "local health endpoint")
	if flags.Parse(args) != nil {
		return 2
	}
	parsed, err := url.Parse(*endpoint)
	if err != nil {
		fmt.Fprintln(errors, "Invalid health endpoint")
		return 2
	}
	ip := net.ParseIP(parsed.Hostname())
	if flags.NArg() != 0 || parsed.Scheme != "http" || ip == nil || !ip.IsLoopback() || parsed.Port() == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		fmt.Fprintln(errors, "Select a numeric loopback HTTP endpoint with an explicit port")
		return 2
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(*endpoint + "/_local/health")
	if err != nil {
		fmt.Fprintln(errors, "Unable to reach gcp-local")
		return 1
	}
	defer response.Body.Close()
	var result map[string]any
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result["service"] != "gcp-local" || result["ready"] != true {
		fmt.Fprintln(errors, "gcp-local is not ready")
		return 1
	}
	_ = json.NewEncoder(output).Encode(result)
	return 0
}
