package run

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DockerRuntime drives one Cloud Run revision container through the Docker
// Engine API on a local unix socket. It is the only component with host access;
// every other decision stays in the provider.
type DockerRuntime struct {
	client *http.Client
	socket string
}

// DefaultDockerSocket is the local engine socket this runtime probes first.
const DefaultDockerSocket = "/var/run/docker.sock"

// NewDockerRuntime builds a Docker-backed Runtime for the given socket path.
func NewDockerRuntime(socket string) *DockerRuntime {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
	}
	return &DockerRuntime{client: &http.Client{Transport: transport}, socket: socket}
}

// call issues one Engine API request and decodes the JSON response.
func (d *DockerRuntime) call(ctx context.Context, method, path string, body any, out any) error {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode the engine request: %w", err)
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, payload)
	if err != nil {
		return fmt.Errorf("build the engine request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
	if err != nil {
		return fmt.Errorf("reach the Docker engine at %s: %w", d.socket, err)
	}
	defer func() { _ = response.Body.Close() }()
	return d.consume(method, path, response, out)
}

// consume drains one engine response and decodes its JSON body.
func (d *DockerRuntime) consume(method, path string, response *http.Response, out any) error {
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read the engine response: %w", err)
	}
	if response.StatusCode >= 300 {
		return status.Errorf(codes.Unavailable, "Docker engine %s %s returned %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode the engine response: %w", err)
		}
	}
	return nil
}

// Start creates and starts the revision container and returns its loopback
// address with the bound host port.
func (d *DockerRuntime) Start(ctx context.Context, spec StartSpec) (string, error) {
	created := struct {
		ID string `json:"Id"`
	}{}
	// A unique engine name keeps concurrent revisions addressable.
	suffix, err := cryptoRandSuffix()
	if err != nil {
		return "", fmt.Errorf("derive the container name: %w", err)
	}
	config := map[string]any{
		"Image": spec.Image,
		"Env":   spec.Env,
		"Labels": map[string]string{
			"gcp-local.service": "run",
		},
		"HostConfig": map[string]any{
			"PublishAllPorts": true,
			"NetworkMode":     "bridge",
		},
	}
	if err := d.call(ctx, http.MethodPost, "/containers/create?name=gcp-local-run-"+suffix, config, &created); err != nil {
		return "", err
	}
	if err := d.call(ctx, http.MethodPost, "/containers/"+created.ID+"/start", nil, nil); err != nil {
		_ = d.Stop(ctx, created.ID)
		return "", err
	}
	var inspect struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := d.call(ctx, http.MethodGet, "/containers/"+created.ID+"/json", nil, &inspect); err != nil {
		_ = d.Stop(ctx, created.ID)
		return "", err
	}
	key := fmt.Sprintf("%d/tcp", spec.Port)
	bindings := inspect.NetworkSettings.Ports[key]
	if len(bindings) == 0 {
		_ = d.Stop(ctx, created.ID)
		return "", status.Errorf(codes.Internal, "The engine exposed no port for %s", key)
	}
	return bindings[0].HostIP + ":" + bindings[0].HostPort, nil
}

// Ready blocks until the container exits and reports its failure, or nil while
// it keeps running.
func (d *DockerRuntime) Ready(ctx context.Context, container string) error {
	var waited struct {
		StatusCode int    `json:"StatusCode"`
		Error      string `json:"Error"`
	}
	if err := d.call(ctx, http.MethodPost, "/containers/"+container+"/wait", nil, &waited); err != nil {
		return fmt.Errorf("await the container: %w", err)
	}
	if waited.Error != "" || waited.StatusCode != 0 {
		return status.Errorf(codes.Internal, "The container exited with status %d: %s", waited.StatusCode, waited.Error)
	}
	return nil
}

// Stop terminates and removes the container; a missing container is success.
func (d *DockerRuntime) Stop(ctx context.Context, container string) error {
	if container == "" {
		return nil
	}
	if err := d.call(ctx, http.MethodPost, "/containers/"+container+"/stop?t=10", nil, nil); err != nil && !strings.Contains(err.Error(), "304") && !strings.Contains(err.Error(), "404") {
		return fmt.Errorf("stop the container: %w", err)
	}
	if err := d.call(ctx, http.MethodDelete, "/containers/"+container+"?force=true", nil, nil); err != nil && !strings.Contains(err.Error(), "404") {
		return fmt.Errorf("remove the container: %w", err)
	}
	return nil
}

// cryptoRandSuffix derives a short unique engine-name suffix.
func cryptoRandSuffix() (string, error) {
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
