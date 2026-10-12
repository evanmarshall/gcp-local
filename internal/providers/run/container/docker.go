package container

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
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
	config DockerConfig
}

// DockerConfig selects how revision containers are attached and labeled.
type DockerConfig struct {
	// Socket is the Docker Engine unix socket.
	Socket string
	// Network attaches revisions to this engine network and dials them by
	// their address on it. Empty publishes each revision's port on the
	// default bridge, for a core that runs on the engine host.
	Network string
	// Environment labels every revision container so a sweep removes only
	// this environment's containers. Required with Network.
	Environment string
}

// Container labels the runtime writes and sweeps by.
const (
	labelService     = "gcp-local.service"
	labelEnvironment = "gcp-local.environment"
)

// pullTimeout bounds one anonymous image pull during a revision launch.
const pullTimeout = 5 * time.Minute

// Docker runtime configuration and image refusals.
var (
	errDockerSocket      = errors.New("the Docker runtime needs an absolute engine socket path")
	errDockerEnvironment = errors.New("the Docker runtime environment label must match [a-z0-9][a-z0-9_-]{0,62}")
	errDockerNetwork     = errors.New("a Docker network needs an environment label and a name matching [A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
	errNoEnvironment     = errors.New("sweeping revision containers needs an environment label")
	errNoLocalAddress    = errors.New("the core has no interface address on the Docker network")
)

// Shapes of the configuration values and image references the runtime accepts.
var (
	environmentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	networkPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	// imagePattern is the Docker reference grammar without uppercase
	// repository names: [registry[:port]/]path[:tag][@sha256:digest].
	imagePattern = regexp.MustCompile(`^(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*(?::[0-9]{1,5})?/)?` +
		`[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*` +
		`(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?$`)
)

// NewDockerRuntime validates the configuration and builds a Docker-backed Runtime.
func NewDockerRuntime(config DockerConfig) (*DockerRuntime, error) {
	if !strings.HasPrefix(config.Socket, "/") {
		return nil, errDockerSocket
	}
	if config.Environment != "" && !environmentPattern.MatchString(config.Environment) {
		return nil, errDockerEnvironment
	}
	if config.Network != "" && (config.Environment == "" || !networkPattern.MatchString(config.Network)) {
		return nil, errDockerNetwork
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", config.Socket)
		},
	}
	return &DockerRuntime{client: &http.Client{Transport: transport}, config: config}, nil
}

// engineError is a non-success Engine API response.
type engineError struct {
	method, path, text string
	code               int
}

func (e *engineError) Error() string {
	return fmt.Sprintf("Docker engine %s %s returned %d: %s", e.method, e.path, e.code, e.text)
}

// GRPCStatus reports engine failures as an unavailable dependency.
func (e *engineError) GRPCStatus() *status.Status { return status.New(codes.Unavailable, e.Error()) }

// engineCode returns the Engine API status of err, or 0 for other failures.
func engineCode(err error) int {
	if engine, ok := errors.AsType[*engineError](err); ok {
		return engine.code
	}
	return 0
}

// send issues one Engine API request and returns the open response.
func (d *DockerRuntime) send(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode the engine request: %w", err)
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, payload)
	if err != nil {
		return nil, fmt.Errorf("build the engine request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	// No X-Registry-Auth header is ever sent: pulls are anonymous.
	response, err := d.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("reach the Docker engine at %s: %w", d.config.Socket, err)
	}
	if response.StatusCode >= 300 {
		defer func() { _ = response.Body.Close() }()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
		return nil, &engineError{method: method, path: path, code: response.StatusCode, text: strings.TrimSpace(string(data))}
	}
	return response, nil
}

// call issues one Engine API request and decodes the JSON response.
func (d *DockerRuntime) call(ctx context.Context, method, path string, body any, out any) error {
	response, err := d.send(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read the engine response: %w", err)
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode the engine response: %w", err)
		}
	}
	return nil
}

// pullRefusal reports why an image missing from the engine is not pulled.
// Google registries would mean pulling from real GCP, and the gcp-local/
// namespace names local images that a public registry must never supply.
func pullRefusal(image string) error {
	registry, _, found := strings.Cut(image, "/")
	if !found || !strings.ContainsAny(registry, ".:") && registry != "localhost" {
		registry = ""
	}
	host, _, _ := strings.Cut(registry, ":")
	switch {
	case host == "gcr.io" || strings.HasSuffix(host, ".gcr.io") || host == "pkg.dev" || strings.HasSuffix(host, ".pkg.dev"):
		return status.Errorf(codes.FailedPrecondition, "Image %s is not present in the local engine; gcp-local never pulls from Google registries (Artifact Registry is repository-only). Build or docker tag the image locally", image)
	case registry == "" && strings.HasPrefix(image, "gcp-local/"):
		return status.Errorf(codes.FailedPrecondition, "Image %s is not present in the local engine; gcp-local/ images are local names and are never pulled", image)
	}
	return nil
}

// ensureImage uses an image the engine already has, or pulls a public one
// anonymously.
func (d *DockerRuntime) ensureImage(ctx context.Context, image string) error {
	if !imagePattern.MatchString(image) {
		return status.Errorf(codes.InvalidArgument, "Image %q is not a Docker image reference", image)
	}
	err := d.call(ctx, http.MethodGet, "/images/"+image+"/json", nil, nil)
	if engineCode(err) != http.StatusNotFound {
		return err
	}
	if refused := pullRefusal(image); refused != nil {
		return refused
	}
	pull, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()
	response, err := d.send(pull, http.MethodPost, "/images/create?"+url.Values{"fromImage": {image}}.Encode(), nil)
	if err != nil {
		return fmt.Errorf("pull %s: %w", image, err)
	}
	defer func() { _ = response.Body.Close() }()
	return pullResult(image, response.Body)
}

// pullResult reads the pull progress stream; the engine reports failures in
// it after a 200 status.
func pullResult(image string, stream io.Reader) error {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var progress struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &progress) == nil && progress.Error != "" {
			return status.Errorf(codes.FailedPrecondition, "Pull %s anonymously failed: %s", image, progress.Error)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read the pull of %s: %w", image, err)
	}
	return nil
}

// createBody is the engine container definition for one revision: only the
// image, the provider-built environment, and the runtime's labels and network.
func (d *DockerRuntime) createBody(spec StartSpec) map[string]any {
	labels := map[string]string{labelService: "run"}
	if d.config.Environment != "" {
		labels[labelEnvironment] = d.config.Environment
	}
	host := map[string]any{
		"NetworkMode": "bridge",
		"SecurityOpt": []string{"no-new-privileges:true"},
	}
	if d.config.Network != "" {
		host["NetworkMode"] = d.config.Network
	} else {
		host["PublishAllPorts"] = true
	}
	return map[string]any{
		"Image":        spec.Image,
		"Env":          spec.Env,
		"Labels":       labels,
		"ExposedPorts": map[string]any{strconv.Itoa(spec.Port) + "/tcp": map[string]any{}},
		"HostConfig":   host,
	}
}

// containerState is the part of a container inspection the runtime reads.
type containerState struct {
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

// Start creates and starts the revision container and returns the address
// the invocation proxy dials and the address its own connections come from.
func (d *DockerRuntime) Start(ctx context.Context, spec StartSpec) (Started, error) {
	if err := d.ensureImage(ctx, spec.Image); err != nil {
		return Started{}, err
	}
	// A unique engine name keeps concurrent revisions addressable.
	suffix, err := cryptoRandSuffix()
	if err != nil {
		return Started{}, fmt.Errorf("derive the container name: %w", err)
	}
	created := struct {
		ID string `json:"Id"`
	}{}
	if err := d.call(ctx, http.MethodPost, "/containers/create?name=gcp-local-run-"+suffix, d.createBody(spec), &created); err != nil {
		return Started{}, err
	}
	started, err := d.launch(ctx, created.ID, spec.Port)
	if err != nil {
		_ = d.Stop(ctx, created.ID)
		return Started{}, err
	}
	return started, nil
}

// launch starts a created container and resolves its addresses.
func (d *DockerRuntime) launch(ctx context.Context, id string, port int) (Started, error) {
	if err := d.call(ctx, http.MethodPost, "/containers/"+id+"/start", nil, nil); err != nil {
		return Started{}, err
	}
	var inspect containerState
	if err := d.call(ctx, http.MethodGet, "/containers/"+id+"/json", nil, &inspect); err != nil {
		return Started{}, err
	}
	return d.addresses(id, port, inspect)
}

// addresses derives the dial and peer addresses from one inspection.
func (d *DockerRuntime) addresses(id string, port int, inspect containerState) (Started, error) {
	network := d.config.Network
	if network == "" {
		network = "bridge"
	}
	peer := inspect.NetworkSettings.Networks[network].IPAddress
	if peer == "" {
		return Started{}, status.Errorf(codes.Internal, "The container has no address on network %s", network)
	}
	if d.config.Network != "" {
		return Started{Address: net.JoinHostPort(peer, strconv.Itoa(port)), Container: id, Peer: peer}, nil
	}
	key := strconv.Itoa(port) + "/tcp"
	bindings := inspect.NetworkSettings.Ports[key]
	if len(bindings) == 0 {
		return Started{}, status.Errorf(codes.Internal, "The engine exposed no port for %s", key)
	}
	// A published port binds every interface; the loopback form is the one
	// the invocation proxy may dial.
	host := bindings[0].HostIP
	if host == "0.0.0.0" || host == "::" || host == "" {
		host = "127.0.0.1"
	}
	return Started{Address: net.JoinHostPort(host, bindings[0].HostPort), Container: id, Peer: peer}, nil
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
	if err := d.call(ctx, http.MethodPost, "/containers/"+container+"/stop?t=10", nil, nil); err != nil && engineCode(err) != http.StatusNotModified && engineCode(err) != http.StatusNotFound {
		return fmt.Errorf("stop the container: %w", err)
	}
	return d.remove(ctx, container)
}

// remove force-removes one container; a missing container is success.
func (d *DockerRuntime) remove(ctx context.Context, container string) error {
	if err := d.call(ctx, http.MethodDelete, "/containers/"+container+"?force=true", nil, nil); err != nil && engineCode(err) != http.StatusNotFound {
		return fmt.Errorf("remove the container: %w", err)
	}
	return nil
}

// Sweep force-removes every container labeled with this runtime's
// environment, running or not. The core sweeps at startup (a previous core may
// have exited without cleanup) and at shutdown. It never touches containers
// of another environment.
func (d *DockerRuntime) Sweep(ctx context.Context) error {
	if d.config.Environment == "" {
		return errNoEnvironment
	}
	filters, err := json.Marshal(map[string][]string{"label": {labelEnvironment + "=" + d.config.Environment}})
	if err != nil {
		return fmt.Errorf("encode the sweep filter: %w", err)
	}
	var listed []struct {
		ID string `json:"Id"`
	}
	if err := d.call(ctx, http.MethodGet, "/containers/json?"+url.Values{"all": {"true"}, "filters": {string(filters)}}.Encode(), nil, &listed); err != nil {
		return fmt.Errorf("list revision containers: %w", err)
	}
	var failures []error
	for _, entry := range listed {
		failures = append(failures, d.remove(ctx, entry.ID))
	}
	return errors.Join(failures...)
}

// LocalAddress returns the core's own address on the configured network: the
// address revision containers dial for their metadata server.
func (d *DockerRuntime) LocalAddress(ctx context.Context) (string, error) {
	var network struct {
		IPAM struct {
			Config []struct {
				Subnet string `json:"Subnet"`
			} `json:"Config"`
		} `json:"IPAM"`
	}
	if err := d.call(ctx, http.MethodGet, "/networks/"+d.config.Network, nil, &network); err != nil {
		return "", fmt.Errorf("inspect network %s: %w", d.config.Network, err)
	}
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("list interface addresses: %w", err)
	}
	subnets := make([]string, 0, len(network.IPAM.Config))
	for _, entry := range network.IPAM.Config {
		subnets = append(subnets, entry.Subnet)
	}
	return addressIn(subnets, addresses)
}

// addressIn returns the first interface address inside one of the subnets.
func addressIn(subnets []string, addresses []net.Addr) (string, error) {
	for _, subnet := range subnets {
		_, block, err := net.ParseCIDR(subnet)
		if err != nil {
			continue
		}
		for _, address := range addresses {
			if candidate, ok := address.(*net.IPNet); ok && block.Contains(candidate.IP) {
				return candidate.IP.String(), nil
			}
		}
	}
	return "", errNoLocalAddress
}

// cryptoRandSuffix derives a short unique engine-name suffix.
func cryptoRandSuffix() (string, error) {
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
