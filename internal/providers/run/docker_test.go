package run

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fixed values the fake engine reports.
const (
	testEnvironment = "gcp-local-0123456789abcdef"
	testNetwork     = testEnvironment + "_backend"
	testContainerIP = "172.30.0.5"
)

// fakeEngine is a minimal Docker Engine API on a unix socket. It records
// every request and serves the images it holds.
type fakeEngine struct {
	mu         sync.Mutex
	socket     string
	images     map[string]bool
	pullError  string
	requests   []string
	created    map[string]any
	authHeader bool
	listed     []string
	removed    []string
}

func newFakeEngine(t *testing.T, images ...string) *fakeEngine {
	t.Helper()
	// macOS limits unix socket paths to 104 bytes; t.TempDir is too deep.
	directory, err := os.MkdirTemp("", "engine") //nolint:usetesting // t.TempDir embeds the test name and overflows the 104-byte macOS unix socket path limit
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	engine := &fakeEngine{socket: filepath.Join(directory, "docker.sock"), images: map[string]bool{}}
	for _, image := range images {
		engine.images[image] = true
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(context.Background(), "unix", engine.socket)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(engine)
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return engine
}

func (f *fakeEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	if r.Header.Get("X-Registry-Auth") != "" {
		f.authHeader = true
	}
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/images/"):
		if !f.images[strings.TrimSuffix(strings.TrimPrefix(path, "/images/"), "/json")] {
			http.Error(w, `{"message":"No such image"}`, http.StatusNotFound)
		}
	case path == "/images/create":
		f.pull(w, r.URL.Query().Get("fromImage"))
	case path == "/containers/create":
		f.created = map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&f.created)
		_, _ = w.Write([]byte(`{"Id":"c1"}`))
	case path == "/containers/c1/json":
		_, _ = io.WriteString(w, `{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"0.0.0.0","HostPort":"49153"}]},"Networks":{"bridge":{"IPAddress":"172.17.0.9"},"`+testNetwork+`":{"IPAddress":"`+testContainerIP+`"}}}}`)
	case path == "/containers/json":
		f.listed = append(f.listed, r.URL.Query().Get("filters"))
		_, _ = w.Write([]byte(`[{"Id":"mine-1"},{"Id":"mine-2"}]`))
	case r.Method == http.MethodDelete:
		f.removed = append(f.removed, strings.TrimPrefix(path, "/containers/"))
	}
}

// pull serves one image pull progress stream.
func (f *fakeEngine) pull(w http.ResponseWriter, image string) {
	if f.pullError != "" {
		_, _ = io.WriteString(w, `{"status":"Pulling"}`+"\n"+`{"error":"`+f.pullError+`"}`+"\n")
		return
	}
	f.images[image] = true
	_, _ = io.WriteString(w, `{"status":"Downloaded newer image"}`+"\n")
}

func (f *fakeEngine) sent(request string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.requests, request)
}

func networkRuntime(t *testing.T, engine *fakeEngine) *DockerRuntime {
	t.Helper()
	docker, err := NewDockerRuntime(DockerConfig{Socket: engine.socket, Network: testNetwork, Environment: testEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	return docker
}

func TestDockerRuntimeRefusesIncompleteConfiguration(t *testing.T) {
	for _, config := range []DockerConfig{
		{Socket: "docker.sock"},
		{Socket: "/var/run/docker.sock", Environment: "Upper Case"},
		{Socket: "/var/run/docker.sock", Network: testNetwork},
		{Socket: "/var/run/docker.sock", Network: "bad network", Environment: testEnvironment},
	} {
		if _, err := NewDockerRuntime(config); err == nil {
			t.Errorf("%+v was accepted", config)
		}
	}
}

func TestDockerStartLabelsAndAttachesToTheEnvironmentNetwork(t *testing.T) {
	engine := newFakeEngine(t, "my-app:dev")
	started, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: "my-app:dev", Port: 8080, Env: []string{"PORT=8080"}})
	if err != nil {
		t.Fatal(err)
	}
	if started.Address != testContainerIP+":8080" || started.Peer != testContainerIP || started.Container != "c1" {
		t.Fatalf("started = %+v", started)
	}
	assertNetworkContainer(t, engine.created)
	if engine.sent("POST /images/create") {
		t.Fatal("a local image was pulled")
	}
}

// assertNetworkContainer requires the labeled, network-attached definition
// with only the provider's environment and no host access.
func assertNetworkContainer(t *testing.T, created map[string]any) {
	t.Helper()
	labels, _ := created["Labels"].(map[string]any)
	if labels[labelService] != "run" || labels[labelEnvironment] != testEnvironment {
		t.Fatalf("labels = %v", labels)
	}
	host, _ := created["HostConfig"].(map[string]any)
	if host["NetworkMode"] != testNetwork || host["PublishAllPorts"] != nil {
		t.Fatalf("host config = %v", host)
	}
	for _, key := range []string{"Binds", "Mounts", "Privileged", "PortBindings"} {
		if host[key] != nil {
			t.Fatalf("host config sets %s: %v", key, host)
		}
	}
	if env, _ := created["Env"].([]any); len(env) != 1 || env[0] != "PORT=8080" {
		t.Fatalf("env = %v", created["Env"])
	}
}

func TestDockerStartOnTheEngineHostPublishesTheRevisionPort(t *testing.T) {
	engine := newFakeEngine(t, "my-app:dev")
	docker, err := NewDockerRuntime(DockerConfig{Socket: engine.socket})
	if err != nil {
		t.Fatal(err)
	}
	started, err := docker.Start(context.Background(), StartSpec{Image: "my-app:dev", Port: 8080})
	if err != nil {
		t.Fatal(err)
	}
	if started.Address != "127.0.0.1:49153" || started.Peer != "172.17.0.9" {
		t.Fatalf("started = %+v", started)
	}
}

func TestDockerStartPullsMissingPublicImagesAnonymously(t *testing.T) {
	engine := newFakeEngine(t)
	if _, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: "nginxdemos/hello:0.4", Port: 8080}); err != nil {
		t.Fatal(err)
	}
	if !engine.sent("POST /images/create") || engine.authHeader {
		t.Fatalf("requests %v, credentials sent %v", engine.requests, engine.authHeader)
	}
	engine.pullError = "pull access denied"
	_, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: "private/app:1", Port: 8080})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "pull access denied") {
		t.Fatalf("failed pull = %v", err)
	}
}

func TestDockerStartRefusesGoogleRegistriesAndLocalNamesItCannotFind(t *testing.T) {
	for _, image := range []string{
		"us-central1-docker.pkg.dev/demo/repo/app:1",
		"gcr.io/demo/app:1",
		"eu.gcr.io/demo/app",
		"gcp-local/workflow:latest",
	} {
		engine := newFakeEngine(t)
		_, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: image, Port: 8080})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("%s: %v", image, err)
		}
		if engine.sent("POST /images/create") || engine.sent("POST /containers/create") {
			t.Errorf("%s reached the engine: %v", image, engine.requests)
		}
	}
	// A locally built image under the same names runs.
	engine := newFakeEngine(t, "us-central1-docker.pkg.dev/demo/repo/app:1")
	if _, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: "us-central1-docker.pkg.dev/demo/repo/app:1", Port: 8080}); err != nil {
		t.Fatal(err)
	}
}

func TestDockerStartRefusesMalformedImageReferences(t *testing.T) {
	for _, image := range []string{"App:1", "app:1/../../containers/x", "app?x=1", "-app", "app:"} {
		engine := newFakeEngine(t)
		_, err := networkRuntime(t, engine).Start(context.Background(), StartSpec{Image: image, Port: 8080})
		if status.Code(err) != codes.InvalidArgument || len(engine.requests) != 0 {
			t.Errorf("%s: %v %v", image, err, engine.requests)
		}
	}
}

func TestSweepRemovesOnlyThisEnvironmentsContainers(t *testing.T) {
	engine := newFakeEngine(t)
	if err := networkRuntime(t, engine).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	var filter map[string][]string
	if len(engine.listed) != 1 || json.Unmarshal([]byte(engine.listed[0]), &filter) != nil {
		t.Fatalf("listings = %v", engine.listed)
	}
	if labels := filter["label"]; len(labels) != 1 || labels[0] != labelEnvironment+"="+testEnvironment {
		t.Fatalf("filter = %v", filter)
	}
	if strings.Join(engine.removed, ",") != "mine-1,mine-2" {
		t.Fatalf("removed = %v", engine.removed)
	}
	unlabeled, err := NewDockerRuntime(DockerConfig{Socket: engine.socket})
	if err != nil {
		t.Fatal(err)
	}
	if err := unlabeled.Sweep(context.Background()); err == nil {
		t.Fatal("a sweep without an environment label was allowed")
	}
}

func TestAddressInPicksTheInterfaceOnTheNetwork(t *testing.T) {
	addresses := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		&net.IPNet{IP: net.ParseIP("172.30.0.2"), Mask: net.CIDRMask(16, 32)},
	}
	if host, err := addressIn([]string{"bogus", "172.30.0.0/16"}, addresses); err != nil || host != "172.30.0.2" {
		t.Fatalf("host = %q %v", host, err)
	}
	if _, err := addressIn([]string{"10.9.0.0/16"}, addresses); err == nil {
		t.Fatal("an address outside the network was accepted")
	}
}

func TestInvocationDialsOnlyLocalRevisionAddresses(t *testing.T) {
	for address, want := range map[string]bool{"127.0.0.1:8080": true, testContainerIP + ":8080": true, "8.8.8.8:80": false, "metadata.google.internal:80": false} {
		if revisionAddress(address) != want {
			t.Errorf("%s: want %v", address, want)
		}
	}
}

func TestMetadataGateServesOnlyTheRevisionPeer(t *testing.T) {
	served := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("token")) })
	for peer, want := range map[string]int{"127.0.0.1": http.StatusOK, testContainerIP: http.StatusForbidden, "": http.StatusForbidden} {
		gate := newPeerGate(served)
		gate.allow(peer)
		request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/computeMetadata/v1/", nil)
		request.RemoteAddr = "127.0.0.1:40000"
		recorder := httptest.NewRecorder()
		gate.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Errorf("peer %q: status %d, want %d", peer, recorder.Code, want)
		}
	}
}
