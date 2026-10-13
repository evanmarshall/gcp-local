package memorystore

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"sync"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// errInjected is a test launch failure.
var errInjected = errors.New("injected launch failure")

// newEngine returns the IAM engine with the pinned roles/redis.* snapshots:
// admin holds roles/redis.admin and viewer roles/redis.viewer on the test
// project, and peer holds roles/redis.admin on the peer project only.
func newEngine(t *testing.T, index *state.Index) *iam.Engine {
	t.Helper()
	engine, err := iam.New(index)
	must(t, err)
	grants := map[string][]iam.Binding{
		testProject: {{Role: "roles/redis.admin", Members: []string{principal(adminToken)}},
			{Role: "roles/redis.viewer", Members: []string{principal(viewerToken)}}},
		peerProject: {{Role: "roles/redis.admin", Members: []string{principal(peerToken)}}},
	}
	for number, project := range []string{testProject, peerProject} {
		must(t, index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager",
			Kind: "project", Number: strconv.Itoa(number + 1), Scope: provider.ProjectScope(project)}))
		_, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Version: 3, Bindings: grants[project]})
		must(t, err)
	}
	return engine
}

// fakeDriver records lifecycle and per-instance calls without processes.
type fakeDriver struct {
	mu                      sync.Mutex
	starts, stops, resets   int
	launched                map[string]launchSpec
	resized                 map[string]int64
	removed                 []string
	nextPort                int
	launchFault, removeFail error
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{launched: map[string]launchSpec{}, resized: map[string]int64{}, nextPort: 16379}
}

func (f *fakeDriver) Start(context.Context) error { f.mu.Lock(); f.starts++; f.mu.Unlock(); return nil }
func (*fakeDriver) Ready(context.Context) error   { return nil }
func (f *fakeDriver) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	f.launched = map[string]launchSpec{}
	return nil
}
func (f *fakeDriver) Reset(context.Context) error { f.mu.Lock(); f.resets++; f.mu.Unlock(); return nil }

func (f *fakeDriver) launch(_ context.Context, id string, spec launchSpec) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.launchFault != nil {
		return 0, f.launchFault
	}
	if _, exists := f.launched[id]; exists {
		return 0, errServerRunning
	}
	f.launched[id] = spec
	f.nextPort++
	return f.nextPort, nil
}

func (f *fakeDriver) resize(_ context.Context, id string, memoryGB int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.launched[id]; !exists {
		return errUnknownServer
	}
	f.resized[id] = memoryGB
	return nil
}

func (f *fakeDriver) remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeFail != nil {
		return f.removeFail
	}
	if _, exists := f.launched[id]; !exists {
		return errUnknownServer
	}
	delete(f.launched, id)
	f.removed = append(f.removed, id)
	return nil
}

// running reports the launched instance IDs.
func (f *fakeDriver) running() map[string]launchSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.launched)
}
