package memorystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"maps"
	"sync"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Role permission sets from the documented Memorystore predefined roles. They
// stand in for the roles/redis.* snapshots until those are recorded from GCP;
// the production provider registers its plans only with the real IAM engine.
var (
	adminPermissions  = []string{"redis.instances.create", "redis.instances.get", "redis.instances.list", "redis.instances.update", "redis.instances.delete", "redis.instances.getAuthString", "redis.operations.get", "redis.operations.list"}
	viewerPermissions = []string{"redis.instances.get", "redis.instances.list", "redis.operations.get", "redis.operations.list"}
)

// Test sentinels.
var (
	errDuplicatePlan = errors.New("duplicate plan")
	errInjected      = errors.New("injected launch failure")
)

// fakeAuthorizer grants permissions per project and subject and records every
// call, so tests can assert authorization happens before backend effects.
type fakeAuthorizer struct {
	mu     sync.Mutex
	rules  map[string]iam.Rule
	grants map[string]map[string]map[string]bool // target -> subject -> permission
	calls  int
}

func newFakeAuthorizer() *fakeAuthorizer {
	return &fakeAuthorizer{rules: map[string]iam.Rule{}, grants: map[string]map[string]map[string]bool{}}
}

func (f *fakeAuthorizer) grant(project, subject string, permissions []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	target := provider.ProjectResource(project)
	if f.grants[target] == nil {
		f.grants[target] = map[string]map[string]bool{}
	}
	if f.grants[target][subject] == nil {
		f.grants[target][subject] = map[string]bool{}
	}
	for _, permission := range permissions {
		f.grants[target][subject][permission] = true
	}
}

func (f *fakeAuthorizer) Register(method string, plan iam.Rule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.rules[method]; exists {
		return errDuplicatePlan
	}
	f.rules[method] = plan
	return nil
}

func (f *fakeAuthorizer) Authorize(call gateway.Call, target iam.Target, _ map[string]bool) (iam.Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	decision := iam.Decision{ID: hex.EncodeToString(raw), Method: call.Method}
	rule, ok := f.rules[call.Method]
	if !ok {
		return decision, status.Error(codes.Unimplemented, "no plan")
	}
	if call.Principal.State != gateway.Authenticated {
		return decision, status.Error(codes.Unauthenticated, "anonymous")
	}
	for _, permission := range rule.Baseline {
		allowed := f.grants[target.Name][call.Principal.Subject][permission]
		decision.Checks = append(decision.Checks, iam.Check{Permission: permission, Target: target.Name, Allowed: allowed})
		if !allowed {
			return decision, status.Error(codes.PermissionDenied, "Missing permission: "+permission)
		}
	}
	return decision, nil
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
