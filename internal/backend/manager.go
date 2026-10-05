// Package backend manages lazily started, explicitly scoped emulator instances.
package backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

var ErrFailed = errors.New("backend failed; explicit reset is required")
var ErrClosed = errors.New("backend manager is shut down")
var ErrPersistenceUnsupported = errors.New("backend snapshots and restore are unsupported")

type Key struct {
	Service string      `json:"service"`
	Scope   state.Scope `json:"scope"`
}
type Backend interface {
	Start(context.Context) error
	Ready(context.Context) error
	Stop(context.Context) error
	Reset(context.Context) error
}
type Definition struct {
	Validate func(state.Scope) error
	// New constructs an inert driver; process/filesystem side effects start in Start.
	New        func(state.Scope) (Backend, error)
	AfterReset func(state.Scope) error
}
type Phase string

const (
	Stopped   Phase = "stopped"
	Starting  Phase = "starting"
	Ready     Phase = "ready"
	Failed    Phase = "failed"
	Stopping  Phase = "stopping"
	Resetting Phase = "resetting"
)

type Status struct {
	Key      Key    `json:"key"`
	Phase    Phase  `json:"phase"`
	Failure  string `json:"failure,omitempty"`
	Snapshot bool   `json:"snapshot"`
	Restore  bool   `json:"restore"`
}
type instance struct {
	gate       chan struct{}
	mu         sync.RWMutex
	status     Status
	backend    Backend
	definition Definition
}

func (i *instance) phase(phase Phase, failure string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.status.Phase = phase
	i.status.Failure = failure
}
func (i *instance) snapshot() Status { i.mu.RLock(); defer i.mu.RUnlock(); return i.status }
func (i *instance) acquire(ctx context.Context) error {
	select {
	case i.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (i *instance) release() { <-i.gate }

type Manager struct {
	mu                        sync.Mutex
	definitions               map[string]Definition
	instances                 map[Key]*instance
	closed                    bool
	lifetime                  context.Context
	cancel                    context.CancelFunc
	startTimeout, stopTimeout time.Duration
}

func NewManager(startTimeout, stopTimeout time.Duration) *Manager {
	lifetime, cancel := context.WithCancel(context.Background())
	return &Manager{definitions: map[string]Definition{}, instances: map[Key]*instance{}, startTimeout: startTimeout, stopTimeout: stopTimeout, lifetime: lifetime, cancel: cancel}
}
func (m *Manager) Register(service string, definition Definition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if m.startTimeout <= 0 || m.stopTimeout <= 0 || service == "" || definition.New == nil || definition.AfterReset == nil {
		return errors.New("backend definition and positive lifecycle timeouts are required")
	}
	if _, exists := m.definitions[service]; exists {
		return errors.New("backend service already registered")
	}
	m.definitions[service] = definition
	return nil
}
func (m *Manager) get(key Key) (*instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	definition, ok := m.definitions[key.Service]
	if !ok {
		return nil, errors.New("backend service is not registered")
	}
	if err := key.Scope.Validate(); err != nil {
		return nil, err
	}
	if definition.Validate != nil {
		if err := definition.Validate(key.Scope); err != nil {
			return nil, err
		}
	}
	if m.instances[key] == nil {
		m.instances[key] = &instance{gate: make(chan struct{}, 1), status: Status{Key: key, Phase: Stopped}, definition: definition}
	}
	return m.instances[key], nil
}
func (m *Manager) Require(ctx context.Context, key Key) (Backend, error) {
	instance, err := m.get(key)
	if err != nil {
		return nil, err
	}
	if err := instance.acquire(ctx); err != nil {
		return nil, err
	}
	defer instance.release()
	return m.require(ctx, key, instance)
}

// Use serializes one provider operation with reset and shutdown. Providers call
// it only after authorization and must use its context for backend work. The
// callback must not recursively enter the same backend instance.
func (m *Manager) Use(ctx context.Context, key Key, operation func(context.Context, Backend) error) error {
	instance, err := m.get(key)
	if err != nil {
		return err
	}
	if err := instance.acquire(ctx); err != nil {
		return err
	}
	defer instance.release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopCancel := context.AfterFunc(m.lifetime, cancel)
	defer stopCancel()
	active, err := m.require(ctx, key, instance)
	if err != nil {
		return err
	}
	return operation(ctx, active)
}

func (m *Manager) require(ctx context.Context, key Key, instance *instance) (Backend, error) {
	var err error
	// Shutdown may have started while this caller waited for the instance gate.
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	phase := instance.snapshot().Phase
	if phase == Failed {
		return nil, ErrFailed
	}
	if phase == Ready {
		probe, cancel := context.WithTimeout(ctx, m.startTimeout)
		defer cancel()
		stopCancel := context.AfterFunc(m.lifetime, cancel)
		defer stopCancel()
		if err := instance.backend.Ready(probe); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			m.fail(instance, "readiness")
			return nil, fmt.Errorf("backend readiness: %w", err)
		}
		return instance.backend, nil
	}
	instance.phase(Starting, "")
	deadline, cancel := context.WithTimeout(ctx, m.startTimeout)
	defer cancel()
	stopCancel := context.AfterFunc(m.lifetime, cancel)
	defer stopCancel()
	if instance.backend == nil {
		instance.backend, err = instance.definition.New(key.Scope)
		if err != nil {
			instance.phase(Failed, "construct")
			return nil, err
		}
		if instance.backend == nil {
			instance.phase(Failed, "construct")
			return nil, errors.New("backend factory returned nil")
		}
	}
	if err := instance.backend.Start(deadline); err != nil {
		m.fail(instance, "start")
		return nil, fmt.Errorf("backend start: %w", err)
	}
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	for {
		err := instance.backend.Ready(deadline)
		if err == nil {
			instance.phase(Ready, "")
			return instance.backend, nil
		}
		if errors.Is(err, ErrProcessExited) {
			m.fail(instance, "readiness")
			return nil, err
		}
		select {
		case <-deadline.Done():
			m.fail(instance, "readiness timeout")
			return nil, deadline.Err()
		case <-timer.C:
		}
	}
}
func (m *Manager) fail(instance *instance, operation string) {
	// A cancelled request must not cancel cleanup of its partially started child.
	ctx, cancel := context.WithTimeout(context.Background(), m.stopTimeout)
	defer cancel()
	if instance.backend != nil {
		if err := instance.backend.Stop(ctx); err != nil {
			operation += "; cleanup incomplete"
		}
	}
	instance.phase(Failed, operation)
}

// Preload uses the same lazy path. On startup failure the caller must Shutdown
// the manager to stop earlier successful instances; no provider state is reset.
func (m *Manager) Preload(ctx context.Context, keys []Key) error {
	for _, key := range keys {
		if _, err := m.Require(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
func (m *Manager) Reset(ctx context.Context, key Key) error {
	ctx, cancel := context.WithTimeout(ctx, m.stopTimeout)
	defer cancel()
	stopCancel := context.AfterFunc(m.lifetime, cancel)
	defer stopCancel()
	instance, err := m.get(key)
	if err != nil {
		return err
	}
	if err := instance.acquire(ctx); err != nil {
		return err
	}
	defer instance.release()
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return ErrClosed
	}
	instance.phase(Resetting, "")
	if instance.backend != nil {
		if err := instance.backend.Stop(ctx); err != nil {
			instance.phase(Failed, "reset stop")
			return err
		}
		if err := instance.backend.Reset(ctx); err != nil {
			instance.phase(Failed, "reset assets")
			return err
		}
	}
	if err := instance.definition.AfterReset(key.Scope); err != nil {
		instance.phase(Failed, "reset provider state")
		return err
	}
	instance.phase(Stopped, "")
	return nil
}
func (m *Manager) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.stopTimeout)
	defer cancel()
	m.mu.Lock()
	m.closed = true
	m.cancel()
	instances := make([]*instance, 0, len(m.instances))
	for _, instance := range m.instances {
		instances = append(instances, instance)
	}
	m.mu.Unlock()
	// Stop independent backends concurrently, sharing the caller's total deadline.
	result := make(chan error, len(instances))
	for _, instance := range instances {
		go func() {
			if err := instance.acquire(ctx); err != nil {
				result <- err
				return
			}
			defer instance.release()
			instance.phase(Stopping, "")
			if instance.backend != nil {
				if err := instance.backend.Stop(ctx); err != nil {
					instance.phase(Failed, "shutdown")
					result <- err
					return
				}
			}
			instance.phase(Stopped, "")
			result <- nil
		}()
	}
	var failures []error
	for range instances {
		if err := <-result; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
func (m *Manager) Status() []Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]Status, 0, len(m.instances))
	for _, instance := range m.instances {
		result = append(result, instance.snapshot())
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i].Key, result[j].Key
		return a.Service+"/"+a.Scope.Project+"/"+a.Scope.Location < b.Service+"/"+b.Scope.Project+"/"+b.Scope.Location
	})
	return result
}

// This first manager has no complete provider+backend snapshot format. Do not
// restore only the index while leaving backend objects missing or inconsistent.
func (m *Manager) Snapshot(context.Context) error { return ErrPersistenceUnsupported }
func (m *Manager) Restore(context.Context) error  { return ErrPersistenceUnsupported }
