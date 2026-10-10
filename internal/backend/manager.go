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
var errDefinitionInvalid = errors.New("backend definition and positive lifecycle timeouts are required")
var errServiceRegistered = errors.New("backend service already registered")
var errServiceUnregistered = errors.New("backend service is not registered")
var errFactoryNil = errors.New("backend factory returned nil")

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
	sessionsMu sync.Mutex
	sessions   map[*session]struct{}
}

type session struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// The instance gate excludes new sessions while lifecycle work drains them.
// Completion never acquires that gate, so a reset cannot deadlock a reader.
func (i *instance) drainSessions(ctx context.Context) error {
	i.sessionsMu.Lock()
	active := make([]*session, 0, len(i.sessions))
	for s := range i.sessions {
		s.cancel()
		active = append(active, s)
	}
	i.sessionsMu.Unlock()
	for _, s := range active {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
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
	lifetime                  context.Context //nolint:containedctx // the manager owns the lifetime, not a request
	cancel                    context.CancelFunc
	sessionLifetime           context.Context //nolint:containedctx // the manager owns the lifetime, not a request
	cancelSessions            context.CancelFunc
	startTimeout, stopTimeout time.Duration
}

func NewManager(startTimeout, stopTimeout time.Duration) *Manager {
	lifetime, cancel := context.WithCancel(context.Background())
	sessions, cancelSessions := context.WithCancel(lifetime)
	return &Manager{definitions: map[string]Definition{}, instances: map[Key]*instance{}, startTimeout: startTimeout, stopTimeout: stopTimeout, lifetime: lifetime, cancel: cancel, sessionLifetime: sessions, cancelSessions: cancelSessions}
}

// Session permits blocking pulls/streams alongside Use operations on a backend
// whose client supports concurrency. It must not publish provider/index state
// or re-enter this instance. Reset cancels and drains sessions before stopping
// the backend. Providers still authorize before entering and before forwarding.
func (m *Manager) Session(ctx context.Context, key Key, operation func(context.Context, Backend) error) error {
	i, err := m.get(key)
	if err != nil {
		return err
	}
	if err := i.acquire(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(m.sessionLifetime, cancel) //nolint:contextcheck // the manager lifetime is rooted at process start by design
	defer func() { stopCancel(); cancel() }()
	if m.sessionLifetime.Err() != nil {
		i.release()
		return ErrClosed
	}
	active, err := m.require(ctx, key, i)
	if err != nil {
		i.release()
		return err
	}
	s := &session{cancel: cancel, done: make(chan struct{})}
	i.sessionsMu.Lock()
	if i.sessions == nil {
		i.sessions = map[*session]struct{}{}
	}
	i.sessions[s] = struct{}{}
	i.sessionsMu.Unlock()
	i.release()
	defer func() {
		i.sessionsMu.Lock()
		delete(i.sessions, s)
		close(s.done)
		i.sessionsMu.Unlock()
	}()
	return operation(ctx, active)
}

// DrainSessions cancels long-lived requests when listeners begin draining.
// It prevents new sessions, while ordinary Use calls retain their drain period.
func (m *Manager) DrainSessions() { m.cancelSessions() }
func (m *Manager) Register(service string, definition Definition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if m.startTimeout <= 0 || m.stopTimeout <= 0 || service == "" || definition.New == nil || definition.AfterReset == nil {
		return errDefinitionInvalid
	}
	if _, exists := m.definitions[service]; exists {
		return errServiceRegistered
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
		return nil, errServiceUnregistered
	}
	if err := key.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("scope validation: %w", err)
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
	stopCancel := context.AfterFunc(m.lifetime, cancel) //nolint:contextcheck // the manager lifetime is rooted at process start by design
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
		stopCancel := context.AfterFunc(m.lifetime, cancel) //nolint:contextcheck // the manager lifetime is rooted at process start by design
		defer stopCancel()
		if err := instance.backend.Ready(probe); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			m.fail(instance, "readiness") //nolint:contextcheck // cleanup must outlive the cancelled request
			return nil, fmt.Errorf("backend readiness: %w", err)
		}
		return instance.backend, nil
	}
	instance.phase(Starting, "")
	deadline, cancel := context.WithTimeout(ctx, m.startTimeout)
	defer cancel()
	stopCancel := context.AfterFunc(m.lifetime, cancel) //nolint:contextcheck // the manager lifetime is rooted at process start by design
	defer stopCancel()
	if instance.backend == nil {
		instance.backend, err = instance.definition.New(key.Scope)
		if err != nil {
			instance.phase(Failed, "construct")
			return nil, err
		}
		if instance.backend == nil {
			instance.phase(Failed, "construct")
			return nil, errFactoryNil
		}
	}
	if err := instance.backend.Start(deadline); err != nil {
		m.fail(instance, "start") //nolint:contextcheck // cleanup must outlive the cancelled request
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
			m.fail(instance, "readiness") //nolint:contextcheck // cleanup must outlive the cancelled request
			return nil, fmt.Errorf("backend readiness: %w", err)
		}
		select {
		case <-deadline.Done():
			m.fail(instance, "readiness timeout") //nolint:contextcheck // cleanup must outlive the cancelled request
			return nil, deadline.Err()
		case <-timer.C:
		}
	}
}
func (m *Manager) fail(instance *instance, operation string) {
	// A cancelled request must not cancel cleanup of its partially started child.
	ctx, cancel := context.WithTimeout(context.Background(), m.stopTimeout)
	defer cancel()
	if err := instance.drainSessions(ctx); err != nil {
		operation += "; session cleanup incomplete"
	}
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
	stopCancel := context.AfterFunc(m.lifetime, cancel) //nolint:contextcheck // the manager lifetime is rooted at process start by design
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
	if err := instance.drainSessions(ctx); err != nil {
		instance.phase(Failed, "reset sessions")
		return err
	}
	if instance.backend != nil {
		if err := instance.backend.Stop(ctx); err != nil {
			instance.phase(Failed, "reset stop")
			return fmt.Errorf("backend stop: %w", err)
		}
		if err := instance.backend.Reset(ctx); err != nil {
			instance.phase(Failed, "reset assets")
			return fmt.Errorf("backend reset: %w", err)
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
			if err := instance.drainSessions(ctx); err != nil {
				instance.phase(Failed, "shutdown sessions")
				result <- err
				return
			}
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

// Snapshot reports that this manager has no complete provider+backend snapshot
// format. Restoring only the index would leave backend objects missing or
// inconsistent, so persistence stays unsupported.
func (m *Manager) Snapshot(context.Context) error { return ErrPersistenceUnsupported }
func (m *Manager) Restore(context.Context) error  { return ErrPersistenceUnsupported }
