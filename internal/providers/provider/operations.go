package provider

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxOperations bounds the operations one scope keeps; the oldest finished
// ones are dropped first.
const maxOperations = 1000

// Operation is one long-running operation, as the metadata renderer sees it.
type Operation struct {
	Name            string    // projects/P/locations/L/operations/ID
	Target, Verb    string    // the resource and action the metadata reports
	Created, Ended  time.Time // Ended is zero until Done
	Done            bool
	CancelRequested bool
	// Metadata is what Start recorded in Launch.Metadata; Response is the
	// finished work's response once Done. A service whose metadata is its
	// resource (Cloud Run) renders them.
	Metadata, Response map[string]any
}

// Operations stores one service's google.longrunning operations, scoped by
// project and location. An operation is done once its work finished and at
// least the store's minimum latency has passed since it started, so clients
// see done=false and poll, as they do against GCP.
type Operations struct {
	store    *state.Store[operationLog]
	latency  time.Duration
	metadata func(Operation) map[string]any
	name     func(scope state.Scope, target, id string) string
	newest   bool          // List renders the newest operation first
	changed  chan struct{} // closed and replaced on every Finish or Cancel
	mu       sync.Mutex    // guards changed
	prefix   string        // starts every ID; "operation-" unless WithPrefix set it
}

type operationLog struct {
	records map[string]*operationRecord
	order   []string // IDs in creation order
}

type operationRecord struct {
	Operation
	finished bool
	latency  time.Duration // the minimum time from Start to done
	doneAt   time.Time     // when a finished operation reports done
	response map[string]any
	failure  *status.Status
}

// NewOperations returns a store for scopes validate accepts. latency is the
// minimum time from Start to done; metadata renders an operation's
// "metadata" field, "@type" included.
func NewOperations(validate func(state.Scope) error, latency time.Duration, metadata func(Operation) map[string]any) *Operations {
	return &Operations{
		store: state.NewStore(func() operationLog {
			return operationLog{records: map[string]*operationRecord{}}
		}, validate),
		latency: latency, metadata: metadata, changed: make(chan struct{}),
		name: func(scope state.Scope, _, id string) string { return OperationName(scope, id) },
	}
}

// Named replaces OperationName as the operation name, for an API that names
// operations after their resource: Bigtable's are
// "operations/projects/P/instances/I/locations/Z/operations/ID".
func (o *Operations) Named(name func(scope state.Scope, target, id string) string) *Operations {
	o.name = name
	return o
}

// NewestFirst makes List render the newest operation first, as Cloud Run
// lists them (recording run-service-lifecycle).
func (o *Operations) NewestFirst() *Operations {
	o.newest = true
	return o
}

// WithPrefix makes Start issue IDs that begin with prefix rather than
// "operation-". Two services whose operations share a REST path (Artifact
// Registry and Memorystore both serve /v1/projects/*/locations/*/operations/*
// on the path-only gateway) route each ID by its prefix. Call it before the
// first Start.
func (o *Operations) WithPrefix(prefix string) *Operations {
	o.prefix = prefix
	return o
}

// Launch describes one operation Start records: the resource and action its
// metadata reports, an optional metadata payload the renderer sees as
// Operation.Metadata (the store keeps the map, so the caller must not change
// it afterwards), and Immediate for a method GCP answers with an operation
// that is done as soon as its work finished, with no minimum latency
// (Bigtable's partial instance and table updates, recording
// bigtable-instance-table-admin).
type Launch struct {
	Target, Verb string
	Metadata     map[string]any
	Immediate    bool
}

// Start records a running operation and returns its ID; its name is
// OperationName(scope, id) unless Named replaced it.
func (o *Operations) Start(scope state.Scope, launch Launch) (string, error) {
	latency := o.latency
	if launch.Immediate {
		latency = 0
	}
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	now := time.Now().UTC()
	id := cmp.Or(o.prefix, "operation-") + strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(raw)
	err := o.store.With(scope, func(log *operationLog) error {
		log.records[id] = &operationRecord{latency: latency, Operation: Operation{Name: o.name(scope, launch.Target, id),
			Target: launch.Target, Verb: launch.Verb, Created: now, Metadata: launch.Metadata}}
		log.order = append(log.order, id)
		log.trim()
		return nil
	})
	return id, err //nolint:wrapcheck // the store's scope refusal is a wire status
}

// Run runs work as one operation on target: Start before it, Finish with
// its response ("@type" included) after it. When work fails the operation is
// deleted and the error returned, so a refused mutation leaves none. It
// returns the operation's ID.
func (o *Operations) Run(scope state.Scope, target, verb string, work func() (map[string]any, error)) (string, error) {
	id, err := o.Start(scope, Launch{Target: target, Verb: verb})
	if err != nil {
		return "", err
	}
	response, err := work()
	if err != nil {
		_ = o.Delete(scope, id)
		return "", err
	}
	o.Finish(scope, id, response, nil)
	return id, nil
}

// Finish completes an operation's work with its response ("@type"
// included), or with err, which the operation reports as its error. Finishing
// a cancelled or deleted operation does nothing.
func (o *Operations) Finish(scope state.Scope, id string, response map[string]any, err error) {
	_ = o.store.With(scope, func(log *operationLog) error {
		if record := log.records[id]; record != nil && !record.finished {
			record.finished, record.response = true, response
			record.doneAt = record.Created.Add(record.latency)
			if now := time.Now(); now.After(record.doneAt) {
				record.doneAt = now
			}
			if err != nil {
				record.failure = status.Convert(err)
			}
		}
		return nil
	})
	o.notify()
}

// Get renders one operation, or NOT_FOUND.
func (o *Operations) Get(scope state.Scope, id string) (map[string]any, error) {
	var body map[string]any
	err := o.store.With(scope, func(log *operationLog) error {
		record := log.records[id]
		if record == nil {
			return NotFound("operations", id)
		}
		body, _ = wire(record, o.metadata, time.Now())
		return nil
	})
	return body, err //nolint:wrapcheck // wire statuses
}

// List renders one page of the scope's operations in creation order, or
// newest first after NewestFirst, as a ListOperationsResponse. A filter is
// refused with gap.
func (o *Operations) List(scope state.Scope, query url.Values, size Paging, gap func(string) error) (map[string]any, error) {
	var body map[string]any
	err := o.store.With(scope, func(log *operationLog) error {
		order := log.order
		if o.newest {
			order = slices.Clone(order)
			slices.Reverse(order)
		}
		page, next, err := Page(order, query, size, gap)
		if err != nil {
			return err
		}
		now := time.Now()
		items := make([]map[string]any, 0, len(page))
		for _, id := range page {
			item, _ := wire(log.records[id], o.metadata, now)
			items = append(items, item)
		}
		body = ListBody("operations", items, next)
		return nil
	})
	return body, err //nolint:wrapcheck // wire statuses
}

// Cancel requests cancellation. An operation whose work hasn't finished ends
// at once with a CANCELLED error; finished work completes as it would have,
// since GCP's cancellation is best effort.
func (o *Operations) Cancel(scope state.Scope, id string) error {
	err := o.store.With(scope, func(log *operationLog) error {
		record := log.records[id]
		if record == nil {
			return NotFound("operations", id)
		}
		record.CancelRequested = true
		if !record.finished {
			record.finished, record.doneAt = true, time.Now()
			record.failure = status.New(codes.Canceled, "Operation was cancelled.")
		}
		return nil
	})
	o.notify()
	return err //nolint:wrapcheck // wire statuses
}

// Delete forgets an operation; it no longer affects the work.
func (o *Operations) Delete(scope state.Scope, id string) error {
	return o.store.With(scope, func(log *operationLog) error { //nolint:wrapcheck // wire statuses
		if log.records[id] == nil {
			return NotFound("operations", id)
		}
		delete(log.records, id)
		for i, current := range log.order {
			if current == id {
				log.order = append(log.order[:i:i], log.order[i+1:]...)
				break
			}
		}
		return nil
	})
}

// Wait blocks until the operation is done or timeout passes, then renders
// it, as google.longrunning.Operations.WaitOperation does.
func (o *Operations) Wait(ctx context.Context, scope state.Scope, id string, timeout time.Duration) (map[string]any, error) {
	deadline := time.Now().Add(timeout)
	for {
		o.mu.Lock()
		changed := o.changed
		o.mu.Unlock()
		var body map[string]any
		var doneAt time.Time
		err := o.store.With(scope, func(log *operationLog) error {
			record := log.records[id]
			if record == nil {
				return NotFound("operations", id)
			}
			body, doneAt = wire(record, o.metadata, time.Now())
			return nil
		})
		if err != nil || body["done"] == true || !time.Now().Before(deadline) {
			return body, err //nolint:wrapcheck // wire statuses
		}
		wake := time.Until(deadline)
		if !doneAt.IsZero() {
			wake = min(wake, time.Until(doneAt))
		}
		timer := time.NewTimer(wake)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// Reset clears a scope's operations.
func (o *Operations) Reset(scope state.Scope) error {
	return o.store.Reset(scope) //nolint:wrapcheck // the store's scope refusal
}

// notify wakes every Wait.
func (o *Operations) notify() {
	o.mu.Lock()
	close(o.changed)
	o.changed = make(chan struct{})
	o.mu.Unlock()
}

// wire renders an operation as GCP's REST API does: done is always present;
// response or error only once done. It also returns when a finished but not
// yet done operation becomes done (zero otherwise).
func wire(record *operationRecord, metadata func(Operation) map[string]any, now time.Time) (map[string]any, time.Time) {
	view := record.Operation
	view.Done = record.finished && !now.Before(record.doneAt)
	if view.Done {
		view.Ended, view.Response = record.doneAt, record.response
	}
	body := map[string]any{"name": view.Name, "metadata": metadata(view), "done": view.Done}
	switch {
	case !view.Done && record.finished:
		return body, record.doneAt
	case !view.Done:
	case record.failure != nil:
		body["error"] = map[string]any{"code": int(record.failure.Code()), "message": record.failure.Message()}
	case record.response != nil:
		body["response"] = record.response
	}
	return body, time.Time{}
}

// trim drops the oldest finished operations beyond maxOperations.
func (l *operationLog) trim() {
	for i := 0; len(l.order) > maxOperations && i < len(l.order); {
		if id := l.order[i]; l.records[id].finished {
			delete(l.records, id)
			l.order = append(l.order[:i:i], l.order[i+1:]...)
			continue
		}
		i++
	}
}
