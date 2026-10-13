package cloudtasks

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exhaustedRetention keeps a task that ran out of attempts readable briefly
// before removal, so a poller can observe its final attempt. GCP removes such
// tasks asynchronously; this delay is a local choice, not a recorded value.
const exhaustedRetention = 3 * time.Second

// pausedPoll is how often a worker rechecks a paused queue.
const pausedPoll = 250 * time.Millisecond

// worker is one task's dispatch goroutine handle.
type worker struct {
	cancel context.CancelFunc
	own    *taskRecord // the task the worker was started for
}

// outcome is the task state after one completed attempt.
type outcome int

const (
	outcomeRetry outcome = iota
	outcomeRemoved
	outcomeExhausted
	outcomeThrottled
)

// launchDispatch starts one background worker for the task. The worker acts
// as the Cloud Tasks service agent, never as the creating caller.
func (p *Provider) launchDispatch(scope state.Scope, key string, own *taskRecord) {
	ctx, cancel := context.WithCancel(context.Background())
	active := &worker{cancel: cancel, own: own}
	p.mu.Lock()
	p.workers[provider.WorkerKey(scope, key)] = active
	p.mu.Unlock()
	go func() {
		defer p.finishWorker(scope, key, active)
		p.dispatchLoop(ctx, scope, key, own)
	}()
}

// finishWorker releases a worker that ended on its own.
func (p *Provider) finishWorker(scope state.Scope, key string, active *worker) {
	active.cancel()
	p.mu.Lock()
	if p.workers[provider.WorkerKey(scope, key)] == active {
		delete(p.workers, provider.WorkerKey(scope, key))
	}
	p.mu.Unlock()
}

// dispatchLoop delivers the task at its schedule time, within the queue's
// rate limits, and retries on the queue's retryConfig until it succeeds,
// exhausts its retries, or is removed.
func (p *Provider) dispatchLoop(ctx context.Context, scope state.Scope, key string, own *taskRecord) {
	for {
		wait, wake, ok := p.nextDispatch(scope, key, own)
		if !ok {
			return
		}
		if wait > 0 {
			if !sleep(ctx, wait, wake) {
				return
			}
			continue
		}
		if !p.afterAttempt(ctx, scope, key, own, p.attempt(ctx, scope, key, own), wake) {
			return
		}
	}
}

// afterAttempt acts on one attempt's outcome and reports whether the worker
// keeps going: an exhausted task is removed after its retention, and a
// throttled dispatch asks again shortly.
func (p *Provider) afterAttempt(ctx context.Context, scope state.Scope, key string, own *taskRecord, result outcome, wake <-chan struct{}) bool {
	switch result {
	case outcomeRemoved:
		return false
	case outcomeExhausted:
		if sleep(ctx, p.retention, nil) {
			p.removeTask(scope, key, own)
		}
		return false
	case outcomeThrottled:
		return sleep(ctx, limiterPoll, wake)
	default:
		return true
	}
}

// sleep waits for the delay or a wake signal; it reports false once the
// worker is cancelled.
func sleep(ctx context.Context, delay time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
	case <-timer.C:
	}
	return true
}

// nextDispatch reports how long the worker waits before its next attempt; a
// paused queue holds its tasks.
func (p *Provider) nextDispatch(scope state.Scope, key string, own *taskRecord) (time.Duration, <-chan struct{}, bool) {
	var (
		wait  time.Duration
		wake  <-chan struct{}
		found bool
	)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := owned(s, key, own)
		if record == nil || s.queues[record.Queue] == nil {
			return nil
		}
		found, wake = true, record.wake
		wait = record.ScheduleAt.Sub(p.clock())
		if s.queues[record.Queue].queue.GetState() == taskspb.Queue_PAUSED {
			wait = pausedPoll
		}
		return nil
	})
	return wait, wake, found
}

// attempt performs one dispatch within the task's dispatchDeadline and
// records it on the task.
func (p *Provider) attempt(ctx context.Context, scope state.Scope, key string, own *taskRecord) outcome {
	snapshot, queue, dispatched, result := p.beginAttempt(scope, key, own)
	if result != outcomeRetry {
		return result
	}
	attemptCtx, cancel := context.WithTimeout(ctx, snapshot.Deadline)
	code, message, responded := p.send(attemptCtx, scope, snapshot)
	cancel()
	if ctx.Err() != nil {
		p.release(scope, queue)
		return outcomeRemoved
	}
	return p.finishAttempt(scope, key, own, queue, &attemptRecord{
		Schedule: snapshot.ScheduleAt, Dispatch: dispatched, Response: p.clock(), Code: code, Message: message,
	}, responded)
}

// beginAttempt takes a dispatch from the queue's rate limits, counts it, and
// snapshots the request inputs. It returns outcomeThrottled when the limits
// hold the dispatch and outcomeRemoved when the task or its queue is gone.
func (p *Provider) beginAttempt(scope state.Scope, key string, own *taskRecord) (taskRecord, *queueRecord, time.Time, outcome) {
	var snapshot taskRecord
	var queue *queueRecord
	dispatched, result := p.clock(), outcomeRemoved
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := owned(s, key, own)
		if record == nil || s.queues[record.Queue] == nil {
			return nil
		}
		queue = s.queues[record.Queue]
		if _, admitted := queue.admit(dispatched); !admitted {
			result = outcomeThrottled
			return nil
		}
		record.Dispatches++
		if record.First == nil {
			record.First = &attemptRecord{Schedule: record.ScheduleAt, Dispatch: dispatched}
		}
		snapshot, result = requestSnapshot(record), outcomeRetry
		return nil
	})
	return snapshot, queue, dispatched, result
}

// release returns an attempt's place under maxConcurrentDispatches; a
// deleted queue's record is simply dropped.
func (p *Provider) release(scope state.Scope, queue *queueRecord) {
	_ = p.scopes.With(scope, func(*scopeState) error {
		queue.inflight = max(queue.inflight-1, 0)
		return nil
	})
}

// finishAttempt records the completed attempt, then removes a succeeded
// task, marks an exhausted one, or schedules the retry from the queue's
// policy. Removal tombstones the task's name.
func (p *Provider) finishAttempt(scope state.Scope, key string, own *taskRecord, owner *queueRecord, last *attemptRecord, responded bool) outcome {
	result := outcomeRemoved
	_ = p.scopes.With(scope, func(s *scopeState) error {
		owner.inflight = max(owner.inflight-1, 0)
		record := owned(s, key, own)
		if record == nil {
			return nil
		}
		queue := s.queues[record.Queue]
		record.Last = last
		if responded {
			record.Responses++
		}
		switch {
		case last.Code == codes.OK || queue == nil:
			s.bury(key, last.Response)
		case retryOf(queue.queue).exhausted(record.Dispatches, last.Response.Sub(record.First.Dispatch)):
			result = outcomeExhausted
		default:
			record.ScheduleAt = last.Response.Add(retryOf(queue.queue).backoff(record.Dispatches))
			result = outcomeRetry
		}
		return nil
	})
	return result
}

// removeTask deletes one task after its retention, tombstoning its name.
func (p *Provider) removeTask(scope state.Scope, key string, own *taskRecord) {
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if owned(s, key, own) != nil {
			s.bury(key, p.clock())
		}
		return nil
	})
}

// send performs one HTTP dispatch. A transport failure has no response and
// reports UNKNOWN, as GCP does for an unreachable target. A token that cannot
// be minted fails the attempt with the mint's status before anything is sent;
// the queue's retryConfig then schedules the next attempt.
func (p *Provider) send(ctx context.Context, scope state.Scope, record taskRecord) (codes.Code, string, bool) {
	request, err := http.NewRequestWithContext(ctx, record.Method, record.URL, bytes.NewReader(record.Body))
	if err != nil {
		return codes.Unknown, "The task request could not be built", false
	}
	headers, err := p.dispatchHeaders(ctx, scope, record)
	if err != nil {
		return status.Code(err), status.Convert(err).Message(), false
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	// Cloud Run counts the queue's own project's deliveries as internal.
	provider.MarkInternal(request, scope.Project)
	response, err := p.client.Do(request)
	if errors.Is(err, context.DeadlineExceeded) {
		// The target did not answer within the task's dispatchDeadline.
		return codes.DeadlineExceeded, "", false
	}
	if err != nil {
		return codes.Unknown, "", false
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return codes.OK, "", true
	}
	return provider.HTTPCode(response.StatusCode), http.StatusText(response.StatusCode), true
}

// dispatchHeaders builds the delivered headers. An OIDC task's token is
// minted for every attempt by the project's Cloud Tasks service agent,
// service-NUMBER@gcp-sa-cloudtasks.iam.gserviceaccount.com, never by the
// task's creator.
func (p *Provider) dispatchHeaders(ctx context.Context, scope state.Scope, record taskRecord) (map[string]string, error) {
	headers := map[string]string{"Content-Type": "application/octet-stream"}
	maps.Copy(headers, record.Headers)
	headers["User-Agent"] = userAgent
	headers["X-Cloudtasks-Queuename"] = provider.LastSegment(record.Queue)
	headers["X-Cloudtasks-Taskname"] = record.ID
	headers["X-Cloudtasks-Taskretrycount"] = strconv.Itoa(record.Dispatches - 1)
	headers["X-Cloudtasks-Taskexecutioncount"] = strconv.Itoa(record.Responses)
	if record.ServiceAccount == "" {
		return headers, nil
	}
	project, ok := p.index.Get(provider.ProjectResource(scope.Project))
	if !ok || project.Number == "" {
		return nil, gap("OIDC tokens need the numeric project of the Cloud Tasks service agent")
	}
	call := gateway.Call{Context: ctx, Project: scope.Project, Principal: gateway.Principal{
		State: gateway.Authenticated, Subject: serviceAgent(project.Number), Project: scope.Project,
		Scopes: []string{identity.CloudPlatform},
	}}
	minted, err := p.tokens.Mint(call, iam.AccountResource(scope.Project, record.ServiceAccount), record.Audience)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "The Cloud Tasks service agent could not mint an OIDC token for %s: %s",
			record.ServiceAccount, status.Convert(err).Message())
	}
	headers["Authorization"] = "Bearer " + minted
	return headers, nil
}

// serviceAgent is the member of a project's Cloud Tasks service agent.
func serviceAgent(number string) string {
	return "serviceAccount:service-" + number + "@gcp-sa-cloudtasks.iam.gserviceaccount.com"
}

// owned returns the task a worker was started for, or nil once it is gone:
// a task deleted and created again under the same name has its own worker,
// and an older worker must never touch it.
func owned(s *scopeState, key string, own *taskRecord) *taskRecord {
	if record := s.tasks[key]; record == own {
		return record
	}
	return nil
}
