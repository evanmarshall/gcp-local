package cloudtasks

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net/http"
	"strconv"
	"time"

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
}

// outcome is the task state after one completed attempt.
type outcome int

const (
	outcomeRetry outcome = iota
	outcomeRemoved
	outcomeExhausted
)

// launchDispatch starts one background worker for the task. The worker acts
// as the Cloud Tasks service agent, never as the creating caller.
func (p *Provider) launchDispatch(scope state.Scope, key string) {
	ctx, cancel := context.WithCancel(context.Background())
	active := &worker{cancel: cancel}
	p.mu.Lock()
	p.workers[provider.WorkerKey(scope, key)] = active
	p.mu.Unlock()
	go func() {
		defer p.finishWorker(scope, key, active)
		p.dispatchLoop(ctx, scope, key)
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

// dispatchLoop delivers the task at its schedule time and retries on the
// queue's retryConfig until it succeeds, exhausts its attempts, or is removed.
func (p *Provider) dispatchLoop(ctx context.Context, scope state.Scope, key string) {
	for {
		wait, wake, ok := p.nextDispatch(scope, key)
		if !ok {
			return
		}
		if wait > 0 {
			if !sleep(ctx, wait, wake) {
				return
			}
			continue
		}
		switch p.attempt(ctx, scope, key) {
		case outcomeRemoved:
			return
		case outcomeExhausted:
			if sleep(ctx, p.retention, nil) {
				p.removeTask(scope, key)
			}
			return
		case outcomeRetry:
		}
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
func (p *Provider) nextDispatch(scope state.Scope, key string) (time.Duration, <-chan struct{}, bool) {
	var (
		wait  time.Duration
		wake  <-chan struct{}
		found bool
	)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.tasks[key]
		if record == nil || s.queues[record.Queue] == nil {
			return nil
		}
		found, wake = true, record.wake
		wait = record.ScheduleAt.Sub(p.clock())
		if s.queues[record.Queue].State == "PAUSED" {
			wait = pausedPoll
		}
		return nil
	})
	return wait, wake, found
}

// attempt performs one dispatch and records it on the task.
func (p *Provider) attempt(ctx context.Context, scope state.Scope, key string) outcome {
	snapshot, dispatched, ok := p.beginAttempt(scope, key)
	if !ok {
		return outcomeRemoved
	}
	code, message, responded := p.send(ctx, scope, snapshot)
	if ctx.Err() != nil {
		return outcomeRemoved
	}
	return p.finishAttempt(scope, key, &attemptRecord{
		Schedule: snapshot.ScheduleAt, Dispatch: dispatched, Response: p.clock(), Code: code, Message: message,
	}, responded)
}

// beginAttempt counts one dispatch and snapshots the request inputs.
func (p *Provider) beginAttempt(scope state.Scope, key string) (taskRecord, time.Time, bool) {
	var snapshot taskRecord
	dispatched, found := p.clock(), false
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.tasks[key]
		if record == nil {
			return nil
		}
		record.Dispatches++
		if record.First == nil {
			record.First = &attemptRecord{Schedule: record.ScheduleAt, Dispatch: dispatched}
		}
		snapshot, found = requestSnapshot(record), true
		return nil
	})
	return snapshot, dispatched, found
}

// finishAttempt records the completed attempt, then removes a succeeded task,
// marks an exhausted one, or schedules the retry from the queue's policy.
func (p *Provider) finishAttempt(scope state.Scope, key string, last *attemptRecord, responded bool) outcome {
	result := outcomeRemoved
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.tasks[key]
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
			delete(s.tasks, key)
		case queue.Retry.exhausted(record.Dispatches):
			result = outcomeExhausted
		default:
			record.ScheduleAt = last.Response.Add(queue.Retry.backoff(record.Dispatches))
			result = outcomeRetry
		}
		return nil
	})
	return result
}

// removeTask deletes one task after its retention.
func (p *Provider) removeTask(scope state.Scope, key string) {
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.tasks, key)
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
	response, err := p.client.Do(request)
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
	headers["X-Cloudtasks-Queuename"] = record.Queue
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
