package cloudscheduler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// publishMethod is the Pub/Sub RPC a Pub/Sub target job calls.
const publishMethod = "/google.pubsub.v1.Publisher/Publish"

// launch starts the job's worker. The worker acts as the Cloud Scheduler
// service agent, never as the caller that created the job.
func (p *Provider) launch(scope state.Scope, key string) {
	ctx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	if previous := p.workers[provider.WorkerKey(scope, key)]; previous != nil {
		previous()
	}
	p.workers[provider.WorkerKey(scope, key)] = cancel
	p.mu.Unlock()
	go p.work(ctx, scope, key)
}

// step is what a worker does next.
type step int

const (
	stepWait step = iota
	stepScheduled
	stepManual
	stepStop
)

// work runs the job's executions: one at each scheduled time while the job
// is enabled, and one for each RunJob request. One execution, retries
// included, finishes before the next starts, as GCP documents.
func (p *Provider) work(ctx context.Context, scope state.Scope, key string) {
	for {
		next, wait, wake := p.plan(scope, key)
		switch next {
		case stepStop:
			return
		case stepScheduled, stepManual:
			p.runExecution(ctx, scope, key)
			continue
		case stepWait:
		}
		var timer <-chan time.Time
		if wait >= 0 {
			timer = p.clock.After(wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-timer:
		}
	}
}

// plan reads what the worker does next. wait is -1 while only a wake (a
// change or RunJob) can end the wait.
func (p *Provider) plan(scope state.Scope, key string) (step, time.Duration, <-chan struct{}) {
	result, wait, wake := stepStop, time.Duration(-1), (<-chan struct{})(nil)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.jobs[key]
		if record == nil {
			return nil
		}
		result, wake = stepWait, record.wake
		switch {
		case record.run:
			record.run, result = false, stepManual
		case record.job.GetState() == schedulerpb.Job_ENABLED && record.job.GetScheduleTime() != nil:
			wait = record.job.GetScheduleTime().AsTime().Sub(p.clock.Now())
			if wait <= 0 {
				result = stepScheduled
			}
		}
		return nil
	})
	return result, wait, wake
}

// runExecution runs one execution: an attempt, then retries under the job's
// retryConfig until one succeeds, the retries run out, or the job is paused
// or deleted. It then schedules the next execution after now.
func (p *Provider) runExecution(ctx context.Context, scope state.Scope, key string) {
	first := p.clock.Now()
	for retries := 0; ; retries++ {
		job, ok := p.snapshot(scope, key)
		if !ok || (retries > 0 && job.GetState() == schedulerpb.Job_PAUSED) {
			return
		}
		result := attempt{started: p.clock.Now()}
		result.code, result.message = p.deliver(ctx, scope.Project, job)
		if ctx.Err() != nil {
			return
		}
		wait, retry := p.finishAttempt(scope, key, result, retries, first)
		if !retry {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.clock.After(wait):
		}
	}
}

// snapshot copies the job an attempt delivers.
func (p *Provider) snapshot(scope state.Scope, key string) (*schedulerpb.Job, bool) {
	var job *schedulerpb.Job
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.jobs[key]; record != nil {
			job = proto.CloneOf(record.job)
		}
		return nil
	})
	return job, job != nil
}

// attempt is one completed delivery: when it started and its RPC status.
type attempt struct {
	started time.Time
	code    codes.Code
	message string
}

// finishAttempt records the attempt and decides on a retry. GCP reports
// lastAttemptTime (the start) together with the attempt's status once the
// attempt completes (recording cloudscheduler-job-lifecycle). A retry sets
// scheduleTime to the retry; otherwise scheduleTime is the next scheduled
// time after now, or absent while paused.
func (p *Provider) finishAttempt(scope state.Scope, key string, result attempt, retries int, first time.Time) (time.Duration, bool) {
	var (
		wait  time.Duration
		retry bool
	)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.jobs[key]
		if record == nil {
			return nil
		}
		now := p.clock.Now()
		record.job.LastAttemptTime = timestamppb.New(result.started)
		record.job.Status = status.New(result.code, result.message).Proto()
		enabled := record.job.GetState() == schedulerpb.Job_ENABLED
		if result.code != codes.OK && enabled {
			wait, retry = retryAfter(record.job.GetRetryConfig(), retries, now.Sub(first))
		}
		switch {
		case retry:
			record.job.ScheduleTime = timestamppb.New(now.Add(wait))
		case enabled:
			record.job.ScheduleTime = timestamppb.New(record.schedule.Next(now.In(record.zone)))
		default:
			record.job.ScheduleTime = nil
		}
		return nil
	})
	return wait, retry
}

// retryAfter applies RetryConfig: retry while fewer than retryCount retries
// ran or, with maxRetryDuration set, while the execution is younger than it;
// with both set, until both limits are reached. Without a configuration, or
// with both zero, a failed attempt waits for the next scheduled time.
func retryAfter(config *schedulerpb.RetryConfig, retries int, elapsed time.Duration) (time.Duration, bool) {
	limit := config.GetMaxRetryDuration().AsDuration()
	if retries >= int(config.GetRetryCount()) && (limit <= 0 || elapsed >= limit) {
		return 0, false
	}
	return provider.Backoff(config.GetMinBackoffDuration().AsDuration(), config.GetMaxBackoffDuration().AsDuration(),
		int(config.GetMaxDoublings()), retries+1), true
}

// deliver performs one attempt as the project's Cloud Scheduler service agent.
func (p *Provider) deliver(ctx context.Context, project string, job *schedulerpb.Job) (codes.Code, string) {
	agent, err := p.serviceAgent(ctx, project)
	if err != nil {
		return status.Code(err), status.Convert(err).Message()
	}
	if target := job.GetPubsubTarget(); target != nil {
		return p.publish(agent, target)
	}
	return p.send(ctx, agent, job)
}

// serviceAgent is the per-project Cloud Scheduler service agent principal,
// named by project number as GCP names it.
func (p *Provider) serviceAgent(ctx context.Context, project string) (gateway.Call, error) {
	resource, ok := p.index.Get(provider.ProjectResource(project))
	if !ok || resource.Number == "" {
		return gateway.Call{}, gap("the Cloud Scheduler service agent needs the project's number in the resource index")
	}
	return gateway.Call{Context: ctx, Project: project, Principal: gateway.Principal{
		State: gateway.Authenticated, Subject: "serviceAccount:service-" + resource.Number + "@gcp-sa-cloudscheduler.iam.gserviceaccount.com",
		Project: project, Scopes: []string{identity.CloudPlatform},
	}}, nil
}

// publish sends the target's message through the Pub/Sub provider, which
// authorizes the service agent for pubsub.topics.publish on the topic.
func (p *Provider) publish(agent gateway.Call, target *schedulerpb.PubsubTarget) (codes.Code, string) {
	agent.Method, agent.Resource = publishMethod, target.GetTopicName()
	request := &pubsubpb.PublishRequest{Topic: target.GetTopicName(), Messages: []*pubsubpb.PubsubMessage{
		{Data: target.GetData(), Attributes: target.GetAttributes()},
	}}
	if _, err := p.registry.Invoke(agent, request); err != nil {
		return status.Code(err), status.Convert(err).Message()
	}
	return codes.OK, ""
}

// send performs one HTTP attempt within the job's attempt deadline. A target
// that does not answer in time fails DEADLINE_EXCEEDED; an unreachable one
// fails UNAVAILABLE.
func (p *Provider) send(ctx context.Context, agent gateway.Call, job *schedulerpb.Job) (codes.Code, string) {
	target := job.GetHttpTarget()
	ctx, cancel := context.WithTimeout(ctx, job.GetAttemptDeadline().AsDuration())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, target.GetHttpMethod().String(), target.GetUri(), bytes.NewReader(target.GetBody()))
	if err != nil {
		return codes.InvalidArgument, "The job request could not be built"
	}
	for key, value := range target.GetHeaders() {
		request.Header.Set(key, value)
	}
	request.Header.Set("X-Cloudscheduler", "true")
	request.Header.Set("X-Cloudscheduler-Jobname", job.GetName()[strings.LastIndex(job.GetName(), "/")+1:])
	if authorization, err := p.authorization(agent, job); err != nil {
		return status.Code(err), status.Convert(err).Message()
	} else if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := p.client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return codes.DeadlineExceeded, "The attempt exceeded its deadline"
		}
		return codes.Unavailable, "The target could not be reached"
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return codes.OK, ""
	}
	return provider.HTTPCode(response.StatusCode), http.StatusText(response.StatusCode)
}

// authorization mints the target's OIDC or OAuth token for the job's service
// account. The service agent needs iam.serviceAccounts.getOpenIdToken or
// getAccessToken on it, which roles/cloudscheduler.serviceAgent grants.
func (p *Provider) authorization(agent gateway.Call, job *schedulerpb.Job) (string, error) {
	target, project := job.GetHttpTarget(), agent.Project
	if oidc := target.GetOidcToken(); oidc != nil {
		if p.config.IDTokens == nil {
			return "", gap("OIDC tokens need the local ID-token issuer")
		}
		token, err := p.config.IDTokens.Mint(agent, iam.AccountResource(project, oidc.GetServiceAccountEmail()), oidc.GetAudience())
		if err != nil {
			return "", err //nolint:wrapcheck // the status becomes the attempt status
		}
		return "Bearer " + token, nil
	}
	oauth := target.GetOauthToken()
	if oauth == nil {
		return "", nil
	}
	if p.config.Identity == nil {
		return "", gap("OAuth tokens need the local identity service")
	}
	account := iam.AccountResource(project, oauth.GetServiceAccountEmail())
	agent.Method, agent.Resource = methodImpersonate, account
	if _, err := p.engine.Authorize(agent, iam.Target{Name: account}, nil); err != nil {
		return "", err //nolint:wrapcheck // the status becomes the attempt status
	}
	token, err := p.config.Identity.Impersonate("serviceAccount:"+oauth.GetServiceAccountEmail(), strings.Fields(oauth.GetScope()))
	if err != nil {
		return "", gap("OAuth tokens need the service account enrolled in the local identity configuration")
	}
	return "Bearer " + token, nil
}
