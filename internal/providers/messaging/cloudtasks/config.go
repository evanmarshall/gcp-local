package cloudtasks

import (
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2beta3/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Documented queue defaults and bounds (v2beta3 Queue, RateLimits, and
// RetryConfig references); the recording echoes the defaults.
const (
	defaultTaskTTL      = 31 * 24 * time.Hour
	defaultTombstoneTTL = time.Hour
	maxDispatchRate     = 500
	maxConcurrent       = 5000
)

// normalizeQueue validates a queue body and fills the documented defaults
// GCP echoes. A pull queue and a task TTL other than the default are gaps:
// no pull API is served, and tasks are never expired.
func normalizeQueue(queue *taskspb.Queue) error {
	switch queue.GetType() {
	case taskspb.Queue_PULL:
		return gap("pull queues are not implemented")
	default:
		queue.Type = taskspb.Queue_PUSH
	}
	if err := normalizeRates(queue); err != nil {
		return err
	}
	if err := normalizeRetry(queue); err != nil {
		return err
	}
	if ttl := queue.GetTaskTtl(); ttl != nil && ttl.AsDuration() != defaultTaskTTL {
		return gap("taskTtl other than the default 2678400s is not implemented")
	}
	queue.TaskTtl = durationpb.New(defaultTaskTTL)
	switch ttl := queue.GetTombstoneTtl().AsDuration(); {
	case ttl < 0:
		return provider.Invalid("Queue.tombstoneTtl must not be negative.")
	case ttl == 0:
		queue.TombstoneTtl = durationpb.New(defaultTombstoneTTL)
	}
	return nil
}

// normalizeRates fills and bounds the rate limits; zero is unset.
func normalizeRates(queue *taskspb.Queue) error {
	if queue.GetRateLimits() == nil {
		queue.RateLimits = &taskspb.RateLimits{}
	}
	rates := queue.GetRateLimits()
	switch {
	case rates.GetMaxDispatchesPerSecond() < 0 || rates.GetMaxDispatchesPerSecond() > maxDispatchRate:
		return provider.Invalid("RateLimits.maxDispatchesPerSecond must be between 0 and 500.")
	case rates.GetMaxBurstSize() < 0:
		return provider.Invalid("RateLimits.maxBurstSize must not be negative.")
	case rates.GetMaxConcurrentDispatches() < 0 || rates.GetMaxConcurrentDispatches() > maxConcurrent:
		return provider.Invalid("RateLimits.maxConcurrentDispatches must be between 0 and 5000.")
	}
	if rates.GetMaxDispatchesPerSecond() == 0 {
		rates.MaxDispatchesPerSecond = maxDispatchRate
	}
	if rates.GetMaxBurstSize() == 0 {
		rates.MaxBurstSize = 100
	}
	if rates.GetMaxConcurrentDispatches() == 0 {
		rates.MaxConcurrentDispatches = 1000
	}
	return nil
}

// normalizeRetry fills the retry defaults, then bounds the configuration.
func normalizeRetry(queue *taskspb.Queue) error {
	retry := retryDefaults(queue)
	switch {
	case retry.GetMaxAttempts() < -1:
		return provider.Invalid("Invalid retryConfig.maxAttempts")
	case retry.GetMaxDoublings() < 0:
		return provider.Invalid("Invalid retryConfig.maxDoublings")
	case retry.GetMinBackoff().AsDuration() < 0 || retry.GetMaxRetryDuration().AsDuration() < 0:
		return provider.Invalid("Invalid retryConfig")
	case retry.GetMaxBackoff().AsDuration() < retry.GetMinBackoff().AsDuration():
		return provider.Invalid("Invalid retryConfig.maxBackoff")
	}
	return nil
}

// retryDefaults fills the documented retry defaults; zero is unset, and
// maxRetryDuration stays absent unless set, as GCP echoes it.
func retryDefaults(queue *taskspb.Queue) *taskspb.RetryConfig {
	if queue.GetRetryConfig() == nil {
		queue.RetryConfig = &taskspb.RetryConfig{}
	}
	retry := queue.GetRetryConfig()
	if retry.GetMaxAttempts() == 0 {
		retry.MaxAttempts = 100
	}
	if retry.GetMinBackoff().AsDuration() == 0 {
		retry.MinBackoff = durationpb.New(100 * time.Millisecond)
	}
	if retry.GetMaxBackoff().AsDuration() == 0 {
		retry.MaxBackoff = durationpb.New(time.Hour)
	}
	if retry.GetMaxDoublings() == 0 {
		retry.MaxDoublings = 16
	}
	if retry.GetMaxRetryDuration().AsDuration() == 0 {
		retry.MaxRetryDuration = nil
	}
	return retry
}

// retryPolicy is one queue's retryConfig. MaxAttempts -1 retries without
// limit; MaxDuration 0 has no time limit.
type retryPolicy struct {
	MaxAttempts  int
	MaxDuration  time.Duration
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
	MaxDoublings int
}

// retryOf reads a normalized queue's retry policy.
func retryOf(queue *taskspb.Queue) retryPolicy {
	retry := queue.GetRetryConfig()
	return retryPolicy{
		MaxAttempts: int(retry.GetMaxAttempts()), MaxDuration: retry.GetMaxRetryDuration().AsDuration(),
		MinBackoff: retry.GetMinBackoff().AsDuration(), MaxBackoff: retry.GetMaxBackoff().AsDuration(),
		MaxDoublings: int(retry.GetMaxDoublings()),
	}
}

// exhausted reports whether a task is out of retries: it has had maxAttempts
// attempts and, when maxRetryDuration is set, that long has passed since its
// first attempt. Both must hold (RetryConfig reference).
func (r retryPolicy) exhausted(dispatches int, sinceFirst time.Duration) bool {
	return r.MaxAttempts >= 0 && dispatches >= r.MaxAttempts && (r.MaxDuration == 0 || sinceFirst >= r.MaxDuration)
}

// backoff is the documented retry interval after the given number of failed
// attempts.
func (r retryPolicy) backoff(failures int) time.Duration {
	return provider.Backoff(r.MinBackoff, r.MaxBackoff, r.MaxDoublings, failures)
}

// limiterPoll is how long a dispatch held by maxConcurrentDispatches waits
// before it asks again.
const limiterPoll = 50 * time.Millisecond

// admit takes one dispatch from the queue's rate limits: a token bucket of
// maxBurstSize tokens refilled at maxDispatchesPerSecond, and at most
// maxConcurrentDispatches attempts in flight. It returns how long to wait
// when the dispatch must wait. Call it under the scope lock.
func (r *queueRecord) admit(now time.Time) (time.Duration, bool) {
	rates := r.queue.GetRateLimits()
	burst, rate := float64(rates.GetMaxBurstSize()), rates.GetMaxDispatchesPerSecond()
	if r.refilled.IsZero() {
		r.tokens = burst
	} else {
		r.tokens = min(burst, r.tokens+now.Sub(r.refilled).Seconds()*rate)
	}
	r.refilled = now
	switch {
	case r.inflight >= int(rates.GetMaxConcurrentDispatches()):
		return limiterPoll, false
	case r.tokens < 1:
		return time.Duration((1 - r.tokens) / rate * float64(time.Second)), false
	}
	r.tokens--
	r.inflight++
	return 0, true
}
