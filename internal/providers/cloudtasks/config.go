package cloudtasks

import (
	"github.com/evanmarshall/gcp-local/internal/providers/provider"

	"maps"
	"time"
)

// Queue fields GCP returns that the recording compares by presence only. The
// values are the documented GCP defaults, not recorded values.
const (
	defaultTaskTTL      = "2678400s"
	defaultTombstoneTTL = "3600s"
	queueTypePush       = "PUSH"
)

// defaultRateLimits are the documented GCP rate-limit defaults; the recording
// compares only the field's presence.
func defaultRateLimits() map[string]any {
	return map[string]any{"maxDispatchesPerSecond": 500, "maxBurstSize": 100, "maxConcurrentDispatches": 1000}
}

// mergeRates overlays caller-supplied rate limits on the defaults.
func mergeRates(current, update map[string]any) map[string]any {
	merged := defaultRateLimits()
	maps.Copy(merged, current)
	maps.Copy(merged, update)
	return merged
}

// retryPolicy is one queue's retryConfig. MaxAttempts -1 retries without limit.
type retryPolicy struct {
	MaxAttempts  int
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
	MaxDoublings int
}

// defaultRetry is the documented GCP retryConfig default.
func defaultRetry() retryPolicy {
	return retryPolicy{MaxAttempts: 100, MinBackoff: 100 * time.Millisecond, MaxBackoff: time.Hour, MaxDoublings: 16}
}

// parseRetry overlays one retryConfig body on the current policy.
func parseRetry(current retryPolicy, raw any) (retryPolicy, error) {
	body, ok := raw.(map[string]any)
	if raw == nil {
		return current, nil
	}
	if !ok {
		return current, provider.Invalid("Invalid retryConfig")
	}
	next := current
	var valid bool
	if next.MaxAttempts, valid = intField(body, "maxAttempts", current.MaxAttempts); !valid || next.MaxAttempts < -1 {
		return current, provider.Invalid("Invalid retryConfig.maxAttempts")
	}
	if next.MaxDoublings, valid = intField(body, "maxDoublings", current.MaxDoublings); !valid || next.MaxDoublings < 0 {
		return current, provider.Invalid("Invalid retryConfig.maxDoublings")
	}
	if next.MinBackoff, valid = durationField(body, "minBackoff", current.MinBackoff); !valid {
		return current, provider.Invalid("Invalid retryConfig.minBackoff")
	}
	if next.MaxBackoff, valid = durationField(body, "maxBackoff", current.MaxBackoff); !valid || next.MaxBackoff < next.MinBackoff {
		return current, provider.Invalid("Invalid retryConfig.maxBackoff")
	}
	return next, nil
}

// intField reads an optional integral JSON number. Zero is the proto3 unset
// value, so it keeps the fallback as GCP does.
func intField(body map[string]any, key string, fallback int) (int, bool) {
	number, ok := body[key].(float64)
	switch {
	case body[key] == nil || ok && number == 0:
		return fallback, true
	case !ok || number != float64(int(number)):
		return 0, false
	}
	return int(number), true
}

// durationField reads an optional protobuf JSON duration; zero keeps the
// fallback like intField.
func durationField(body map[string]any, key string, fallback time.Duration) (time.Duration, bool) {
	if body[key] == nil {
		return fallback, true
	}
	text, ok := body[key].(string)
	if !ok {
		return 0, false
	}
	value, valid := parseDuration(text)
	if valid && value == 0 {
		return fallback, true
	}
	return value, valid
}

// wire renders the retryConfig echo.
func (r retryPolicy) wire() map[string]any {
	return map[string]any{
		"maxAttempts": r.MaxAttempts, "minBackoff": formatDuration(r.MinBackoff),
		"maxBackoff": formatDuration(r.MaxBackoff), "maxDoublings": r.MaxDoublings,
	}
}

// exhausted reports whether a task with this many dispatches is out of attempts.
func (r retryPolicy) exhausted(dispatches int) bool {
	return r.MaxAttempts >= 0 && dispatches >= r.MaxAttempts
}

// backoff is the documented retry interval after the given number of failed
// attempts: minBackoff doubles maxDoublings times, then grows linearly, capped
// at maxBackoff.
func (r retryPolicy) backoff(failures int) time.Duration {
	interval := r.MinBackoff
	step := failures - 1
	for i := 0; i < step && i < r.MaxDoublings && interval < r.MaxBackoff; i++ {
		interval *= 2
	}
	if linear := step - r.MaxDoublings; linear > 0 && interval > 0 && interval < r.MaxBackoff {
		if int64(linear) >= int64(r.MaxBackoff/interval) {
			return r.MaxBackoff
		}
		interval += interval * time.Duration(linear)
	}
	return min(interval, r.MaxBackoff)
}
