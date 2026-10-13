package bucketconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// maxRetentionPeriod is GCP's bound on a bucket retention period: under 100
// years of seconds (storage/docs/bucket-lock#retention-periods).
const maxRetentionPeriod = 3_155_760_000

// ParseRetentionPeriod reads a bucket retentionPolicy's retentionPeriod, a
// JSON string or number of seconds. GCP refuses a non-integer with its
// parser's error and a period outside (0, 100 years) with its own (recorded:
// storage-retention-holds "policy-text", "policy-zero", "policy-negative").
// The policy's other fields (effectiveTime, isLocked) are output-only.
func ParseRetentionPeriod(raw json.RawMessage) (int64, error) {
	var policy struct {
		Period json.RawMessage `json:"retentionPeriod"`
	}
	if json.Unmarshal(raw, &policy) != nil || len(policy.Period) == 0 {
		return 0, GapError("retentionPolicy without a retentionPeriod")
	}
	text := string(bytes.TrimSpace(policy.Period))
	if unquoted, err := strconv.Unquote(text); err == nil {
		text = unquoted
	}
	seconds, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, invalid("Parse Error: Invalid value for TYPE_INT64 field: '%s'.", bytes.TrimSpace(policy.Period))
	}
	if seconds <= 0 || seconds >= maxRetentionPeriod {
		return 0, invalid("Retention policy must have a retention period greater than 0 and less than 100 years.")
	}
	return seconds, nil
}

// Object retention modes (objects resource, retention.mode).
const (
	ModeUnlocked = "Unlocked"
	ModeLocked   = "Locked"
)

// Retention is one object's retention configuration.
type Retention struct {
	Mode  string
	Until time.Time
}

// MarshalJSON renders retainUntilTime as Storage writes times.
func (r Retention) MarshalJSON() ([]byte, error) {
	encoded, err := json.Marshal(map[string]string{"mode": r.Mode, "retainUntilTime": r.Until.UTC().Format("2006-01-02T15:04:05.000Z")})
	if err != nil {
		return nil, fmt.Errorf("encode the object retention: %w", err)
	}
	return encoded, nil
}

// ParseRetention reads an objects.patch retention value: nil for JSON null,
// which removes it. Both fields are required, the mode is one of two, and
// the time must be after now (recorded: storage-object-retention
// "mode-unknown", "mode-missing", "until-missing", "until-past").
func ParseRetention(raw json.RawMessage, now time.Time) (*Retention, error) {
	if string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil //nolint:nilnil // JSON null removes the retention
	}
	var value struct {
		Mode  *string `json:"mode"`
		Until *string `json:"retainUntilTime"`
	}
	if json.Unmarshal(raw, &value) != nil {
		return nil, GapError("retention values of another JSON type")
	}
	switch {
	case value.Mode != nil && *value.Mode != ModeUnlocked && *value.Mode != ModeLocked:
		return nil, invalid("Invalid value for: %s is not a valid value", *value.Mode)
	case value.Mode == nil:
		return nil, invalid("Missing retention mode. Both mode and retain until time are required.")
	case value.Until == nil:
		return nil, invalid("Missing retain until time. Both mode and retain until time are required.")
	}
	until, err := time.Parse(time.RFC3339Nano, *value.Until)
	if err != nil {
		return nil, GapError("retainUntilTime that is not an RFC 3339 time")
	}
	if !until.After(now) {
		return nil, invalid("Retain until time cannot be in the past.")
	}
	return &Retention{Mode: *value.Mode, Until: until}, nil
}

// ChangeRetention checks replacing an object's retention with next (nil
// removes it). Unlocked retention may be removed or shortened only with
// overrideUnlockedRetention; locked retention may only be extended (recorded:
// storage-object-retention "unlocked-shortened", "unlocked-removed",
// "locked-removed", "locked-unlocked").
func ChangeRetention(current, next *Retention, override bool) error {
	if current == nil {
		return nil
	}
	weakened := next == nil || next.Until.Before(current.Until)
	switch {
	case current.Mode == ModeLocked && (weakened || next.Mode != ModeLocked):
		return forbidden("The locked object retention cannot be removed. Its retention mode cannot be changed and its retention period cannot be shortened.")
	case current.Mode == ModeUnlocked && weakened && !override:
		return forbidden("The unlocked object retention cannot be removed and its retention period cannot be shortened without overriding unlocked retention intent and permission.")
	}
	return nil
}

// Protection is what keeps one object generation from being deleted,
// replaced, or made noncurrent.
type Protection struct {
	TemporaryHold, EventBasedHold bool
	// Until is the later of the bucket retention policy's expiration and the
	// object's own retainUntilTime; zero when neither applies.
	Until time.Time
}

// Refuse returns GCP's refusal for removing the object bucket/name at now,
// or nil. Holds are refused with reason forbidden and retention with
// retentionPolicyNotMet (recorded: storage-retention-holds
// "temporary-hold-delete", "event-hold-delete", "retained-delete").
func (p Protection) Refuse(bucket, name string, now time.Time) *RefusalError {
	hold := ""
	switch {
	case p.TemporaryHold:
		hold = "Temporary"
	case p.EventBasedHold:
		hold = "Event-Based"
	}
	if hold != "" {
		return forbidden("Object '%s/%s' is under active %s hold and cannot be deleted, overwritten or archived until hold is removed.", bucket, name, hold)
	}
	if now.Before(p.Until) {
		return &RefusalError{Status: 403, Reason: "retentionPolicyNotMet", Message: fmt.Sprintf(
			"Object '%s/%s' is subject to bucket's retention policy or object retention and cannot be deleted or overwritten until %s",
			bucket, name, p.Until.UTC().Format("2006-01-02T15:04:05.999999-07:00"))}
	}
	return nil
}
