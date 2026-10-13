package bucketconfig

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// refusal asserts err is a RefusalError with the status and message.
func refusal(t *testing.T, err error, status int, message string) {
	t.Helper()
	refused, ok := errors.AsType[*RefusalError](err)
	if !ok || refused.Status != status || refused.Message != message {
		t.Fatalf("got %v, want %d %q", err, status, message)
	}
}

func TestParseRetentionPeriod(t *testing.T) {
	for raw, want := range map[string]int64{`{"retentionPeriod":"3600"}`: 3600, `{"retentionPeriod":5}`: 5} {
		if got, err := ParseRetentionPeriod(json.RawMessage(raw)); err != nil || got != want {
			t.Fatalf("%s: got %d, %v", raw, got, err)
		}
	}
	bounds := "Retention policy must have a retention period greater than 0 and less than 100 years."
	for _, raw := range []string{`{"retentionPeriod":"0"}`, `{"retentionPeriod":"-1"}`, `{"retentionPeriod":"3155760000"}`} {
		_, err := ParseRetentionPeriod(json.RawMessage(raw))
		refusal(t, err, 400, bounds)
	}
	_, err := ParseRetentionPeriod(json.RawMessage(`{"retentionPeriod":"soon"}`))
	refusal(t, err, 400, `Parse Error: Invalid value for TYPE_INT64 field: '"soon"'.`)
	if _, err := ParseRetentionPeriod(json.RawMessage(`{}`)); !errors.As(err, new(GapError)) {
		t.Fatalf("a policy without a period: %v", err)
	}
}

func TestRetentionChanges(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	parse := func(raw string) *Retention {
		t.Helper()
		retention, err := ParseRetention(json.RawMessage(raw), now)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		return retention
	}
	unlocked := parse(`{"mode":"Unlocked","retainUntilTime":"2026-10-14T00:00:00Z"}`)
	shorter := parse(`{"mode":"Unlocked","retainUntilTime":"2026-10-13T12:00:00Z"}`)
	locked := parse(`{"mode":"Locked","retainUntilTime":"2026-10-14T00:00:00Z"}`)
	longer := parse(`{"mode":"Locked","retainUntilTime":"2026-10-15T00:00:00Z"}`)
	if parse(`null`) != nil {
		t.Fatal("null did not remove the retention")
	}
	weaken := "The unlocked object retention cannot be removed and its retention period cannot be shortened without overriding unlocked retention intent and permission."
	refusal(t, ChangeRetention(unlocked, shorter, false), 403, weaken)
	refusal(t, ChangeRetention(unlocked, nil, false), 403, weaken)
	for _, change := range []struct {
		current, next *Retention
		override      bool
	}{{nil, unlocked, false}, {unlocked, shorter, true}, {unlocked, nil, true}, {unlocked, locked, false}, {locked, longer, false}} {
		if err := ChangeRetention(change.current, change.next, change.override); err != nil {
			t.Fatalf("%+v: %v", change, err)
		}
	}
	permanent := "The locked object retention cannot be removed. Its retention mode cannot be changed and its retention period cannot be shortened."
	refusal(t, ChangeRetention(locked, nil, true), 403, permanent)
	refusal(t, ChangeRetention(locked, unlocked, true), 403, permanent)
	for raw, message := range map[string]string{
		`{"mode":"Forever","retainUntilTime":"2026-10-14T00:00:00Z"}`:  "Invalid value for: Forever is not a valid value",
		`{"mode":"Unlocked","retainUntilTime":"2020-01-01T00:00:00Z"}`: "Retain until time cannot be in the past.",
		`{"mode":"Unlocked"}`:                       "Missing retain until time. Both mode and retain until time are required.",
		`{"retainUntilTime":"2026-10-14T00:00:00Z"}`: "Missing retention mode. Both mode and retain until time are required.",
	} {
		_, err := ParseRetention(json.RawMessage(raw), now)
		refusal(t, err, 400, message)
	}
}

func TestProtectionRefusals(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	refusal(t, (Protection{TemporaryHold: true, EventBasedHold: true}).Refuse("b", "o", now), 403,
		"Object 'b/o' is under active Temporary hold and cannot be deleted, overwritten or archived until hold is removed.")
	refusal(t, (Protection{EventBasedHold: true}).Refuse("b", "o", now), 403,
		"Object 'b/o' is under active Event-Based hold and cannot be deleted, overwritten or archived until hold is removed.")
	retained := (Protection{Until: now.Add(time.Second)}).Refuse("b", "o", now)
	if retained == nil || retained.Reason != "retentionPolicyNotMet" {
		t.Fatalf("retention: %v", retained)
	}
	if (Protection{Until: now}).Refuse("b", "o", now) != nil {
		t.Fatal("expired retention still refuses")
	}
}
