package bucketconfig

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestParseLifecycleRefusals follows storage-lifecycle-rules: each invalid
// rule GCP refuses, with its message.
func TestParseLifecycleRefusals(t *testing.T) {
	for rule, message := range map[string]string{
		`{"action":{"type":"Explode"},"condition":{"age":1}}`:                                         "Invalid value for: Explode is not a valid value",
		`{"action":{"type":"SetStorageClass"},"condition":{"age":1}}`:                                 "Lifecycle storage class rules must specify the destination storage class.",
		`{"action":{"type":"SetStorageClass","storageClass":"GLACIER"},"condition":{"age":1}}`:        "Unknown storage class 'GLACIER' is not a valid option for SET_STORAGE_CLASS actions.",
		`{"action":{"type":"SetStorageClass","storageClass":"MULTI_REGIONAL"},"condition":{"age":1}}`: "Project 7 may not create storageClass MULTI_REGIONAL buckets with locationConstraint US-CENTRAL1.",
		`{"action":{"type":"Delete"},"condition":{}}`:                                                 "Lifecycle rules must have a condition.",
		`{"action":{"type":"Delete"}}`:                                                                "Lifecycle rules must have a condition.",
		`{"action":{"type":"Delete"},"condition":{"ageInDays":1}}`:                                    "Lifecycle rules must have a condition.",
		`{"condition":{"age":1}}`:                                                                     "Lifecycle rules must have an action.",
		`{"action":{"type":"AbortIncompleteMultipartUpload"},"condition":{"isLive":true}}`:            "Lifecycle age_days, matches_prefix, and matches_suffix conditions are the only ones allowed for a ABORT_INCOMPLETE_MULTIPART_UPLOAD rule.",
		`{"action":{"type":"Delete"},"condition":{"age":-1}}`:                                         "Lifecycle Age Condition must be greater than or equal to 0, but was -1",
		`{"action":{"type":"Delete"},"condition":{"createdBefore":"2020-01-01T00:00:00Z"}}`:           `Invalid value for: Invalid format: "2020-01-01T00:00:00Z"`,
		`{"action":{"type":"Delete"},"condition":{"matchesStorageClass":["GLACIER"]}}`:                `Invalid storage class "GLACIER"`,
		`{"action":{"type":"Delete"},"condition":{"numNewerVersions":-1}}`:                            "Lifecycle num_newer_versions Condition must be greater than or equal to 0, but was -1",
	} {
		_, err := ParseLifecycle(json.RawMessage(`{"rule":[`+rule+`]}`), "us-central1", "7")
		refusal(t, err, 400, message)
	}
	for _, rule := range []string{
		`{"action":{"type":"Delete"},"condition":{"daysSinceNoncurrentTime":-1}}`,
		`{"action":{"type":"Delete"},"condition":{"age":"30"}}`,
		`{"action":{"type":"SetStorageClass","storageClass":"REGIONAL"},"condition":{"age":1}}`,
	} {
		if _, err := ParseLifecycle(json.RawMessage(`{"rule":[`+rule+`]}`), "us-central1", "7"); !errors.As(err, new(GapError)) {
			t.Fatalf("%s: got %v, want a gap", rule, err)
		}
	}
	for _, raw := range []string{`null`, `{"rule":[]}`, ``} {
		if rules, err := ParseLifecycle(json.RawMessage(raw), "US", "7"); err != nil || rules != nil {
			t.Fatalf("%q: got %v, %v", raw, rules, err)
		}
	}
}

// TestLifecycleRendering keeps zero values a rule sets and drops what it doesn't.
func TestLifecycleRendering(t *testing.T) {
	rules, err := ParseLifecycle(json.RawMessage(`{"rule":[{"action":{"type":"Delete"},"condition":{"age":0,"isLive":false,"matchesPrefix":[]}}]}`), "US", "7")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rules)
	if want := `[{"action":{"type":"Delete"},"condition":{"age":0,"isLive":false}}]`; err != nil || string(encoded) != want {
		t.Fatalf("got %s, want %s", encoded, want)
	}
}

// TestLifecycleApply evaluates each condition and the action precedence.
func TestLifecycleApply(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := created.Add(10 * 24 * time.Hour)
	live := Object{Name: "logs/a.tmp", StorageClass: ClassStandard, Created: created}
	noncurrent := Object{Name: "doc", StorageClass: ClassStandard, Created: created, Noncurrent: created.Add(24 * time.Hour), NewerVersions: 2}
	parse := func(raw string) []Rule {
		t.Helper()
		rules, err := ParseLifecycle(json.RawMessage(`{"rule":[`+raw+`]}`), "US", "7")
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		return rules
	}
	cases := []struct {
		rules  string
		object Object
		want   string
	}{
		{`{"action":{"type":"Delete"},"condition":{"age":10}}`, live, ActionDelete},
		{`{"action":{"type":"Delete"},"condition":{"age":11}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"matchesPrefix":["logs/"],"matchesSuffix":[".tmp"]}}`, live, ActionDelete},
		{`{"action":{"type":"Delete"},"condition":{"matchesPrefix":["logs/"],"matchesSuffix":[".log"]}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"createdBefore":"2026-01-02"}}`, live, ActionDelete},
		{`{"action":{"type":"Delete"},"condition":{"createdBefore":"2026-01-01"}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"isLive":false}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"isLive":false,"numNewerVersions":2,"daysSinceNoncurrentTime":9}}`, noncurrent, ActionDelete},
		{`{"action":{"type":"Delete"},"condition":{"numNewerVersions":3}}`, noncurrent, ""},
		{`{"action":{"type":"Delete"},"condition":{"noncurrentTimeBefore":"2026-01-03"}}`, noncurrent, ActionDelete},
		{`{"action":{"type":"Delete"},"condition":{"noncurrentTimeBefore":"2026-01-03"}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"daysSinceCustomTime":1}}`, live, ""},
		{`{"action":{"type":"Delete"},"condition":{"customTimeBefore":"2027-01-01"}}`, Object{Name: "c", Created: created, CustomTime: created}, ActionDelete},
		{`{"action":{"type":"AbortIncompleteMultipartUpload"},"condition":{"age":0}}`, live, ""},
		{`{"action":{"type":"SetStorageClass","storageClass":"NEARLINE"},"condition":{"age":1}},{"action":{"type":"SetStorageClass","storageClass":"ARCHIVE"},"condition":{"age":1}},{"action":{"type":"SetStorageClass","storageClass":"COLDLINE"},"condition":{"age":1}}`, live, "ARCHIVE"},
		{`{"action":{"type":"SetStorageClass","storageClass":"NEARLINE"},"condition":{"age":1}},{"action":{"type":"Delete"},"condition":{"age":1}}`, live, ActionDelete},
		{`{"action":{"type":"SetStorageClass","storageClass":"STANDARD"},"condition":{"age":1}}`, live, ""},
	}
	for _, test := range cases {
		action, found := Apply(parse(test.rules), test.object, now)
		got := ""
		if found {
			got = action.Type
			if action.Type == ActionSetStorageClass {
				got = action.StorageClass
			}
		}
		if got != test.want {
			t.Fatalf("%s on %+v: got %q, want %q", test.rules, test.object, got, test.want)
		}
	}
}
