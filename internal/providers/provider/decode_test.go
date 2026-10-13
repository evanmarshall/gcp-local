package provider

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var testGap = CoverageGap("Test")

// violation returns a refusal's code, message, and BadRequest field.
func violation(t *testing.T, err error) (codes.Code, string, string) {
	t.Helper()
	s := status.Convert(err)
	field := "<none>"
	for _, detail := range s.Details() {
		if bad, ok := detail.(*errdetails.BadRequest); ok && len(bad.GetFieldViolations()) == 1 {
			field = bad.GetFieldViolations()[0].GetField()
		}
	}
	return s.Code(), s.Message(), field
}

func TestDecodeProtoRefusesAsGCPDoes(t *testing.T) {
	for _, tc := range []struct {
		name, body, message, field string
		code                       codes.Code
	}{
		{"unknown top-level name", `{"schedule":"* * * * *","bogus":1}`,
			`Invalid JSON payload received. Unknown name "bogus" at 'job': Cannot find field.`, "job", codes.InvalidArgument},
		{"unknown nested name", `{"pubsubTarget":{"topicName":"t","bogus":1}}`,
			`Invalid JSON payload received. Unknown name "bogus" at 'job.pubsub_target': Cannot find field.`, "job.pubsub_target", codes.InvalidArgument},
		{"string for an integer", `{"retryConfig":{"retryCount":"many"}}`,
			`Invalid value at 'job.retry_config.retry_count' (TYPE_INT32), "many"`, "job.retry_config.retry_count", codes.InvalidArgument},
		{"number for a string", `{"schedule":5}`,
			`Invalid value at 'job.schedule' (TYPE_STRING), 5`, "job.schedule", codes.InvalidArgument},
		{"string for a message", `{"retryConfig":"x"}`,
			`Invalid value at 'job.retry_config' (type.googleapis.com/google.cloud.scheduler.v1.RetryConfig), "x"`, "job.retry_config", codes.InvalidArgument},
		{"unknown enum value", `{"httpTarget":{"httpMethod":"FETCH"}}`,
			`Invalid value at 'job.http_target.http_method' (type.googleapis.com/google.cloud.scheduler.v1.HttpMethod), "FETCH"`, "job.http_target.http_method", codes.InvalidArgument},
		{"malformed duration", `{"attemptDeadline":"soon"}`,
			`Invalid value at 'job.attempt_deadline' (type.googleapis.com/google.protobuf.Duration), "soon"`, "job.attempt_deadline", codes.InvalidArgument},
		{"number in a string map", `{"httpTarget":{"headers":{"A":1}}}`,
			`Invalid value at 'job.http_target.headers[A]' (TYPE_STRING), 1`, "job.http_target.headers[A]", codes.InvalidArgument},
		{"unmodeled field", `{"appEngineHttpTarget":{"relativeUri":"/"}}`,
			"Test coverage gap: appEngineHttpTarget is not modeled", "<none>", codes.Unimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := DecodeProto([]byte(tc.body), "job", &schedulerpb.Job{}, testGap, "appEngineHttpTarget")
			code, message, field := violation(t, err)
			if code != tc.code || message != tc.message || field != tc.field {
				t.Fatalf("got %v %q field %q", code, message, field)
			}
		})
	}
}

func TestDecodeProtoAcceptsProtoJSON(t *testing.T) {
	job := &schedulerpb.Job{}
	body := `{"schedule":"* * * * *","retry_config":{"retryCount":"2","maxDoublings":3},"attemptDeadline":"30s",
		"httpTarget":{"httpMethod":"PUT","headers":{"A":"b"},"body":"eA=="},"appEngineHttpTarget":null,"state":"PAUSED"}`
	if err := DecodeProto([]byte(body), "job", job, testGap, "appEngineHttpTarget"); err != nil {
		t.Fatal(err)
	}
	if job.GetRetryConfig().GetRetryCount() != 2 || job.GetHttpTarget().GetHttpMethod() != schedulerpb.HttpMethod_PUT ||
		job.GetAttemptDeadline().GetSeconds() != 30 || string(job.GetHttpTarget().GetBody()) != "x" {
		t.Fatal(job)
	}
	if err := DecodeProto(nil, "job", &schedulerpb.Job{}, testGap); err != nil {
		t.Fatal(err)
	}
}

type testBody struct {
	Name     string            `json:"name"`
	Size     *int64            `json:"size"`
	Labels   map[string]string `json:"labels"`
	Settings struct {
		Tier  string   `json:"tier"`
		Flags []string `json:"flags"`
	} `json:"settings"`
	Generation int64           `json:"generation,string"`
	Opaque     json.RawMessage `json:"opaque"`
}

var testFields = Fields{Unmodeled: []string{"replicas", "settings.backup", "zones"}, OutputOnly: []string{"createTime"}}

func TestDecodeJSONRefusesUnknownUnmodeledAndMistypedFields(t *testing.T) {
	for _, tc := range []struct {
		name, body, message, field string
		code                       codes.Code
	}{
		{"unmodeled top-level field", `{"name":"a","replicas":2}`, "Test coverage gap: replicas is not modeled", "<none>", codes.Unimplemented},
		{"unmodeled nested field", `{"settings":{"tier":"x","backup":{"enabled":true}}}`, "Test coverage gap: settings.backup is not modeled", "<none>", codes.Unimplemented},
		{"unknown name", `{"bogus":1}`, `Invalid JSON payload received. Unknown name "bogus" at 'thing': Cannot find field.`, "thing", codes.InvalidArgument},
		{"unknown nested name", `{"settings":{"bogus":1}}`, `Invalid JSON payload received. Unknown name "bogus" at 'thing.settings': Cannot find field.`, "thing.settings", codes.InvalidArgument},
		{"number for a string", `{"name":5}`, "Invalid value at 'thing.name' (TYPE_STRING), 5", "thing.name", codes.InvalidArgument},
		{"string for an integer", `{"size":"big"}`, `Invalid value at 'thing.size' (TYPE_INT64), "big"`, "thing.size", codes.InvalidArgument},
		{"number in a list of strings", `{"settings":{"flags":["a",1]}}`, "Invalid value at 'thing.settings.flags[1]' (TYPE_STRING), 1", "thing.settings.flags[1]", codes.InvalidArgument},
		{"array for an object", `{"settings":[]}`, "Invalid value at 'thing.settings' (TYPE_MESSAGE), []", "thing.settings", codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, message, field := violation(t, DecodeJSON([]byte(tc.body), "thing", &testBody{}, testGap, testFields))
			if code != tc.code || message != tc.message || field != tc.field {
				t.Fatalf("got %v %q field %q", code, message, field)
			}
		})
	}
}

func TestDecodeJSONAcceptsDefaultsAndOutputFields(t *testing.T) {
	var body testBody
	raw := `{"name":"a","size":3,"labels":{"k":"v"},"settings":{"tier":"t","flags":["x"],"backup":{}},"generation":"7",
		"opaque":{"any":[1]},"createTime":"2026-01-01T00:00:00Z","replicas":0,"zones":[],"name2":null}`
	err := DecodeJSON([]byte(raw), "thing", &body, testGap, testFields)
	if code, message, _ := violation(t, err); code != codes.InvalidArgument || message != `Invalid JSON payload received. Unknown name "name2" at 'thing': Cannot find field.` {
		t.Fatalf("an unknown null field: %v", err)
	}
	raw = strings.Replace(raw, `,"name2":null`, "", 1)
	if err := DecodeJSON([]byte(raw), "thing", &body, testGap, testFields); err != nil {
		t.Fatal(err)
	}
	if body.Name != "a" || *body.Size != 3 || body.Settings.Flags[0] != "x" || body.Generation != 7 || string(body.Opaque) != `{"any":[1]}` {
		t.Fatalf("%+v", body)
	}
	if err := DecodeJSON([]byte(" "), "thing", &testBody{}, testGap, Fields{}); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeJSONCanIgnoreUnknownNames(t *testing.T) {
	fields := testFields
	fields.IgnoreUnknown = true
	var body testBody
	if err := DecodeJSON([]byte(`{"name":"a","bogus":1,"settings":{"other":true}}`), "thing", &body, testGap, fields); err != nil || body.Name != "a" {
		t.Fatalf("unknown names: %v %+v", err, body)
	}
	if code, _, _ := violation(t, DecodeJSON([]byte(`{"replicas":2,"bogus":1}`), "thing", &body, testGap, fields)); code != codes.Unimplemented {
		t.Fatalf("an unmodeled field must stay a gap: %v", code)
	}
}

func TestUnmodeledListsEveryUncoveredInputField(t *testing.T) {
	got := Unmodeled(&schedulerpb.Job{}, "name", "pubsubTarget", "retryConfig.retryCount")
	for _, want := range []string{"description", "httpTarget", "retryConfig.maxRetryDuration"} {
		if !slices.Contains(got, want) {
			t.Errorf("Unmodeled lacks %s: %v", want, got)
		}
	}
	for _, covered := range []string{"name", "pubsubTarget", "pubsubTarget.topicName", "retryConfig", "retryConfig.retryCount"} {
		if slices.Contains(got, covered) {
			t.Errorf("Unmodeled lists covered %s", covered)
		}
	}
}

func TestMaskPathsAndApplyMask(t *testing.T) {
	current := &schedulerpb.Job{Description: "old", Schedule: "* * * * *", RetryConfig: &schedulerpb.RetryConfig{RetryCount: 1, MaxDoublings: 2}}
	update := &schedulerpb.Job{Description: "new", RetryConfig: &schedulerpb.RetryConfig{RetryCount: 3, MaxDoublings: 9}}
	paths, err := MaskPaths("description,retry_config.retryCount,schedule", update, map[protoreflect.Name]bool{"state": true})
	if err != nil {
		t.Fatal(err)
	}
	merged := ApplyMask(current, update, paths)
	if merged.GetDescription() != "new" || merged.GetSchedule() != "" || merged.GetRetryConfig().GetRetryCount() != 3 || merged.GetRetryConfig().GetMaxDoublings() != 2 {
		t.Fatalf("merged = %v", merged)
	}
	if _, err := MaskPaths("state", update, map[protoreflect.Name]bool{"state": true}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("output path = %v", err)
	}
}
