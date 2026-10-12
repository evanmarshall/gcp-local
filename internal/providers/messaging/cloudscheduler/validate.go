package cloudscheduler

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	// Job time zones resolve from the embedded tz database, not the host's.
	_ "time/tzdata"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Documented limits and defaults of the Job and RetryConfig references.
const (
	maxDescription  = 500
	maxURI          = 2083
	maxRetryCount   = 5
	maxBackoffLimit = time.Hour
	userAgent       = "Google-Cloud-Scheduler"
	octetStream     = "application/octet-stream"
)

// HTTP attempt deadline bounds and default (Job.attempt_deadline reference).
var (
	minDeadline     = 15 * time.Second
	maxDeadline     = 30 * time.Minute
	defaultDeadline = 3 * time.Minute
)

// cronParser reads the unix-cron format Cloud Scheduler documents: five
// fields, names for months and weekdays, no seconds and no descriptors.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// bodyMethods are the HTTP methods that may carry a body.
var bodyMethods = map[schedulerpb.HttpMethod]bool{
	schedulerpb.HttpMethod_POST: true, schedulerpb.HttpMethod_PUT: true, schedulerpb.HttpMethod_PATCH: true,
}

// newRecord validates a job, fills its documented defaults in place, and
// returns its record with the parsed schedule and time zone.
func newRecord(job *schedulerpb.Job, project string) (*jobRecord, error) {
	if len(job.GetDescription()) > maxDescription {
		return nil, provider.Invalid("The description must not exceed 500 characters")
	}
	schedule, err := parseSchedule(job.GetSchedule())
	if err != nil {
		return nil, err
	}
	zone, err := parseZone(job.GetTimeZone())
	if err != nil {
		return nil, err
	}
	if err := normalizeTarget(job, project); err != nil {
		return nil, err
	}
	if err := normalizeRetry(job); err != nil {
		return nil, err
	}
	return &jobRecord{job: job, schedule: schedule, zone: zone, wake: make(chan struct{}, 1)}, nil
}

// parseSchedule parses a unix-cron schedule. The English-like App Engine
// syntax ("every 5 minutes") is a coverage gap.
func parseSchedule(text string) (*cron.SpecSchedule, error) {
	if strings.TrimSpace(text) == "" {
		return nil, provider.Invalid("A schedule is required")
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "every ") {
		return nil, gap("English-like schedules are not implemented; use unix-cron")
	}
	schedule, err := cronParser.Parse(text)
	if err != nil {
		return nil, provider.Invalid("Invalid schedule " + text + ": " + err.Error())
	}
	// Without descriptors the parser returns only field schedules.
	spec, ok := schedule.(*cron.SpecSchedule)
	if !ok {
		return nil, provider.Invalid("Invalid schedule " + text)
	}
	return spec, nil
}

// parseZone resolves a tz database name; empty and "utc" are UTC.
func parseZone(name string) (*time.Location, error) {
	if name == "" || strings.EqualFold(name, "utc") {
		return time.UTC, nil
	}
	zone, err := time.LoadLocation(name)
	if err != nil || name == "Local" {
		return nil, provider.Invalid("Invalid time zone " + name)
	}
	return zone, nil
}

// normalizeTarget requires exactly one supported target and fills its
// defaults. decodeJob refuses App Engine targets.
func normalizeTarget(job *schedulerpb.Job, project string) error {
	switch {
	case job.GetPubsubTarget() != nil:
		// Pub/Sub targets have no attempt deadline (recorded).
		job.AttemptDeadline = nil
		return checkPubsubTarget(job.GetPubsubTarget(), project)
	case job.GetHttpTarget() != nil:
		return normalizeHTTPTarget(job)
	}
	return provider.Invalid("A job needs an httpTarget or a pubsubTarget")
}

// checkPubsubTarget requires a same-project topic and a non-empty message.
func checkPubsubTarget(target *schedulerpb.PubsubTarget, project string) error {
	id, found := strings.CutPrefix(target.GetTopicName(), "projects/"+project+"/topics/")
	if !found || id == "" || strings.Contains(id, "/") {
		return provider.Invalid("pubsubTarget.topicName must name a topic in the job's project")
	}
	if len(target.GetData()) == 0 && len(target.GetAttributes()) == 0 {
		return provider.Invalid("A Pub/Sub message needs data or at least one attribute")
	}
	return nil
}

// normalizeHTTPTarget validates the URI, method, body, and token, and fills
// the defaults GCP echoes: POST, User-Agent, an octet-stream Content-Type for
// a body, the URI as OIDC audience, cloud-platform as OAuth scope, and a
// three-minute attempt deadline.
func normalizeHTTPTarget(job *schedulerpb.Job) error {
	target := job.GetHttpTarget()
	if err := checkRequest(target); err != nil {
		return err
	}
	target.Headers = targetHeaders(target)
	if err := normalizeToken(target); err != nil {
		return err
	}
	deadline := defaultDeadline
	if job.GetAttemptDeadline() != nil {
		deadline = job.GetAttemptDeadline().AsDuration()
	}
	if deadline < minDeadline || deadline > maxDeadline {
		return provider.Invalid("attemptDeadline must be between 15 seconds and 30 minutes for HTTP targets")
	}
	job.AttemptDeadline = durationpb.New(deadline)
	return nil
}

// checkRequest validates the target URI and the body's method, defaulting
// the method to POST.
func checkRequest(target *schedulerpb.HttpTarget) error {
	parsed, err := url.Parse(target.GetUri())
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || len(target.GetUri()) > maxURI {
		return provider.Invalid("httpTarget.uri must be an absolute http or https URL")
	}
	if target.GetHttpMethod() == schedulerpb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		target.HttpMethod = schedulerpb.HttpMethod_POST
	}
	if len(target.GetBody()) > 0 && !bodyMethods[target.GetHttpMethod()] {
		return provider.Invalid("A request body needs the POST, PUT, or PATCH method")
	}
	return nil
}

// targetHeaders drops the headers GCP replaces or computes (HttpTarget
// reference), sets its User-Agent, and defaults a body's Content-Type.
func targetHeaders(target *schedulerpb.HttpTarget) map[string]string {
	headers := map[string]string{}
	for key, value := range target.GetHeaders() {
		switch http.CanonicalHeaderKey(key) {
		case "User-Agent", "Host", "Content-Length":
		default:
			headers[key] = value
		}
	}
	headers["User-Agent"] = userAgent
	if len(target.GetBody()) > 0 && !hasHeader(headers, "Content-Type") {
		headers["Content-Type"] = octetStream
	}
	return headers
}

// hasHeader reports whether headers holds name in any case.
func hasHeader(headers map[string]string, name string) bool {
	for key := range headers {
		if http.CanonicalHeaderKey(key) == name {
			return true
		}
	}
	return false
}

// normalizeToken requires the token's service account and fills defaults.
func normalizeToken(target *schedulerpb.HttpTarget) error {
	if oidc := target.GetOidcToken(); oidc != nil {
		if oidc.GetServiceAccountEmail() == "" {
			return provider.Invalid("oidcToken.serviceAccountEmail is required")
		}
		if oidc.GetAudience() == "" {
			oidc.Audience = target.GetUri()
		}
	}
	if oauth := target.GetOauthToken(); oauth != nil {
		if oauth.GetServiceAccountEmail() == "" {
			return provider.Invalid("oauthToken.serviceAccountEmail is required")
		}
		if oauth.GetScope() == "" {
			oauth.Scope = identity.CloudPlatform
		}
	}
	return nil
}

// tokenAccount names the service account a job's HTTP target mints a token for.
func tokenAccount(job *schedulerpb.Job) string {
	target := job.GetHttpTarget()
	if email := target.GetOidcToken().GetServiceAccountEmail(); email != "" {
		return email
	}
	return target.GetOauthToken().GetServiceAccountEmail()
}

// normalizeRetry fills the documented RetryConfig defaults on a supplied
// configuration and checks its bounds. An absent configuration stays absent.
func normalizeRetry(job *schedulerpb.Job) error {
	retry := job.GetRetryConfig()
	if retry == nil {
		return nil
	}
	defaultDuration(&retry.MaxRetryDuration, 0)
	defaultDuration(&retry.MinBackoffDuration, 5*time.Second)
	defaultDuration(&retry.MaxBackoffDuration, time.Hour)
	if retry.GetMaxDoublings() == 0 {
		retry.MaxDoublings = 5
	}
	minimum, maximum := retry.GetMinBackoffDuration().AsDuration(), retry.GetMaxBackoffDuration().AsDuration()
	switch {
	case retry.GetRetryCount() < 0 || retry.GetRetryCount() > maxRetryCount:
		return provider.Invalid("retryConfig.retryCount must be between 0 and 5")
	case retry.GetMaxRetryDuration().AsDuration() < 0 || retry.GetMaxDoublings() < 0:
		return provider.Invalid("Invalid retryConfig")
	case minimum < 0 || maximum > maxBackoffLimit || minimum > maximum:
		return provider.Invalid("retryConfig backoff durations must satisfy 0 <= minBackoffDuration <= maxBackoffDuration <= 3600s")
	}
	return nil
}

// defaultDuration sets an absent duration field to its default.
func defaultDuration(field **durationpb.Duration, value time.Duration) {
	if *field == nil {
		*field = durationpb.New(value)
	}
}

// outputFields are the Job fields an update mask may not name.
var outputFields = map[protoreflect.Name]bool{
	"name": true, "user_update_time": true, "state": true, "status": true,
	"schedule_time": true, "last_attempt_time": true,
}

// maskPaths resolves an update mask to field paths of Job, accepting JSON or
// proto names. Without a mask, every field the update sets is updated.
func maskPaths(mask string, update *schedulerpb.Job) ([][]protoreflect.FieldDescriptor, error) {
	descriptor := update.ProtoReflect().Descriptor()
	var paths [][]protoreflect.FieldDescriptor
	if strings.TrimSpace(mask) == "" {
		update.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			if !outputFields[field.Name()] {
				paths = append(paths, []protoreflect.FieldDescriptor{field})
			}
			return true
		})
		return paths, nil
	}
	for path := range strings.SplitSeq(mask, ",") {
		resolved, err := resolvePath(descriptor, strings.TrimSpace(path))
		if err != nil {
			return nil, err
		}
		paths = append(paths, resolved)
	}
	return paths, nil
}

// resolvePath resolves one dotted mask path; only message fields may have
// children, and output fields are refused.
func resolvePath(descriptor protoreflect.MessageDescriptor, path string) ([]protoreflect.FieldDescriptor, error) {
	var fields []protoreflect.FieldDescriptor
	for i, segment := range strings.Split(path, ".") {
		if descriptor == nil {
			return nil, provider.Invalid("Invalid update mask path " + path)
		}
		field := descriptor.Fields().ByName(protoreflect.Name(segment))
		if field == nil {
			field = descriptor.Fields().ByJSONName(segment)
		}
		if field == nil || (i == 0 && outputFields[field.Name()]) {
			return nil, provider.Invalid("Invalid update mask path " + path)
		}
		fields = append(fields, field)
		descriptor = field.Message()
		if field.IsMap() || field.IsList() {
			descriptor = nil
		}
	}
	return fields, nil
}

// applyMask copies each masked field from update onto a copy of current; a
// field the update leaves unset is cleared.
func applyMask(current, update *schedulerpb.Job, paths [][]protoreflect.FieldDescriptor) *schedulerpb.Job {
	merged := proto.CloneOf(current)
	for _, path := range paths {
		target, source := merged.ProtoReflect(), update.ProtoReflect()
		for _, field := range path[:len(path)-1] {
			target = target.Mutable(field).Message()
			source = source.Get(field).Message()
		}
		leaf := path[len(path)-1]
		if source.Has(leaf) {
			target.Set(leaf, source.Get(leaf))
		} else {
			target.Clear(leaf)
		}
	}
	return merged
}

// touchesTarget reports whether an update changes the job's target.
func touchesTarget(paths [][]protoreflect.FieldDescriptor) bool {
	for _, path := range paths {
		switch path[0].Name() {
		case "http_target", "pubsub_target", "app_engine_http_target":
			return true
		}
	}
	return false
}
