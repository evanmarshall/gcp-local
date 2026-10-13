package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file models a secret's lifecycle policies (replication, topics,
// rotation, expiration, version aliases, and delayed version destruction) and
// the event notifications GCP publishes to a secret's topics. Each secret with
// a pending time (next rotation, expiry, or a scheduled destruction) has one
// worker that acts at that time. Notifications are published through the
// Pub/Sub provider as the project's Secret Manager service agent.

// publishMethod is the Pub/Sub RPC event notifications use.
const publishMethod = "/google.pubsub.v1.Publisher/Publish"

// methodTopicPublish is the engine-only plan for the service agent's publish
// permission, checked when topics are configured.
const methodTopicPublish = "secretmanager.topics.publish"

// Documented limits (Secret and Rotation field references).
const (
	maxTopics      = 10
	maxAliases     = 50
	minRotationLag = 300 * time.Second
	minExpiryLead  = time.Minute
	minPeriod      = time.Hour
	maxSpan        = 3153600000 * time.Second // 100 years
	minDestroyTTL  = 24 * time.Hour
	maxDestroyTTL  = 1000 * 24 * time.Hour
)

// Event types and attributes of the Secret Manager event notifications
// reference.
const (
	eventCreate           = "SECRET_CREATE"
	eventUpdate           = "SECRET_UPDATE"
	eventDelete           = "SECRET_DELETE"
	eventRotate           = "SECRET_ROTATE"
	eventVersionAdd       = "SECRET_VERSION_ADD"
	eventVersionEnable    = "SECRET_VERSION_ENABLE"
	eventVersionDisable   = "SECRET_VERSION_DISABLE"
	eventVersionDestroy   = "SECRET_VERSION_DESTROY"
	eventVersionScheduled = "SECRET_VERSION_DESTROY_SCHEDULED"
	eventConfigured       = "TOPIC_CONFIGURED"
)

var (
	topicName  = regexp.MustCompile(`^projects/([^/]+)/topics/([^/]+)$`)
	aliasName  = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,62}$`)
	regionName = regexp.MustCompile(`^[a-z]+(-[a-z]+)+[0-9]+$`)
	// Recorded refusals (secretmanager-secret-policies), with the maximums'
	// wording modeled on the minimums'.
	errRotation  = status.Error(codes.InvalidArgument, "There must be at least one topic configured when a Rotation policy is set.")
	errNoNext    = status.Error(codes.FailedPrecondition, "Next rotation time must be set while the rotation period is set.")
	errNoVersion = status.Error(codes.InvalidArgument, "Aliases cannot be assigned to versions that don't exist")
)

// policies are a secret's lifecycle settings. Zero values mean unset;
// replicas is nil for automatic replication.
type policies struct {
	Replicas   []string
	Topics     []string
	Expire     time.Time
	Next       time.Time
	Period     time.Duration
	Aliases    map[string]int64
	DestroyTTL time.Duration
}

// clone copies the settings so a patch can be validated before it applies.
func (s policies) clone() policies {
	s.Replicas, s.Topics, s.Aliases = slices.Clone(s.Replicas), slices.Clone(s.Topics), maps.Clone(s.Aliases)
	return s
}

// readPolicies validates each setting of a create or update body on its own;
// cross-field rules wait for checkPolicies.
func readPolicies(body *secretmanagerpb.Secret, now time.Time) (policies, error) {
	var read policies
	var err error
	if read.Replicas, err = readReplicas(body.GetReplication()); err != nil {
		return read, err
	}
	for _, topic := range body.GetTopics() {
		read.Topics = append(read.Topics, topic.GetName())
	}
	if len(read.Topics) > maxTopics {
		return read, status.Errorf(codes.InvalidArgument, "A secret can have at most %d topics.", maxTopics)
	}
	if read.Expire, err = readExpiration(body, now); err != nil {
		return read, err
	}
	if read.Next, read.Period, err = readRotation(body.GetRotation()); err != nil {
		return read, err
	}
	if read.Aliases, err = readAliases(body.GetVersionAliases()); err != nil {
		return read, err
	}
	if ttl := body.GetVersionDestroyTtl(); ttl != nil {
		read.DestroyTTL = ttl.AsDuration()
		if read.DestroyTTL < minDestroyTTL || read.DestroyTTL > maxDestroyTTL {
			return read, status.Errorf(codes.InvalidArgument, "Version destroy TTL must be at %s.",
				map[bool]string{true: "least [24h]", false: "most [1000d]"}[read.DestroyTTL < minDestroyTTL])
		}
	}
	return read, nil
}

// readReplicas reads a user-managed replica list in request order.
func readReplicas(replication *secretmanagerpb.Replication) ([]string, error) {
	managed := replication.GetUserManaged()
	if managed == nil {
		return nil, nil
	}
	if len(managed.GetReplicas()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "At least one location must be provided for a user_managed policy.")
	}
	var replicas []string
	for _, replica := range managed.GetReplicas() {
		location := replica.GetLocation()
		if !regionName.MatchString(location) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid replica location %q.", location)
		}
		if slices.Contains(replicas, location) {
			return nil, status.Errorf(codes.InvalidArgument, "Duplicate replica location %q.", location)
		}
		replicas = append(replicas, location)
	}
	return replicas, nil
}

// readExpiration turns expireTime or ttl into the expiry time; ttl is input
// only and GCP reports the computed expireTime.
func readExpiration(body *secretmanagerpb.Secret, now time.Time) (time.Time, error) {
	var expire time.Time
	switch {
	case body.GetExpireTime() != nil:
		expire = body.GetExpireTime().AsTime()
	case body.GetTtl() != nil:
		expire = now.Add(body.GetTtl().AsDuration())
	default:
		return time.Time{}, nil
	}
	if expire.Before(now.Add(minExpiryLead)) {
		return time.Time{}, status.Error(codes.InvalidArgument, "Expiration time must be at least [1m] in the future.")
	}
	return expire, nil
}

// readRotation reads a rotation policy and checks the period's range.
func readRotation(rotation *secretmanagerpb.Rotation) (time.Time, time.Duration, error) {
	var next time.Time
	var period time.Duration
	if rotation.GetNextRotationTime() != nil {
		next = rotation.GetNextRotationTime().AsTime()
	}
	if rotation.GetRotationPeriod() != nil {
		period = rotation.GetRotationPeriod().AsDuration()
		switch {
		case period < minPeriod:
			return next, period, status.Error(codes.InvalidArgument, "Rotation period cannot be shorter than [1h].")
		case period > maxSpan:
			return next, period, status.Error(codes.InvalidArgument, "Rotation period cannot be longer than [100y].")
		case period%time.Second != 0:
			return next, period, status.Error(codes.InvalidArgument, "Rotation period must be a whole number of seconds.")
		}
	}
	return next, period, nil
}

// checkNext applies the documented window to a newly set next rotation time.
func checkNext(next, now time.Time) error {
	if !next.IsZero() && next.Before(now.Add(minRotationLag)) {
		return status.Error(codes.InvalidArgument, "Next rotation time must be at least [5m] in the future.")
	}
	if !next.IsZero() && next.After(now.Add(maxSpan)) {
		return status.Error(codes.InvalidArgument, "Next rotation time must be at most [100y] in the future.")
	}
	return nil
}

// readAliases validates alias names; checkPolicies checks their versions.
func readAliases(aliases map[string]int64) (map[string]int64, error) {
	if len(aliases) > maxAliases {
		return nil, status.Errorf(codes.InvalidArgument, "A secret can have at most %d version aliases.", maxAliases)
	}
	for _, alias := range slices.Sorted(maps.Keys(aliases)) {
		if !aliasName.MatchString(alias) || alias == versionLatest || alias == "NEW" {
			return nil, status.Errorf(codes.InvalidArgument, "[%s] must follow pattern [a-zA-Z][a-Aa-Z0-9_-]+, "+
				"be less than 64 characters and cannot be \"latest\" or \"NEW\"", alias)
		}
	}
	return maps.Clone(aliases), nil
}

// checkPolicies applies the cross-field rules to a secret's resulting
// settings: a rotation period needs a next rotation time, rotation needs a
// topic, and each alias names an existing version.
func checkPolicies(settings policies, versions map[string]*versionRecord) error {
	if settings.Period > 0 && settings.Next.IsZero() {
		return errNoNext
	}
	if !settings.Next.IsZero() && len(settings.Topics) == 0 {
		return errRotation
	}
	for _, alias := range slices.Sorted(maps.Keys(settings.Aliases)) {
		if versions[strconv.FormatInt(settings.Aliases[alias], 10)] == nil {
			return errNoVersion
		}
	}
	return nil
}

// checkTopics requires each topic to exist and the project's Secret Manager
// service agent to hold pubsub.topics.publish on it, as GCP documents for
// configuring notifications.
func (p *Provider) checkTopics(ctx context.Context, project string, topics []string) error {
	for _, topic := range topics {
		match := topicName.FindStringSubmatch(topic)
		if match == nil {
			return status.Errorf(codes.InvalidArgument, "Invalid Pub/Sub topic name %q.", topic)
		}
		resource := "//pubsub.googleapis.com/" + topic
		if _, indexed := p.index.Get(resource); !indexed {
			// Recorded "missing-topic".
			return status.Errorf(codes.NotFound, "Topic [%s] not found.", topic)
		}
		agent, ok := p.serviceAgent(ctx, project)
		denied := !ok
		if ok {
			agent.Method, agent.Resource, agent.Project = methodTopicPublish, topic, match[1]
			_, err := p.engine.Authorize(agent, iam.Target{Name: resource}, nil)
			denied = err != nil
		}
		if denied {
			// Recorded "topics-without-publisher".
			member := strings.TrimPrefix(agent.Principal.Subject, "serviceAccount:")
			return status.Errorf(codes.FailedPrecondition, "Permission 'pubsub.topics.publish' denied for %s for pubsub topic: %s "+
				"or the topic doesn't exist. Grant the 'pubsub.topic.publish' permission (or 'roles/pubsub.publisher') on this topic to %s.",
				member, topic, member)
		}
	}
	return nil
}

// serviceAgent is the project's Secret Manager service agent, named by
// project number as GCP names it.
func (p *Provider) serviceAgent(ctx context.Context, project string) (gateway.Call, bool) {
	resource, ok := p.index.Get(provider.ProjectResource(project))
	if !ok || resource.Number == "" {
		return gateway.Call{}, false
	}
	return gateway.Call{Context: ctx, Project: project, Principal: gateway.Principal{
		State: gateway.Authenticated, Subject: "serviceAccount:service-" + resource.Number + "@gcp-sa-secretmanager.iam.gserviceaccount.com",
		Project: project, Scopes: []string{identity.CloudPlatform},
	}}, true
}

// event is one notification rendered under the scope lock.
type event struct {
	topics     []string
	attributes map[string]string
	data       []byte
}

// notify queues one notification for the secret's topics; version is ""
// for a secret event.
func (s *scopeState) notify(project string, record *secretRecord, kind string, document map[string]any, version string, now time.Time) {
	if len(record.Topics) == 0 {
		return
	}
	data, _ := json.Marshal(document) //nolint:errchkjson // plain wire documents
	attributes := map[string]string{
		"eventType": kind, "dataFormat": "JSON_API_V1", "secretId": secretWireName(project, record.Name),
		"timestamp": provider.Stamp(now),
	}
	if version != "" {
		attributes["versionId"] = versionWireName(project, record.Name, version)
	}
	s.outbox = append(s.outbox, event{topics: slices.Clone(record.Topics), attributes: attributes, data: data})
}

// deleted queues the SECRET_DELETE notification; how is REQUESTED or EXPIRATION.
func (s *scopeState) deleted(project string, record *secretRecord, how string, now time.Time) {
	before := len(s.outbox)
	s.notify(project, record, eventDelete, secretDocument(project, record), "", now)
	if len(s.outbox) > before {
		s.outbox[before].attributes["deleteType"] = how
	}
}

// configured queues the TOPIC_CONFIGURED test message GCP sends each newly
// configured topic: no data and no attribute but eventType (recorded
// "notifications").
func (s *scopeState) configured(topics []string) {
	if len(topics) > 0 {
		s.outbox = append(s.outbox, event{topics: slices.Clone(topics), attributes: map[string]string{"eventType": eventConfigured}})
	}
}

// drain takes the queued notifications.
func (s *scopeState) drain() []event {
	events := s.outbox
	s.outbox = nil
	return events
}

// publish delivers notifications as the service agent, outside any scope
// lock and past the request's cancellation: GCP publishes after the
// mutation, whoever waits. Delivery is best effort, as in
// GCP: a failed publish changes nothing about the secret.
func (p *Provider) publish(ctx context.Context, project string, events []event) {
	if len(events) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	agent, ok := p.serviceAgent(ctx, project)
	if !ok {
		return
	}
	for _, notification := range events {
		for _, topic := range notification.topics {
			call := agent
			call.Method, call.Resource = publishMethod, topic
			if match := topicName.FindStringSubmatch(topic); match != nil {
				call.Project = match[1]
			}
			request := &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{
				{Data: notification.data, Attributes: maps.Clone(notification.attributes)},
			}}
			_, _ = p.registry.Invoke(call, request)
		}
	}
}

// launch (re)starts the secret's worker when it has a pending time.
func (p *Provider) launch(scope state.Scope, record *secretRecord) {
	p.stop(scope, record.Name)
	if dueAt(record).IsZero() {
		return
	}
	ctx, cancel := context.WithCancel(p.lifetime)
	p.mu.Lock()
	p.workers[provider.WorkerKey(scope, record.Name)] = cancel
	p.mu.Unlock()
	go p.work(ctx, scope, record.Name)
}

// stop cancels the secret's worker, if any.
func (p *Provider) stop(scope state.Scope, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cancel := p.workers[provider.WorkerKey(scope, id)]; cancel != nil {
		cancel()
		delete(p.workers, provider.WorkerKey(scope, id))
	}
}

// stopScope cancels every worker of a scope, for a reset.
func (p *Provider) stopScope(scope state.Scope) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prefix := provider.WorkerKey(scope, "")
	for key, cancel := range p.workers {
		if strings.HasPrefix(key, prefix) {
			cancel()
			delete(p.workers, key)
		}
	}
}

// work waits for the secret's next pending time and acts on it, until none
// is left or the worker is cancelled.
func (p *Provider) work(ctx context.Context, scope state.Scope, id string) {
	for {
		wait, events, more := p.tick(scope, id)
		p.publish(ctx, scope.Project, events)
		if !more {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.after(wait):
		}
	}
}

// dueAt is the secret's earliest pending time, zero when none.
func dueAt(record *secretRecord) time.Time {
	var due time.Time
	consider := func(moment time.Time) {
		if !moment.IsZero() && (due.IsZero() || moment.Before(due)) {
			due = moment
		}
	}
	consider(record.Next)
	consider(record.Expire)
	for _, version := range record.Versions {
		consider(version.ScheduledDestroy)
	}
	return due
}

// tick applies every pending action that is due under the scope lock and
// returns the wait until the next one, the notifications to publish, and
// whether the worker continues.
func (p *Provider) tick(scope state.Scope, id string) (time.Duration, []event, bool) {
	var (
		wait   time.Duration
		events []event
		more   bool
	)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.secrets[id]
		if record == nil {
			return nil
		}
		now, project := p.clock(), p.wireProject(scope.Project)
		s.destroyDue(project, record, now)
		s.rotateDue(project, record, now)
		if !record.Expire.IsZero() && !now.Before(record.Expire) {
			s.deleted(project, record, "EXPIRATION", now)
			if p.index.Remove(serviceName, scope, secretResource(scope.Project, id)) == nil {
				delete(s.secrets, id)
			}
			events = s.drain()
			return nil
		}
		events = s.drain()
		if due := dueAt(record); !due.IsZero() {
			wait, more = due.Sub(now), true
		}
		return nil
	})
	return wait, events, more
}

// destroyDue completes the delayed destructions whose time has come.
func (s *scopeState) destroyDue(project string, record *secretRecord, now time.Time) {
	for _, version := range sortedVersions(record) {
		if version.ScheduledDestroy.IsZero() || now.Before(version.ScheduledDestroy) {
			continue
		}
		version.State, version.Payload, version.DestroyedAt = stateDestroyed, nil, version.ScheduledDestroy
		version.ScheduledDestroy = time.Time{}
		version.Revision++
		s.notify(project, record, eventVersionDestroy, versionDocument(project, record, version), version.Number, now)
	}
}

// rotateDue sends one SECRET_ROTATE once nextRotationTime has come, then
// moves the time past now: GCP advances nextRotationTime by rotationPeriod,
// and without a period the schedule ends. Nothing changes the value.
func (s *scopeState) rotateDue(project string, record *secretRecord, now time.Time) {
	if record.Next.IsZero() || now.Before(record.Next) {
		return
	}
	for record.Period > 0 && !now.Before(record.Next) {
		record.Next = record.Next.Add(record.Period)
	}
	if record.Period == 0 {
		record.Next = time.Time{}
	}
	record.Revision++
	// Recorded "notifications": the data is the secret after the schedule moved.
	s.notify(project, record, eventRotate, secretDocument(project, record), "", now)
}

// Wire renderings of the lifecycle settings.

// replicationDocument renders automatic or user-managed replication; status
// is the version's replicationStatus form, which has the same shape here.
func replicationDocument(replicas []string) map[string]any {
	if replicas == nil {
		return map[string]any{"automatic": map[string]any{}}
	}
	listed := make([]any, 0, len(replicas))
	for _, location := range replicas {
		listed = append(listed, map[string]any{"location": location})
	}
	return map[string]any{"userManaged": map[string]any{"replicas": listed}}
}

// addPolicies renders the set lifecycle settings into a secret document.
func addPolicies(document map[string]any, settings policies) {
	if len(settings.Topics) > 0 {
		topics := make([]any, 0, len(settings.Topics))
		for _, topic := range settings.Topics {
			topics = append(topics, map[string]any{keyName: topic})
		}
		document["topics"] = topics
	}
	if !settings.Expire.IsZero() {
		document["expireTime"] = protoStamp(settings.Expire)
	}
	if !settings.Next.IsZero() {
		rotation := map[string]any{"nextRotationTime": protoStamp(settings.Next)}
		if settings.Period > 0 {
			rotation["rotationPeriod"] = protoDuration(settings.Period)
		}
		document["rotation"] = rotation
	}
	if len(settings.Aliases) > 0 {
		aliases := map[string]any{}
		for alias, number := range settings.Aliases {
			aliases[alias] = strconv.FormatInt(number, 10)
		}
		document["versionAliases"] = aliases
	}
	if settings.DestroyTTL > 0 {
		document["versionDestroyTtl"] = protoDuration(settings.DestroyTTL)
	}
}

// protoStamp renders a timestamp as proto3 JSON does (0, 3, 6, or 9 digits),
// so a requested time reads back as sent.
func protoStamp(moment time.Time) string {
	encoded, err := protojson.Marshal(timestamppb.New(moment))
	if err != nil {
		return provider.Stamp(moment)
	}
	return strings.Trim(string(encoded), `"`)
}

// protoDuration renders a duration as proto3 JSON does ("3600s").
func protoDuration(duration time.Duration) string {
	encoded, err := protojson.Marshal(durationpb.New(duration))
	if err != nil {
		return fmt.Sprintf("%ds", int64(duration.Seconds()))
	}
	return strings.Trim(string(encoded), `"`)
}
