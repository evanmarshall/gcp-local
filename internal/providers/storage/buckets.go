package storage

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/bucketconfig"
	"github.com/evanmarshall/gcp-local/internal/state"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) patchBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, name string) (*gateway.RESTResponse, error) {
	patch, err := decodeBucketPatch(r.JSON, name)
	if err != nil {
		return nil, err
	}
	current, refused, err := p.currentBucket(ctx, b, r, scope, name)
	if refused != nil || err != nil {
		return refused, err
	}
	metadata, refused, err := p.catalogMetadata(scope, name)
	if refused != nil || err != nil {
		return refused, err
	}
	previous := maps.Clone(metadata)
	if err := applyBucketPatch(metadata, patch, name, p.now()); err != nil {
		return nil, err
	}
	// The backend change comes last: every catalog field is validated by now.
	if refused, err := patchVersioning(ctx, b, name, patch["versioning"], current); refused != nil || err != nil {
		return refused, err
	}
	return p.storeBucketPatch(scope, name, previous, metadata, current)
}

// lockRetention serves buckets.lockRetentionPolicy: it locks the bucket's
// unlocked retention policy, irreversibly. ifMetagenerationMatch is required
// and checked first (recorded: storage-bucket-lock "lock-without-precondition",
// "lock-stale-precondition", "lock-again").
func (p *Provider) lockRetention(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, name string) (*gateway.RESTResponse, error) {
	if !r.Query.Has(queryIfMetaMatch) {
		return wireError(400, "required", "Required parameter: "+queryIfMetaMatch), nil
	}
	current, refused, err := p.currentBucket(ctx, b, r, scope, name)
	if refused != nil || err != nil {
		return refused, err
	}
	metadata, refused, err := p.catalogMetadata(scope, name)
	if refused != nil || err != nil {
		return refused, err
	}
	policy, _ := metadata["retentionPolicy"].(map[string]any)
	if locked, _ := policy["isLocked"].(bool); policy == nil || locked {
		return wireError(400, "invalid", "Bucket '"+name+"' does not have an unlocked retention policy."), nil
	}
	policy["isLocked"] = true
	return p.storeBucketPatch(scope, name, metadata, metadata, current)
}

// catalogMetadata returns a copy of a bucket's provider-owned metadata.
func (p *Provider) catalogMetadata(scope state.Scope, name string) (map[string]any, *gateway.RESTResponse, error) {
	bucket, found, err := p.catalog.Bucket(scope, name)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, notFound(name, ""), nil
	}
	var metadata map[string]any
	_ = json.Unmarshal([]byte(bucket.Metadata), &metadata)
	return metadata, nil, nil
}

// storeBucketPatch advances the metageneration of patched metadata, stores it,
// and renders the patched bucket from current, the bucket before the patch,
// dropping the fields the patch removed from previous.
func (p *Provider) storeBucketPatch(scope state.Scope, name string, previous, metadata, current map[string]any) (*gateway.RESTResponse, error) {
	for key := range previous {
		if _, kept := metadata[key]; !kept {
			delete(current, key)
		}
	}
	nextMetageneration(metadata, current, name)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, provider.Invalid("Unable to encode the bucket update")
	}
	if err := p.catalog.UpdateMetadata(scope, name, string(encoded)); err != nil {
		return nil, err
	}
	p.bucketMetadata(scope, current)
	return &gateway.RESTResponse{Status: 200, JSON: current}, nil
}

// applyBucketPatch applies the patched catalog-owned fields to a copy of the
// bucket's metadata, so a refused field leaves the stored metadata unchanged.
func applyBucketPatch(metadata map[string]any, patch map[string]json.RawMessage, name string, now time.Time) error {
	for _, field := range []struct {
		key   string
		apply func(json.RawMessage) error
	}{
		{"labels", func(raw json.RawMessage) error { return patchLabels(metadata, raw) }},
		{"softDeletePolicy", func(raw json.RawMessage) error { return patchSoftDelete(metadata, raw, now) }},
		{"retentionPolicy", func(raw json.RawMessage) error { return patchRetentionPolicy(metadata, raw, name, now) }},
		{keyDefaultHold, func(raw json.RawMessage) error { return patchDefaultHold(metadata, raw) }},
		{"lifecycle", func(raw json.RawMessage) error { return patchLifecycle(metadata, raw) }},
	} {
		if raw, ok := patch[field.key]; ok {
			if err := field.apply(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// patchRetentionPolicy sets, changes, or removes the bucket retention policy.
// A locked policy can't be removed or shortened (recorded:
// storage-bucket-lock "locked-remove", "locked-reduce"); an unchanged or
// longer one keeps its lock and effective time.
func patchRetentionPolicy(metadata map[string]any, raw json.RawMessage, name string, now time.Time) error {
	current, _ := metadata["retentionPolicy"].(map[string]any)
	locked, _ := current["isLocked"].(bool)
	if string(bytes.TrimSpace(raw)) == jsonNull {
		if locked {
			return &bucketconfig.RefusalError{Status: 403, Reason: "retentionPolicyNotMet", Message: "Bucket '" + name + "' has a locked Retention Policy which cannot be removed."}
		}
		delete(metadata, "retentionPolicy")
		return nil
	}
	period, err := bucketconfig.ParseRetentionPeriod(raw)
	if err != nil {
		return err //nolint:wrapcheck // GCP's refusal, rendered as is
	}
	if !locked {
		metadata["retentionPolicy"] = retentionPolicy(period, storageTime(now), false)
		return nil
	}
	previous, _ := strconv.ParseInt(fmt.Sprint(current["retentionPeriod"]), 10, 64)
	if period < previous {
		return &bucketconfig.RefusalError{Status: 403, Reason: "forbidden", Message: "Cannot reduce retention duration of a locked Retention Policy for bucket '" + name + "'."}
	}
	current["retentionPeriod"] = strconv.FormatInt(period, 10)
	return nil
}

// patchLifecycle replaces the bucket's lifecycle rules; null or no rules
// removes them (recorded: storage-lifecycle-rules "rules-cleared").
func patchLifecycle(metadata map[string]any, raw json.RawMessage) error {
	location, _ := metadata["location"].(string)
	project, _ := metadata["projectNumber"].(string)
	rules, err := bucketconfig.ParseLifecycle(raw, location, project)
	if err != nil {
		return err //nolint:wrapcheck // GCP's refusal, rendered as is
	}
	setLifecycle(metadata, rules)
	return nil
}

// patchDefaultHold sets the default event-based hold new objects take.
func patchDefaultHold(metadata map[string]any, raw json.RawMessage) error {
	var hold *bool
	if json.Unmarshal(raw, &hold) != nil || hold == nil {
		return gap(keyDefaultHold + " values other than true and false")
	}
	metadata[keyDefaultHold] = *hold
	return nil
}

// currentBucket reads the bucket a patch applies to, merged with its catalog
// metadata, and checks the patch preconditions against it.
func (p *Provider) currentBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, name string) (map[string]any, *gateway.RESTResponse, error) {
	response, err := b.forward(ctx, "GET", "/storage/v1/b/"+name, nil, nil, nil)
	if err != nil || response.Status != 200 {
		return nil, response, err
	}
	current, err := jsonObject(response.JSON)
	if err != nil {
		return nil, nil, err
	}
	p.bucketMetadata(scope, current)
	if failed, err := preconditions(r, current, MethodBucketsPatch); failed != nil || err != nil {
		return nil, failed, err
	}
	return current, nil, nil
}

// decodeBucketPatch reads a bucket patch of the patchedBucketFields. gcloud
// sends the bucket's name with every update; it must name the bucket.
func decodeBucketPatch(body []byte, bucket string) (map[string]json.RawMessage, error) {
	var patch map[string]json.RawMessage
	if json.Unmarshal(body, &patch) != nil || patch == nil {
		return nil, provider.Invalid("Expected a bucket patch")
	}
	if raw, ok := patch[keyName]; ok {
		var name string
		if json.Unmarshal(raw, &name) != nil || name != bucket {
			return nil, provider.Invalid("Bucket name does not match the patch target")
		}
		delete(patch, keyName)
	}
	for key := range patch {
		if !slices.Contains(patchedBucketFields, key) {
			return nil, gap("bucket updates of " + key)
		}
	}
	return patch, nil
}

// patchedBucketFields are the bucket fields a patch may change.
var patchedBucketFields = []string{"labels", "softDeletePolicy", "versioning", "retentionPolicy", keyDefaultHold, "lifecycle"}

// patchVersioning turns versioning on or off in the backend, which keeps the
// noncurrent generations, when the patch names it (raw is nil otherwise).
// Turning it off keeps the existing ones (recorded: storage-object-versioning
// "versions-after-suspend"). The backend's update sets both fields it models,
// so the backend's default event-based hold, which this provider does not
// read, stays false.
func patchVersioning(ctx context.Context, b *memoryBackend, name string, raw json.RawMessage, current map[string]any) (*gateway.RESTResponse, error) {
	if raw == nil {
		return nil, nil //nolint:nilnil // the patch leaves versioning alone
	}
	var versioning struct {
		Enabled *bool `json:"enabled"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&versioning) != nil || versioning.Enabled == nil {
		return nil, gap("versioning updates set enabled only")
	}
	payload, err := json.Marshal(map[string]any{"versioning": map[string]bool{keyEnabled: *versioning.Enabled}, "defaultEventBasedHold": false})
	if err != nil {
		return nil, provider.Invalid("Unable to encode the versioning update")
	}
	response, err := b.forward(ctx, http.MethodPatch, "/storage/v1/b/"+url.PathEscape(name), nil, http.Header{headerContentType: {contentJSON}}, payload)
	if err != nil || response.Status != http.StatusOK {
		return response, err
	}
	current["versioning"] = map[string]any{keyEnabled: *versioning.Enabled}
	return nil, nil //nolint:nilnil // a nil response with a nil error means the update applied
}

// patchSoftDelete sets a new soft-delete retention, which takes effect now
// and applies to objects deleted from then on; already soft-deleted objects
// keep their hard-delete time (storage/docs/soft-delete). A disabled policy
// reports only its zero duration (recorded: storage-gcloud-bucket-defaults).
func patchSoftDelete(metadata map[string]any, raw json.RawMessage, now time.Time) error {
	var policy struct {
		Seconds json.Number `json:"retentionDurationSeconds"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || policy.Seconds == "" {
		return gap("softDeletePolicy updates set retentionDurationSeconds only")
	}
	seconds, err := softDeleteRetention(policy.Seconds)
	if err != nil {
		return err
	}
	metadata["softDeletePolicy"] = softDeletePolicy(seconds, storageTime(now))
	return nil
}

// patchLabels applies a labels patch: a null value removes that label and a
// null labels object clears them all.
func patchLabels(metadata map[string]any, raw json.RawMessage) error {
	var changes map[string]*string
	if json.Unmarshal(raw, &changes) != nil {
		return provider.Invalid("Labels must be strings or null")
	}
	labels, _ := metadata["labels"].(map[string]any)
	if labels == nil || changes == nil {
		labels = map[string]any{}
	}
	for key, value := range changes {
		if value == nil {
			delete(labels, key)
		} else {
			labels[key] = *value
		}
	}
	metadata["labels"] = labels
	return nil
}

// nextMetageneration advances the bucket metageneration and its derived etag,
// and stamps the update time.
func nextMetageneration(metadata, current map[string]any, name string) {
	generationText, _ := current["metageneration"].(string)
	generation, _ := strconv.ParseUint(generationText, 10, 64)
	next := strconv.FormatUint(generation+1, 10)
	metadata["metageneration"] = next
	metadata["etag"] = objectETag(name, next)
	delete(metadata, "rpo")
	metadata["updated"] = time.Now().UTC().Format(time.RFC3339Nano)
}

// List paging (objects/list, buckets/list, folders/list). A GCP page token
// resumes after the last entry of the page that issued it, so entries added
// or removed between pages are neither repeated nor skipped; offset tokens
// (provider.Page) would shift. Tokens are opaque: the list kind and the last
// entry's name.
const (
	queryPageToken = "pageToken"
	objectPages    = "objects"
	bucketPages    = "buckets"
	folderPages    = "folders"
)

// listToken is the page token that resumes a listing of kind after last.
func listToken(kind, last string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(kind + "\x00" + last))
}

// listAfter decodes a page token of kind into the name to resume after. An
// empty token starts the listing; valid is false for a token no listing of
// kind issued.
func listAfter(kind, token string) (string, bool) {
	if token == "" {
		return "", true
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	after, found := strings.CutPrefix(string(raw), kind+"\x00")
	return after, err == nil && found
}

// listSize parses maxResults or pageSize, an unsigned integer that GCP caps
// at 1000 (recorded: storage-list-paging "max-results-negative",
// "max-results-text", "max-results-above-limit"). Absent is 1000; zero is the
// method's own reading of it.
func listSize(text string, zero int) (int, *gateway.RESTResponse) {
	if text == "" {
		return maxListResults, nil
	}
	value, err := strconv.ParseUint(text, 10, 32)
	if err != nil {
		return 0, wireError(400, "invalidParameter", "Invalid unsigned integer value: '"+text+"'.")
	}
	if value == 0 {
		return zero, nil
	}
	return int(min(value, maxListResults)), nil
}

// pageAfter returns the entries named after after, at most limit of them,
// and the token of the next page, "" on the last. entries are in name order.
func pageAfter[T any](entries []T, name func(T) string, after string, limit int, kind string) ([]T, string) {
	start := 0
	if after != "" {
		start = sort.Search(len(entries), func(i int) bool { return name(entries[i]) > after })
	}
	end := min(start+limit, len(entries))
	if end == start || end == len(entries) {
		return entries[start:end], ""
	}
	return entries[start:end], listToken(kind, name(entries[end-1]))
}

// listBuckets serves buckets.list: the scope's buckets in name order, paged
// here, each merged with its catalog metadata. returnPartialSuccess=true
// lists the same buckets: every bucket is reachable, so GCP's unreachable
// list is empty and omitted (recorded: storage-go-client-projection
// "bucket-list-partial-success").
func (p *Provider) listBuckets(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	// maxResults=0 lists a full page (recorded: storage-list-paging
	// "bucket-max-results-zero").
	limit, refused := listSize(r.Query.Get(queryMaxResults), maxListResults)
	if refused != nil {
		return refused, nil
	}
	query := url.Values{}
	if prefix := r.Query.Get(queryPrefix); prefix != "" {
		query.Set(queryPrefix, prefix)
	}
	response, err := b.forward(ctx, http.MethodGet, "/storage/v1/b", query, nil, nil)
	if err != nil || response.Status != http.StatusOK {
		return response, err
	}
	body, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	buckets := itemList(body["items"])
	slices.SortStableFunc(buckets, func(left, right map[string]any) int {
		return cmp.Compare(documentName(left), documentName(right))
	})
	for _, item := range buckets {
		p.bucketMetadata(scope, item)
	}
	after, valid := listAfter(bucketPages, r.Query.Get(queryPageToken))
	if !valid {
		// GCP lists nothing after a token it did not issue (recorded:
		// storage-list-paging "bucket-invalid-page-token").
		buckets = nil
	}
	page, next := pageAfter(buckets, documentName, after, limit, bucketPages)
	result := provider.ListBody("items", page, next)
	result[keyKind] = "storage#buckets"
	return &gateway.RESTResponse{JSON: result}, nil
}

// lifecycleInterval is how often the lifecycle worker applies the rules. GCP
// applies them asynchronously, typically within a day of an object
// qualifying (storage/docs/lifecycle#behavior); the worker acts within a
// minute of the provider's clock.
const lifecycleInterval = time.Minute

// runLifecycle applies the lifecycle rules every lifecycleInterval until ctx
// ends.
func (p *Provider) runLifecycle(ctx context.Context) {
	ticker := time.NewTicker(lifecycleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.sweepLifecycle(ctx)
		}
	}
}

// sweepLifecycle applies every bucket's lifecycle rules once at the
// provider's clock, under each scope's backend gate. GCP performs lifecycle
// actions itself, as the project's Storage service agent, and requires no
// role for them; they never remove a held or retained generation.
func (p *Provider) sweepLifecycle(ctx context.Context) {
	for scope, buckets := range p.catalog.lifecycleRules() {
		_ = p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: scope}, func(session context.Context, driver backend.Backend) error {
			b, err := backendOf(driver)
			if err != nil {
				return err
			}
			for _, name := range slices.Sorted(maps.Keys(buckets)) {
				if err := p.applyLifecycle(session, b, scope, name, buckets[name]); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

// applyLifecycle applies one bucket's rules to each of its generations.
func (p *Provider) applyLifecycle(ctx context.Context, b *memoryBackend, scope state.Scope, bucket string, rules []bucketconfig.Rule) error {
	if resource, found := p.index.Get(BucketResource(bucket)); !found || resource.Scope != scope {
		return nil
	}
	objects, refused, err := listedObjects(ctx, b, bucket, url.Values{queryVersions: {"true"}})
	if refused != nil || err != nil {
		return err
	}
	versioned, err := b.versioned(ctx, bucket)
	if err != nil {
		return err
	}
	now := p.now()
	for i, object := range objects {
		view := lifecycleView(b, bucket, object, newerVersions(objects, i))
		if action, found := bucketconfig.Apply(rules, view, now); found {
			if err := p.lifecycleAction(ctx, b, scope, bucket, object, action, versioned && view.Noncurrent.IsZero()); err != nil {
				return err
			}
		}
	}
	return nil
}

// lifecycleAction performs one action on one generation at the provider's
// clock. SetStorageClass changes the class in place; Delete skips a held or
// retained generation.
func (p *Provider) lifecycleAction(ctx context.Context, b *memoryBackend, scope state.Scope, bucket string, object map[string]any, action bucketconfig.Action, archive bool) error {
	if action.Type == bucketconfig.ActionSetStorageClass {
		now := p.now()
		b.setOverlay(bucket, documentName(object), fmt.Sprint(object[keyGeneration]), func(overlay *objectOverlay) {
			overlay.storageClass, overlay.classUpdated = action.StorageClass, now
		})
		return nil
	}
	if protected := p.refuseRemoval(b, scope, bucket, object); protected != nil {
		return nil //nolint:nilerr // a protected generation is skipped, as GCP's lifecycle skips it
	}
	return p.lifecycleDelete(ctx, b, scope, bucket, object, archive)
}

// newerVersions counts the generations of objects[i]'s name after it; objects
// are in name and generation order.
func newerVersions(objects []map[string]any, i int) int {
	count := 0
	for _, later := range objects[i+1:] {
		if documentName(later) != documentName(objects[i]) {
			break
		}
		count++
	}
	return count
}

// lifecycleView is what the lifecycle conditions read about one generation.
func lifecycleView(b *memoryBackend, bucket string, object map[string]any, newer int) bucketconfig.Object {
	parse := func(key string) time.Time {
		text, _ := object[key].(string)
		parsed, _ := time.Parse(time.RFC3339Nano, text)
		return parsed
	}
	class, _ := object["storageClass"].(string)
	overlay := b.overlay(bucket, documentName(object), fmt.Sprint(object[keyGeneration]))
	return bucketconfig.Object{Name: documentName(object), StorageClass: class, Created: parse("timeCreated"),
		CustomTime: overlay.customTime, Noncurrent: parse("timeDeleted"), NewerVersions: newer}
}

// lifecycleDelete performs a Delete action: in a versioned bucket the live
// generation becomes noncurrent; otherwise the generation is deleted, and
// soft-deleted when the bucket keeps soft-deleted objects.
func (p *Provider) lifecycleDelete(ctx context.Context, b *memoryBackend, scope state.Scope, bucket string, object map[string]any, archive bool) error {
	name, generation := documentName(object), fmt.Sprint(object[keyGeneration])
	query := url.Values{keyGeneration: {generation}}
	if archive {
		query = nil
	}
	keep, err := p.retainRemoved(ctx, b, scope, bucket, name, generation, !archive)
	if err != nil {
		return err
	}
	response, err := b.forward(ctx, http.MethodDelete, objectPath(bucket, name), query, nil, nil)
	if err != nil {
		return err
	}
	noContentOnSuccess(response)
	if err := keep(response); err != nil {
		return err
	}
	if response.Status == http.StatusNoContent {
		p.notifyRemoval(scope, bucket, object, !archive, nil)
	}
	return b.prune(ctx, bucket, name, generation)
}

// publicationQueue bounds the notification messages waiting for delivery;
// a full queue makes the object change that sends one wait.
const publicationQueue = 1024

// publishMethod is the Pub/Sub RPC notifications are delivered through.
const publishMethod = "/google.pubsub.v1.Publisher/Publish"

// publication is one notification message for the delivery worker.
type publication struct {
	agent      gateway.Call
	topic      string
	data       []byte
	attributes map[string]string
}

// deliver publishes queued notification messages through the Pub/Sub
// provider as the Storage service agent until ctx ends. Pub/Sub authorizes
// the agent for pubsub.topics.publish on each message; a refused or failed
// publish drops that message, and later changes still publish.
func (p *Provider) deliver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-p.publications:
			call := message.agent
			call.Context, call.Method, call.Resource = ctx, publishMethod, message.topic
			_, _ = p.registry.Invoke(call, &pubsubpb.PublishRequest{Topic: message.topic, Messages: []*pubsubpb.PubsubMessage{
				{Data: message.data, Attributes: message.attributes},
			}})
		}
	}
}

// storageAgent is the project's Cloud Storage service agent, named by
// project number as GCP names it (recorded: storage-notifications
// "service-agent").
func (p *Provider) storageAgent(project string) (gateway.Call, string, bool) {
	resource, ok := p.index.Get(provider.ProjectResource(project))
	if !ok || resource.Number == "" {
		return gateway.Call{}, "", false
	}
	email := "service-" + resource.Number + "@gs-project-accounts.iam.gserviceaccount.com"
	return gateway.Call{Project: project, Principal: gateway.Principal{
		State: gateway.Authenticated, Subject: "serviceAccount:" + email, Project: project,
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
	}}, email, true
}

// executeNotification serves the notification configuration methods and the
// project's service account.
func (p *Provider) executeNotification(req request) (*gateway.RESTResponse, error) {
	switch req.call.Method {
	case MethodServiceAccountGet:
		_, email, ok := p.storageAgent(req.call.Project)
		if !ok {
			return nil, gap("the Storage service agent needs the project's number in the resource index")
		}
		return &gateway.RESTResponse{JSON: map[string]any{keyKind: "storage#serviceAccount", "email_address": email}}, nil
	case MethodNotificationsInsert:
		return p.createNotification(req)
	case MethodNotificationsDelete:
		if p.catalog.deleteNotification(req.scope, req.bucket, req.rest.Path["notification"]) {
			return &gateway.RESTResponse{Status: http.StatusNoContent}, nil
		}
		return notificationMissing(), nil
	}
	return p.readNotifications(req), nil
}

// notificationMissing answers a request for a configuration that doesn't
// exist (recorded: storage-notifications "get-missing", "deleted-get").
func notificationMissing() *gateway.RESTResponse {
	return wireError(404, "notFound", "The requested resource was not found.")
}

// readNotifications serves notifications.list and notifications.get.
func (p *Provider) readNotifications(req request) *gateway.RESTResponse {
	configs := p.catalog.notifications(req.scope, req.bucket)
	if req.call.Method == MethodNotificationsGet {
		for _, config := range configs {
			if config.ID == req.rest.Path["notification"] {
				return &gateway.RESTResponse{JSON: notificationDocument(req.bucket, config)}
			}
		}
		return notificationMissing()
	}
	documents := make([]map[string]any, 0, len(configs))
	for _, config := range configs {
		documents = append(documents, notificationDocument(req.bucket, config))
	}
	result := provider.ListBody("items", documents, "")
	result[keyKind] = "storage#notifications"
	return &gateway.RESTResponse{JSON: result}
}

// createNotification serves notifications.insert. GCP refuses a topic that
// doesn't exist, or that the Storage service agent can't publish to, with
// its recorded errors (storage-notifications "create-missing-topic",
// "create-without-grant").
func (p *Provider) createNotification(req request) (*gateway.RESTResponse, error) {
	config, err := bucketconfig.ParseNotification(req.rest.JSON)
	if err != nil {
		return nil, err //nolint:wrapcheck // GCP's refusal, rendered as is
	}
	agent, email, ok := p.storageAgent(req.scope.Project)
	if !ok {
		return nil, gap("the Storage service agent needs the project's number in the resource index")
	}
	if _, found := p.index.Get(config.Topic); !found {
		return wireError(400, "invalid", "Cloud Pub/Sub topic '"+config.Topic+"' not found, or user '"+email+"' does not have permission to it."), nil
	}
	agent.Context, agent.Method, agent.Resource = req.call.Context, methodNotificationPublish, config.Topic
	if _, err := p.iam.Authorize(agent, iam.Target{Name: config.Topic}, nil); err != nil {
		if status.Code(err) != codes.PermissionDenied {
			return nil, gap("authorizing the Storage service agent on the topic: " + status.Convert(err).Message())
		}
		// GCP's message repeats "to" (recorded: "create-without-grant").
		return wireError(403, "forbidden", "The service account '"+email+"' does not have permission to publish messages to "+
			"to the Cloud Pub/Sub topic '"+config.Topic+"', or that topic does not exist."), nil
	}
	stored, err := p.catalog.addNotification(req.scope, req.bucket, notificationConfig{Notification: config, Origin: "http://" + req.rest.Host})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: notificationDocument(req.bucket, stored)}, nil
}

// notificationDocument is the wire shape of one notification configuration
// (notifications resource): optional fields appear only when set (recorded:
// storage-notifications "create-filtered", "create-unfiltered-short-topic").
func notificationDocument(bucket string, config notificationConfig) map[string]any {
	document := map[string]any{
		keyKind: "storage#notification", "id": config.ID, "topic": config.Topic, "payload_format": config.PayloadFormat,
		"etag": config.ID, "selfLink": config.Origin + "/storage/v1/b/" + url.PathEscape(bucket) + "/notificationConfigs/" + config.ID,
	}
	if len(config.EventTypes) > 0 {
		document["event_types"] = slices.Clone(config.EventTypes)
	}
	if len(config.CustomAttributes) > 0 {
		document["custom_attributes"] = maps.Clone(config.CustomAttributes)
	}
	if config.ObjectNamePrefix != "" {
		document["object_name_prefix"] = config.ObjectNamePrefix
	}
	return document
}

// notify queues the messages one object change sends to the bucket's
// matching notification configurations, as GCP sends them (recorded:
// storage-notifications "finalize", "metadata-update", "delete"). object is
// the generation the event names; extra adds the overwrite attributes.
func (p *Provider) notify(scope state.Scope, bucket, event string, object map[string]any, extra map[string]string) {
	configs := p.catalog.notifications(scope, bucket)
	agent, _, ok := p.storageAgent(scope.Project)
	if len(configs) == 0 || object == nil || !ok {
		return
	}
	name, generation := documentName(object), fmt.Sprint(object[keyGeneration])
	eventTime := p.now().UTC().Format(time.RFC3339Nano)
	for _, config := range configs {
		if !config.Matches(event, name) {
			continue
		}
		attributes := map[string]string{
			"notificationConfig": "projects/_/buckets/" + bucket + "/notificationConfigs/" + config.ID,
			"eventType":          event, "payloadFormat": config.PayloadFormat, "bucketId": bucket,
			"objectId": name, "objectGeneration": generation, "eventTime": eventTime,
		}
		maps.Copy(attributes, extra)
		maps.Copy(attributes, config.CustomAttributes)
		var data []byte
		if config.PayloadFormat == bucketconfig.PayloadJSON {
			document := maps.Clone(object)
			publishLinks(document, config.Origin)
			encoded, err := json.Marshal(document)
			if err != nil {
				continue
			}
			data = encoded
		}
		p.publications <- publication{agent: agent, topic: strings.TrimPrefix(config.Topic, bucketconfig.TopicPrefix), data: data, attributes: attributes}
	}
}

// notifyWrite sends the events of a write that created object, replacing
// the live generation replaced (nil when none): OBJECT_FINALIZE, and
// OBJECT_ARCHIVE in a versioned bucket or OBJECT_DELETE otherwise for the
// replaced one, with the overwrite attributes GCP documents
// (storage/docs/pubsub-notifications#attributes).
func (p *Provider) notifyWrite(ctx context.Context, b *memoryBackend, scope state.Scope, bucket string, object, replaced map[string]any) {
	if object == nil || len(p.catalog.notifications(scope, bucket)) == 0 {
		return
	}
	if replaced == nil {
		p.notify(scope, bucket, bucketconfig.EventFinalize, object, nil)
		return
	}
	p.notify(scope, bucket, bucketconfig.EventFinalize, object, map[string]string{"overwroteGeneration": fmt.Sprint(replaced[keyGeneration])})
	versioned, _ := b.versioned(ctx, bucket)
	p.notifyRemoval(scope, bucket, replaced, !versioned, map[string]string{"overwrittenByGeneration": fmt.Sprint(object[keyGeneration])})
}

// notifyRemoval sends OBJECT_DELETE for a generation removed for good, or
// OBJECT_ARCHIVE for a live one a versioned bucket kept as noncurrent.
func (p *Provider) notifyRemoval(scope state.Scope, bucket string, object map[string]any, permanent bool, extra map[string]string) {
	event := bucketconfig.EventDelete
	if !permanent {
		event = bucketconfig.EventArchive
	}
	p.notify(scope, bucket, event, object, extra)
}
