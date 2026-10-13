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

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/bucketconfig"
	"github.com/evanmarshall/gcp-local/internal/state"
)

func (p *Provider) patchBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, name string) (*gateway.RESTResponse, error) {
	patch, err := decodeBucketPatch(r.JSON)
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

// decodeBucketPatch reads a bucket patch, which may change labels and the
// soft-delete retention only.
func decodeBucketPatch(body []byte) (map[string]json.RawMessage, error) {
	var patch map[string]json.RawMessage
	if json.Unmarshal(body, &patch) != nil || patch == nil {
		return nil, provider.Invalid("Expected a bucket patch")
	}
	for key := range patch {
		if !slices.Contains(patchedBucketFields, key) {
			return nil, gap("bucket updates of " + key)
		}
	}
	return patch, nil
}

// patchedBucketFields are the bucket fields a patch may change.
var patchedBucketFields = []string{"labels", "softDeletePolicy", "versioning", "retentionPolicy", keyDefaultHold}

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
