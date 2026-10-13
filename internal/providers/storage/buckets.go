package storage

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	bucket, found, err := p.catalog.Bucket(scope, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return notFound(name, ""), nil
	}
	var metadata map[string]any
	_ = json.Unmarshal([]byte(bucket.Metadata), &metadata)
	if refused, err := applyBucketPatch(metadata, patch); refused != nil || err != nil {
		return refused, err
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

// applyBucketPatch applies the patched fields to the bucket's metadata.
func applyBucketPatch(metadata map[string]any, patch map[string]json.RawMessage) (*gateway.RESTResponse, error) {
	if raw, ok := patch["labels"]; ok {
		if err := patchLabels(metadata, raw); err != nil {
			return nil, err
		}
	}
	if raw, ok := patch["softDeletePolicy"]; ok {
		return patchSoftDelete(metadata, raw)
	}
	return nil, nil //nolint:nilnil // a nil response with a nil error means the patch applied
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
		if key != "labels" && key != "softDeletePolicy" {
			return nil, gap("bucket updates currently support labels and softDeletePolicy only")
		}
	}
	return patch, nil
}

// patchSoftDelete sets a new soft-delete retention, which takes effect now
// and applies to objects deleted from then on; already soft-deleted objects
// keep their hard-delete time (storage/docs/soft-delete). A disabled policy
// reports only its zero duration (recorded: storage-gcloud-bucket-defaults).
func patchSoftDelete(metadata map[string]any, raw json.RawMessage) (*gateway.RESTResponse, error) {
	var policy struct {
		Seconds json.Number `json:"retentionDurationSeconds"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || policy.Seconds == "" {
		return nil, gap("softDeletePolicy updates set retentionDurationSeconds only")
	}
	seconds, refused := softDeleteRetention(policy.Seconds)
	if refused != nil {
		return refused, nil
	}
	metadata["softDeletePolicy"] = softDeletePolicy(seconds, storageTime(time.Now()))
	return nil, nil //nolint:nilnil // a nil response with a nil error means the patch applied
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
