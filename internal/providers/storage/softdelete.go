package storage

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Soft delete (storage/docs/soft-delete). A bucket with a retention duration
// keeps every object generation that a delete, overwrite, rewrite, or restore
// removes. Soft-deleted objects are hidden from reads and listings, listed with
// softDeleted=true, read by generation with softDeleted=true, and restored as a
// new live generation; the soft-deleted copy stays until its hard-delete time
// (recorded: storage-bucket-defaults). The provider owns them because the
// backend has no soft delete; their bytes stay in memory until the hard-delete
// time passes or the bucket is deleted.

// softObject is one soft-deleted object generation.
type softObject struct {
	Bucket, Name, Generation string
	// Object is the object resource at deletion, with softDeleteTime and
	// hardDeleteTime set.
	Object map[string]any
	Data   []byte
	Hard   time.Time
}

func softKey(bucket, name, generation string) string {
	return bucket + "\x00" + name + "\x00" + generation
}

// retainObject records a generation the backend has just removed.
func (c *Catalog) retainObject(scope state.Scope, object softObject) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		if _, ok := value.Buckets[object.Bucket]; !ok {
			return errBucketAbsent
		}
		value.SoftDeleted[softKey(object.Bucket, object.Name, object.Generation)] = object
		return nil
	}); err != nil {
		return fmt.Errorf("retain %s/%s: %w", object.Bucket, object.Name, err)
	}
	return nil
}

// softDeletedObjects returns a bucket's unexpired soft-deleted objects, ordered
// by name and generation, and drops every expired one.
func (c *Catalog) softDeletedObjects(scope state.Scope, bucket string, now time.Time) []softObject {
	c.mu.Lock()
	defer c.mu.Unlock()
	var items []softObject
	_ = c.store.With(scope, func(value *projectState) error {
		maps.DeleteFunc(value.SoftDeleted, func(_ string, object softObject) bool { return !now.Before(object.Hard) })
		for _, object := range value.SoftDeleted {
			if object.Bucket == bucket {
				items = append(items, object)
			}
		}
		return nil
	})
	slices.SortFunc(items, func(a, b softObject) int {
		if order := cmp.Compare(a.Name, b.Name); order != 0 {
			return order
		}
		left, _ := strconv.ParseInt(a.Generation, 10, 64)
		right, _ := strconv.ParseInt(b.Generation, 10, 64)
		return cmp.Compare(left, right)
	})
	return items
}

// softDeletedObject returns one unexpired soft-deleted generation.
func (c *Catalog) softDeletedObject(scope state.Scope, bucket, name, generation string, now time.Time) (softObject, bool) {
	for _, object := range c.softDeletedObjects(scope, bucket, now) {
		if object.Name == name && object.Generation == generation {
			return object, true
		}
	}
	return softObject{}, false
}

// bucketSettings reads the provider-owned soft-delete retention and public
// access prevention of one bucket.
func (p *Provider) bucketSettings(scope state.Scope, bucket string) (int64, string) {
	stored, found, err := p.catalog.Bucket(scope, bucket)
	if err != nil || !found {
		return 0, ""
	}
	var metadata struct {
		SoftDelete struct {
			Seconds json.Number `json:"retentionDurationSeconds"`
		} `json:"softDeletePolicy"`
		IAM struct {
			Public string `json:"publicAccessPrevention"`
		} `json:"iamConfiguration"`
	}
	_ = json.Unmarshal([]byte(stored.Metadata), &metadata)
	seconds, _ := strconv.ParseInt(string(metadata.SoftDelete.Seconds), 10, 64)
	return seconds, metadata.IAM.Public
}

// retainLive snapshots the live object a mutation may remove when the bucket
// keeps soft-deleted objects. The returned keep records the snapshot once the
// mutation has succeeded; it does nothing for a failed mutation, a bucket
// without a policy, or an object that did not exist.
func (p *Provider) retainLive(ctx context.Context, b *memoryBackend, scope state.Scope, bucket, name string) (func(*gateway.RESTResponse) error, error) {
	skip := func(*gateway.RESTResponse) error { return nil }
	retention, _ := p.bucketSettings(scope, bucket)
	if retention == 0 {
		return skip, nil
	}
	current, missing, err := currentObject(ctx, b, bucket, name)
	if err != nil {
		return nil, err
	}
	if missing != nil {
		return skip, nil
	}
	generation, _ := current[keyGeneration].(string)
	data, err := readSource(ctx, b, bucket, name, generation)
	if err != nil {
		return nil, err
	}
	return func(response *gateway.RESTResponse) error {
		// An unset status is the gateway's 200.
		if response == nil || (response.Status != 0 && (response.Status < 200 || response.Status >= 300)) {
			return nil
		}
		now := time.Now()
		hard := now.Add(time.Duration(retention) * time.Second)
		current["softDeleteTime"], current["hardDeleteTime"] = storageTime(now), storageTime(hard)
		return p.catalog.retainObject(scope, softObject{Bucket: bucket, Name: name, Generation: generation,
			Object: current, Data: data, Hard: hard})
	}, nil
}

// softDeletedOptions are the objects.list options served with
// softDeleted=true; any other documented option is a gap.
var softDeletedOptions = []string{querySoftDeleted, queryPrefix, queryMaxResults, "projection", queryVersions, "prettyPrint", "fields", queryAlt}

// listSoftDeleted serves objects.list with softDeleted=true. A bucket without
// a policy refuses it (recorded: storage-gcloud-bucket-defaults
// "soft-deleted-list-without-policy").
func (p *Provider) listSoftDeleted(req request) (*gateway.RESTResponse, error) {
	limit, refused, err := softDeletedLimit(req.rest.Query)
	if refused != nil || err != nil {
		return refused, err
	}
	if retention, _ := p.bucketSettings(req.scope, req.bucket); retention == 0 {
		return wireError(400, "invalid", "Soft delete policy is required to list soft-deleted versions"), nil
	}
	prefix := req.rest.Query.Get(queryPrefix)
	items := []any{}
	for _, object := range p.catalog.softDeletedObjects(req.scope, req.bucket, time.Now()) {
		if strings.HasPrefix(object.Name, prefix) {
			items = append(items, maps.Clone(object.Object))
		}
	}
	if len(items) > limit {
		return nil, gap("soft-deleted listings longer than one page")
	}
	body := map[string]any{keyKind: "storage#objects"}
	if len(items) > 0 {
		body["items"] = items
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// softDeletedLimit checks a soft-deleted listing's options and returns its
// page size: maxResults, at most 1000 (objects/list).
func softDeletedLimit(query url.Values) (int, *gateway.RESTResponse, error) {
	for key := range query {
		if !slices.Contains(softDeletedOptions, key) {
			return 0, nil, gap("softDeleted listing with " + key)
		}
	}
	if versions, _ := booleanOption(query, queryVersions); versions {
		return 0, nil, gap("softDeleted listing with versions")
	}
	text := query.Get(queryMaxResults)
	if text == "" {
		return maxListResults, nil, nil
	}
	if parsed, ok := positiveInt(text); ok {
		return min(parsed, maxListResults), nil, nil
	}
	return 0, wireError(400, "invalid", "Invalid maxResults"), nil
}

// maxListResults is the largest objects.list page.
const maxListResults = 1000

// positiveInt parses a positive decimal integer.
func positiveInt(text string) (int, bool) {
	value, err := strconv.Atoi(text)
	return value, err == nil && value > 0
}

// getSoftDeleted serves objects.get with softDeleted=true, which names one
// generation (objects/get: "generation must be set" with softDeleted).
func (p *Provider) getSoftDeleted(req request) (*gateway.RESTResponse, error) {
	query := req.rest.Query
	for _, key := range []string{queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch} {
		if query.Has(key) {
			return nil, gap("softDeleted read with " + key)
		}
	}
	if query.Get(queryAlt) == altMedia {
		return nil, gap("soft-deleted object media")
	}
	object, refused := p.softDeletedGeneration(req, wireError(400, "invalid", "A generation is required to read a soft-deleted object"))
	if refused != nil {
		return refused, nil
	}
	return &gateway.RESTResponse{JSON: maps.Clone(object.Object)}, nil
}

// softDeletedGeneration finds the soft-deleted generation a request names,
// or returns missing when it names none and notFound when it is absent.
func (p *Provider) softDeletedGeneration(req request, missing *gateway.RESTResponse) (softObject, *gateway.RESTResponse) {
	generation := req.rest.Query.Get(keyGeneration)
	if generation == "" {
		return softObject{}, missing
	}
	object, found := p.catalog.softDeletedObject(req.scope, req.bucket, req.object, generation, time.Now())
	if !found {
		return softObject{}, notFound(req.bucket, req.object)
	}
	return object, nil
}

// restoreObject serves objects.restore: it writes the soft-deleted generation
// as a new live generation, soft-deleting any live object it replaces, and
// keeps the soft-deleted copy (recorded: storage-bucket-defaults "restore",
// "soft-deleted-after-restore").
func (p *Provider) restoreObject(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	if retention, _ := p.bucketSettings(req.scope, req.bucket); retention == 0 {
		return nil, gap("restore in a bucket without a soft-delete policy")
	}
	object, refused := p.softDeletedGeneration(req, wireError(400, "required", "Required parameter: generation"))
	if refused != nil {
		return refused, nil
	}
	if response, err := preconditions(req.rest, current, req.call.Method); response != nil || err != nil {
		return response, err
	}
	keep, err := p.retainLive(ctx, b, req.scope, req.bucket, req.object)
	if err != nil {
		return nil, err
	}
	metadata, err := copiedMetadata(object.Object)
	if err != nil {
		return nil, err
	}
	upload, err := multipartUpload(req.object, metadata, object.Data)
	if err != nil {
		return nil, err
	}
	stored, err := b.forward(ctx, http.MethodPost, "/upload/storage/v1/b/"+url.PathEscape(req.bucket)+"/o", upload.query, upload.headers, upload.body)
	if err != nil {
		return nil, err
	}
	return stored, keep(stored)
}
