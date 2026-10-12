package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
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
