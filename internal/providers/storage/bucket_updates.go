package storage

import (
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
	if raw, ok := patch["labels"]; ok {
		if err := patchLabels(metadata, raw); err != nil {
			return nil, err
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

// decodeBucketPatch reads a bucket patch, which may change labels only.
func decodeBucketPatch(body []byte) (map[string]json.RawMessage, error) {
	var patch map[string]json.RawMessage
	if json.Unmarshal(body, &patch) != nil || patch == nil {
		return nil, provider.Invalid("Expected a bucket patch")
	}
	for key := range patch {
		if key != "labels" {
			return nil, gap("bucket updates currently support labels only")
		}
	}
	return patch, nil
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
