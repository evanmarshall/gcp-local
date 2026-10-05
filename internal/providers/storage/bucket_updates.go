package storage

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func invalid(message string) error { return status.Error(codes.InvalidArgument, message) }
func (p *Provider) patchBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, name string) (*gateway.RESTResponse, error) {
	var patch map[string]json.RawMessage
	if json.Unmarshal(r.JSON, &patch) != nil || patch == nil {
		return nil, invalid("Expected a bucket patch")
	}
	for key := range patch {
		if key != "labels" {
			return nil, gap("bucket updates currently support labels only")
		}
	}
	response, err := b.forward(ctx, "GET", "/storage/v1/b/"+name, nil, nil, nil)
	if err != nil || response.Status != 200 {
		return response, err
	}
	current := response.JSON.(map[string]any)
	p.bucketMetadata(scope, current)
	if failed, err := preconditions(r, current, "storage.buckets.patch"); failed != nil || err != nil {
		return failed, err
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
		var changes map[string]*string
		if json.Unmarshal(raw, &changes) != nil {
			return nil, invalid("Labels must be strings or null")
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
	}
	generation, _ := strconv.ParseUint(current["metageneration"].(string), 10, 64)
	metadata["metageneration"] = strconv.FormatUint(generation+1, 10)
	metadata["etag"] = objectETag(name, metadata["metageneration"].(string))
	metadata["updated"] = time.Now().UTC().Format(time.RFC3339Nano)
	encoded, _ := json.Marshal(metadata)
	if err := p.catalog.UpdateMetadata(scope, name, string(encoded)); err != nil {
		return nil, err
	}
	p.bucketMetadata(scope, current)
	return &gateway.RESTResponse{Status: 200, JSON: current}, nil
}
