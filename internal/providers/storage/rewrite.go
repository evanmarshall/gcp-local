package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// copiedFields are the source object fields a rewrite carries to its
// destination unless the request body replaces them.
var copiedFields = []string{keyContentType, keyMetadata, keyCacheControl, keyDisposition, keyLanguage}

// rewriteRequest is one validated objects.rewrite call.
type rewriteRequest struct {
	call                    gateway.Call
	rest                    *gateway.RESTRequest
	source, destination     iam.Target
	scope                   state.Scope
	sourceBucket, sourceKey string
	bucket, object          string
	metadata                map[string]json.RawMessage
}

// rewrite copies one object in a single call: it reads as the caller with
// storage.objects.get on the source and writes with the insert plan
// (create, plus delete when replacing) on the destination. It never returns
// done=false, so rewriteToken continuation is not needed. Source and
// destination must share a project, because each project has its own backend.
func (p *Provider) rewrite(call gateway.Call, r *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	req, refused, err := p.newRewrite(call, r)
	if refused != nil || err != nil {
		return refused, err
	}
	for _, check := range []struct {
		method string
		target iam.Target
	}{{MethodObjectsGet, req.source}, {MethodObjectsInsert, req.destination}} {
		preflight := req.call
		preflight.Method, preflight.Resource = check.method, check.target.Name
		if denied, _, err := p.authorize(preflight, check.target, nil, false); denied != nil || err != nil {
			return denied, err
		}
	}
	var response *gateway.RESTResponse
	err = p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: req.scope}, func(ctx context.Context, driver backend.Backend) error {
		b, backendErr := backendOf(driver)
		if backendErr != nil {
			return backendErr
		}
		req.call.Context = ctx //nolint:fatcontext // one request-scoped handoff of the manager session
		var serveErr error
		response, serveErr = p.serveRewrite(ctx, b, req)
		return serveErr
	})
	if err != nil {
		return response, fmt.Errorf("rewrite the object: %w", err)
	}
	return response, nil
}

// newRewrite validates a rewrite and resolves both targets.
func (p *Provider) newRewrite(call gateway.Call, r *gateway.RESTRequest) (rewriteRequest, *gateway.RESTResponse, error) {
	if err := validateQuery(MethodObjectsRewrite, r); err != nil {
		return rewriteRequest{}, nil, err
	}
	req := rewriteRequest{call: call, rest: r, sourceBucket: r.Path["sourceBucket"], sourceKey: r.Path["sourceObject"],
		bucket: r.Path["destinationBucket"], object: r.Path["destinationObject"]}
	metadata, err := rewriteMetadata(r.JSON, req.bucket, req.object)
	if err != nil {
		return rewriteRequest{}, nil, err
	}
	req.metadata = metadata
	source, sourceScope, refused, err := p.resolveTarget(gateway.Call{Method: MethodObjectsGet}, req.sourceBucket, req.sourceKey)
	if refused != nil || err != nil {
		return rewriteRequest{}, refused, err
	}
	destination, scope, refused, err := p.resolveTarget(gateway.Call{Method: MethodObjectsInsert}, req.bucket, req.object)
	if refused != nil || err != nil {
		return rewriteRequest{}, refused, err
	}
	if sourceScope != scope {
		return rewriteRequest{}, nil, gap("rewrite between buckets of different projects")
	}
	req.source, req.destination, req.scope = source, destination, scope
	req.call.Location = state.Global
	return req, nil, nil
}

// rewriteMetadata validates the optional destination metadata body with the
// upload rules; its name and bucket must name the destination.
func rewriteMetadata(body []byte, bucket, object string) (map[string]json.RawMessage, error) {
	if !validObjectName(object) {
		return nil, status.Error(codes.InvalidArgument, "Invalid object name")
	}
	metadata := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid object metadata")
	}
	named := &uploadRequest{name: object}
	if err := applyUploadMetadata(&gateway.RESTRequest{Path: map[string]string{keyBucket: bucket}}, metadata, named); err != nil {
		return nil, err
	}
	if named.name != object {
		return nil, status.Error(codes.InvalidArgument, "Object names disagree")
	}
	delete(metadata, keyName)
	delete(metadata, keyBucket)
	return metadata, nil
}

// serveRewrite authorizes both sides under the backend gate, then copies.
func (p *Provider) serveRewrite(ctx context.Context, b *memoryBackend, req rewriteRequest) (*gateway.RESTResponse, error) {
	if missing := p.missingBucket(req.scope, req.sourceBucket, req.bucket); missing != nil {
		return missing, nil
	}
	source, refused, err := p.authorizeSource(ctx, b, req)
	if refused != nil || err != nil {
		return refused, err
	}
	write := req.call
	write.Method, write.Resource = MethodObjectsInsert, req.destination.Name
	features, current, err := p.objectFeatures(ctx, b, write, req.bucket, req.object)
	if err != nil {
		return nil, err
	}
	denied, id, err := p.authorize(write, req.destination, features, true)
	if denied != nil || err != nil {
		return denied, err
	}
	if response, err := preconditions(req.rest, current, MethodObjectsInsert); response != nil || err != nil {
		return response, err
	}
	response, err := p.copyRetaining(ctx, b, req, source)
	finishResponse(response, id, req.rest.Host)
	return response, err
}

// copyRetaining copies the source over the destination, soft-deleting the
// destination generation it replaces when the bucket keeps them.
func (p *Provider) copyRetaining(ctx context.Context, b *memoryBackend, req rewriteRequest, source map[string]any) (*gateway.RESTResponse, error) {
	keep, err := p.retainLive(ctx, b, req.scope, req.bucket, req.object)
	if err != nil {
		return nil, err
	}
	response, err := copyObject(ctx, b, req, source)
	if err != nil {
		return response, err
	}
	return response, keep(response)
}

// missingBucket refuses the first bucket not owned by scope, or returns nil.
func (p *Provider) missingBucket(scope state.Scope, buckets ...string) *gateway.RESTResponse {
	for _, bucket := range buckets {
		if resource, found := p.index.Get(BucketResource(bucket)); !found || resource.Scope != scope {
			return notFound(bucket, "")
		}
	}
	return nil
}

// authorizeSource checks storage.objects.get on the source and reads its
// metadata, honoring sourceGeneration.
func (p *Provider) authorizeSource(ctx context.Context, b *memoryBackend, req rewriteRequest) (map[string]any, *gateway.RESTResponse, error) {
	read := req.call
	read.Method, read.Resource = MethodObjectsGet, req.source.Name
	if denied, _, err := p.authorize(read, req.source, nil, true); denied != nil || err != nil {
		return nil, denied, err
	}
	source, refused, err := currentObject(ctx, b, req.sourceBucket, req.sourceKey)
	if refused != nil || err != nil {
		return nil, refused, err
	}
	if generation := req.rest.Query.Get("sourceGeneration"); generation != "" && source[keyGeneration] != generation {
		return nil, notFound(req.sourceBucket, req.sourceKey), nil
	}
	return source, nil, nil
}

// copyObject writes the source's media and metadata, overlaid with the
// request's metadata, to the destination.
func copyObject(ctx context.Context, b *memoryBackend, req rewriteRequest, source map[string]any) (*gateway.RESTResponse, error) {
	data, err := readSource(ctx, b, req.sourceBucket, req.sourceKey, req.rest.Query.Get("sourceGeneration"))
	if err != nil {
		return nil, err
	}
	metadata, err := copiedMetadata(source)
	if err != nil {
		return nil, err
	}
	maps.Copy(metadata, req.metadata)
	upload, err := multipartUpload(req.object, metadata, data)
	if err != nil {
		return nil, err
	}
	stored, err := b.forward(ctx, http.MethodPost, "/upload/storage/v1/b/"+url.PathEscape(req.bucket)+"/o", upload.query, upload.headers, upload.body)
	if err != nil || stored.Status != http.StatusOK {
		return stored, err
	}
	size := strconv.Itoa(len(data))
	return &gateway.RESTResponse{JSON: map[string]any{
		keyKind: "storage#rewriteResponse", "totalBytesRewritten": size, "objectSize": size, "done": true, "resource": stored.JSON}}, nil
}

// copiedMetadata selects the copiedFields of an object resource as upload
// metadata; rewrite and restore carry them to the new generation.
func copiedMetadata(source map[string]any) (map[string]json.RawMessage, error) {
	metadata := map[string]json.RawMessage{}
	for _, field := range copiedFields {
		if value, ok := source[field]; ok {
			encoded, err := json.Marshal(value)
			if err != nil {
				return nil, status.Error(codes.Internal, "Unable to copy the source metadata")
			}
			metadata[field] = encoded
		}
	}
	return metadata, nil
}

// readSource reads one object's media from the backend.
func readSource(ctx context.Context, b *memoryBackend, bucket, object, generation string) ([]byte, error) {
	query := url.Values{queryAlt: {altMedia}}
	if generation != "" {
		query.Set(keyGeneration, generation)
	}
	response, err := b.forward(ctx, http.MethodGet, objectPath(bucket, object), query, nil, nil)
	if err != nil {
		return nil, err
	}
	if response.Status != http.StatusOK || response.Body == nil {
		return nil, status.Error(codes.Unavailable, "Unable to read the rewrite source")
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read the rewrite source: %w", err)
	}
	return data, nil
}
