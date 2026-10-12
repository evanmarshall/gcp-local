package storage

import (
	"cmp"
	"context"
	"crypto/md5" //nolint:gosec // GCS reports and checks MD5 content hashes
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/xmlapi"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// patchedObjectFields are the object fields a patch may set: the ones the Go
// client's ObjectHandle.Update sends for ordinary metadata
// (storage/docs/json_api/v1/objects/patch). Every other writable field (acl,
// customTime, holds, retention, contexts, storageClass) is a gap.
var patchedObjectFields = map[string]bool{
	keyContentType: true, keyCacheControl: true, keyDisposition: true,
	"contentEncoding": true, keyLanguage: true, keyMetadata: true,
}

// objectPatch is one decoded objects.patch body. strings holds the string
// fields to set; metadata is the custom metadata change, where a nil value
// removes that key, and clearMetadata removes every key first.
type objectPatch struct {
	strings       map[string]string
	metadata      map[string]*string
	setMetadata   bool
	clearMetadata bool
}

// decodeObjectPatch reads an objects.patch body and refuses each field and
// value the emulator does not apply.
func decodeObjectPatch(body []byte, target map[string]string) (objectPatch, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return objectPatch{}, provider.Invalid("Expected an object patch")
	}
	patch := &objectPatch{strings: map[string]string{}}
	for key, raw := range fields {
		if err := patch.decodeField(key, raw, target); err != nil {
			return objectPatch{}, err
		}
	}
	return *patch, nil
}

// decodeField records one patch field.
func (patch *objectPatch) decodeField(key string, raw json.RawMessage, target map[string]string) error {
	// The Go client sends the bucket in every patch body. The bucket and
	// name identify the target and must agree with the path.
	if identity, ok := target[key]; ok {
		var value string
		if json.Unmarshal(raw, &value) != nil || value != identity {
			return provider.Invalid("Object " + key + " does not match the patch target")
		}
		return nil
	}
	if !patchedObjectFields[key] {
		return gap("unsupported object patch field " + key)
	}
	if key == keyMetadata {
		// null clears every key, and a null value removes one key.
		if json.Unmarshal(raw, &patch.metadata) != nil {
			return provider.Invalid("metadata values must be strings or null")
		}
		patch.setMetadata, patch.clearMetadata = true, patch.metadata == nil
		return nil
	}
	var value *string
	if json.Unmarshal(raw, &value) != nil {
		return provider.Invalid(key + " must be a string")
	}
	// The backend keeps a field it is sent empty, so clearing one (null,
	// which the Go client sends for "") is not applied here.
	if value == nil || *value == "" {
		return gap("clearing object field " + key)
	}
	patch.strings[key] = *value
	return nil
}

// mergedMetadata applies the patch to an object's current custom metadata.
func (patch *objectPatch) mergedMetadata(current map[string]any) map[string]string {
	merged := map[string]string{}
	if existing, ok := current[keyMetadata].(map[string]any); ok && !patch.clearMetadata {
		for key, value := range existing {
			if text, ok := value.(string); ok {
				merged[key] = text
			}
		}
	}
	for key, value := range patch.metadata {
		if value == nil {
			delete(merged, key)
		} else {
			merged[key] = *value
		}
	}
	return merged
}

// patchObject updates one object's metadata in place. It runs after complete
// authorization: preconditions and the generation are checked against the
// object read under the backend gate, the backend writes, and only then does
// the metageneration advance.
func patchObject(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	patch, err := decodeObjectPatch(req.rest.JSON, map[string]string{keyBucket: req.bucket, keyName: req.object})
	if err != nil {
		return nil, err
	}
	current, refused, err := currentObject(ctx, b, req.bucket, req.object)
	if refused != nil || err != nil {
		return refused, err
	}
	if response, err := preconditions(req.rest, current, MethodObjectsPatch); response != nil || err != nil {
		return response, err
	}
	generation, _ := current[keyGeneration].(string)
	if wanted := req.rest.Query.Get(keyGeneration); wanted != "" && wanted != generation {
		return notFound(req.bucket, req.object), nil
	}
	if response, err := writeObjectPatch(ctx, b, req, patch, current); response != nil || err != nil {
		return response, err
	}
	b.advanceMetageneration(req.bucket, req.object, generation)
	return b.forward(ctx, http.MethodGet, objectPath(req.bucket, req.object), nil, nil, nil)
}

// writeObjectPatch sends a patch to the backend. Upstream's PATCH sets the
// string fields; its PUT replaces the custom metadata, which is how a key is
// removed. Manager.Use serializes the scope, so no request sees one write
// without the other. A non-200 backend answer is returned as is.
func writeObjectPatch(ctx context.Context, b *memoryBackend, req request, patch objectPatch, current map[string]any) (*gateway.RESTResponse, error) {
	type backendWrite struct {
		verb string
		body any
	}
	var writes []backendWrite
	if len(patch.strings) > 0 {
		writes = append(writes, backendWrite{http.MethodPatch, patch.strings})
	}
	if patch.setMetadata {
		writes = append(writes, backendWrite{http.MethodPut, map[string]any{keyMetadata: patch.mergedMetadata(current)}})
	}
	for _, write := range writes {
		payload, err := json.Marshal(write.body)
		if err != nil {
			return nil, provider.Invalid("Unable to encode the object patch")
		}
		response, err := b.forward(ctx, write.verb, objectPath(req.bucket, req.object), nil, http.Header{headerContentType: {"application/json"}}, payload)
		if err != nil || response.Status != http.StatusOK {
			return response, err
		}
	}
	return nil, nil //nolint:nilnil // a nil response with a nil error means every write succeeded
}

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

// xmlQuery lists the XML API query options served; GET also takes
// generation and the response-header overrides.
var xmlQuery = map[string]bool{
	"X-Goog-Algorithm": true, "X-Goog-Credential": true, "X-Goog-Date": true,
	"X-Goog-Expires": true, "X-Goog-SignedHeaders": true, "X-Goog-Signature": true,
}

// xmlGetQuery lists the options only an XML GET takes.
var xmlGetQuery = map[string]bool{keyGeneration: true, "response-content-type": true, "response-content-disposition": true}

// xmlHeaders lists the x-goog- request headers an XML request may carry.
var xmlHeaders = map[string]bool{"x-goog-api-client": true, "x-goog-gcs-idempotency-token": true}

// validateXMLRequest refuses XML API options and headers this provider does
// not implement, so none is silently ignored.
func validateXMLRequest(r *gateway.RESTRequest) error {
	for key, values := range r.Query {
		if !xmlQuery[key] && (!xmlGetQuery[key] || r.Verb != http.MethodGet) {
			return gap("unsupported XML API query option " + key)
		}
		if len(values) != 1 {
			return status.Error(codes.InvalidArgument, "Query options must not repeat")
		}
	}
	for key := range r.Headers {
		if err := validateXMLHeader(r, key); err != nil {
			return err
		}
	}
	if !validObjectName(r.Path["object"]) {
		return status.Error(codes.InvalidArgument, "Invalid object name")
	}
	return nil
}

// validateXMLHeader refuses a conditional or x-goog- header that is not served.
func validateXMLHeader(r *gateway.RESTRequest, key string) error {
	lower := strings.ToLower(key)
	switch {
	case lower == "if-match" || lower == "if-none-match":
		return gap("XML API conditional headers")
	case lower == headerPayloadHash && r.Headers.Get(key) != xmlapi.UnsignedPayload:
		return gap("signed payload hashes; sign with UNSIGNED-PAYLOAD")
	case strings.HasPrefix(lower, "x-goog-meta-") && r.Verb == http.MethodPut:
	case strings.HasPrefix(lower, "x-goog-") && !xmlHeaders[lower] && lower != headerPayloadHash:
		return gap("XML API header " + lower)
	}
	return nil
}

// newXMLRequest resolves an XML API object request. A V4 signature makes its
// signer the principal; otherwise the bearer token's principal applies.
func (p *Provider) newXMLRequest(call gateway.Call, r *gateway.RESTRequest) (request, *gateway.RESTResponse, error) {
	if err := validateXMLRequest(r); err != nil {
		return request{}, nil, err
	}
	bucket := r.Path[keyBucket]
	signed := xmlapi.IsSigned(r.Query)
	if signed {
		if call.Principal.State != gateway.Anonymous {
			return request{}, xmlapi.Error(400, "InvalidArgument", "Authenticate with a signature or an Authorization header, not both.", ""), nil
		}
		principal, refused := xmlapi.VerifySignedURL(r, time.Now(), p.iam.VerifySignature)
		if refused != nil {
			return request{}, refused, nil
		}
		call.Principal = principal
	}
	if _, found := p.index.Get(BucketResource(bucket)); !found {
		if !signed {
			// The XML route matches almost any path. Without a signature or a
			// known bucket, report the missing route as the gateway would.
			return request{}, nil, status.Error(codes.Unimplemented, "No provider implements this REST API method, and no bucket "+bucket+" exists for the XML API")
		}
		return request{}, xmlapi.Error(404, "NoSuchBucket", "The specified bucket does not exist.", ""), nil
	}
	req := request{call: call, rest: r, bucket: bucket, object: r.Path["object"], xml: true}
	req.call.Method = MethodObjectsGet
	if r.Verb == http.MethodPut {
		req.call.Method = MethodObjectsInsert
		upload, refused, err := xmlUpload(r, req.object)
		if refused != nil || err != nil {
			return request{}, refused, err
		}
		req.upload = upload
	}
	return p.resolveRequest(req)
}

// xmlUpload reads an XML PUT body and its object headers.
func xmlUpload(r *gateway.RESTRequest, name string) (*uploadRequest, *gateway.RESTResponse, error) {
	data, err := readChunk(r.Media)
	if err != nil {
		return nil, nil, err
	}
	if want := r.Headers.Get("Content-MD5"); want != "" {
		sum := md5.Sum(data) //nolint:gosec // GCS reports and checks MD5 content hashes
		if want != base64.StdEncoding.EncodeToString(sum[:]) {
			return nil, xmlapi.Error(400, "BadDigest", "The Content-MD5 you specified did not match what we received.", ""), nil
		}
	}
	metadata := map[string]json.RawMessage{}
	for header, field := range map[string]string{headerContentType: keyContentType, "Cache-Control": keyCacheControl,
		"Content-Disposition": keyDisposition, "Content-Language": keyLanguage} {
		if value := r.Headers.Get(header); value != "" {
			metadata[field] = rawString(value)
		}
	}
	custom := map[string]string{}
	for key := range r.Headers {
		if lower := strings.ToLower(key); strings.HasPrefix(lower, "x-goog-meta-") {
			custom[strings.TrimPrefix(lower, "x-goog-meta-")] = r.Headers.Get(key)
		}
	}
	if len(custom) > 0 {
		encoded, err := json.Marshal(custom)
		if err != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "Invalid x-goog-meta- headers")
		}
		metadata[keyMetadata] = encoded
	}
	upload, err := multipartUpload(name, metadata, data)
	return upload, nil, err
}

// executeXML reads an object's media, or writes one and answers with the XML
// API's empty body and object headers.
func executeXML(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	if req.call.Method != MethodObjectsInsert {
		return xmlGet(ctx, b, req)
	}
	response, err := insertObject(ctx, b, req, current)
	if err != nil || response.Status != http.StatusOK {
		return response, err
	}
	object, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	return xmlapi.Stored(object), nil
}

// xmlGet reads an object's media with the response-header overrides applied.
func xmlGet(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	current, refused, err := currentObject(ctx, b, req.bucket, req.object)
	if refused != nil || err != nil {
		return refused, err
	}
	query := url.Values{queryAlt: {altMedia}}
	if generation := req.rest.Query.Get(keyGeneration); generation != "" {
		if current[keyGeneration] != generation {
			return notFound(req.bucket, req.object), nil
		}
		query.Set(keyGeneration, generation)
	}
	response, err := b.forward(ctx, http.MethodGet, objectPath(req.bucket, req.object), query, req.rest.Headers, nil)
	if err != nil || response.Status >= 300 {
		return response, err
	}
	for option, header := range map[string]string{"response-content-type": headerContentType, "response-content-disposition": "Content-Disposition"} {
		if value := req.rest.Query.Get(option); value != "" {
			response.Headers.Set(header, value)
		}
	}
	return response, nil
}

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
