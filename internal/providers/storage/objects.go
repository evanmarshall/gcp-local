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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/bucketconfig"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/xmlapi"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// patchedObjectFields are the object fields a patch may set: the ones the Go
// client's ObjectHandle.Update sends for ordinary metadata, holds, and object
// retention (storage/docs/json_api/v1/objects/patch). Every other writable
// field (acl, contexts, storageClass) is a gap.
var patchedObjectFields = map[string]bool{
	keyContentType: true, keyCacheControl: true, keyDisposition: true,
	"contentEncoding": true, keyLanguage: true, keyMetadata: true,
	keyTemporaryHold: true, keyEventBasedHold: true, keyRetention: true, keyCustomTime: true,
}

// overlayFields are the patch fields the provider owns in the overlay.
var overlayFields = map[string]bool{keyRetention: true, keyTemporaryHold: true, keyEventBasedHold: true, keyCustomTime: true}

// objectPatch is one decoded objects.patch body. strings holds the string
// fields to set; metadata is the custom metadata change, where a nil value
// removes that key, and clearMetadata removes every key first. holds maps a
// hold field to its new value; retention is the new object retention when
// setRetention is set, nil to remove it.
type objectPatch struct {
	strings       map[string]string
	metadata      map[string]*string
	setMetadata   bool
	clearMetadata bool
	holds         map[string]bool
	setRetention  bool
	retention     *bucketconfig.Retention
	// customTime is the new customTime; zero leaves it.
	customTime time.Time
}

// decodeObjectPatch reads an objects.patch body and refuses each field and
// value the emulator does not apply.
func decodeObjectPatch(body []byte, target map[string]string, now time.Time) (objectPatch, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return objectPatch{}, provider.Invalid("Expected an object patch")
	}
	patch := &objectPatch{strings: map[string]string{}, holds: map[string]bool{}}
	for key, raw := range fields {
		if err := patch.decodeField(key, raw, target, now); err != nil {
			return objectPatch{}, err
		}
	}
	return *patch, nil
}

// decodeField records one patch field.
func (patch *objectPatch) decodeField(key string, raw json.RawMessage, target map[string]string, now time.Time) error {
	// The Go client sends the bucket in every patch body. The bucket and
	// name identify the target and must agree with the path.
	if identity, ok := target[key]; ok {
		var value string
		if json.Unmarshal(raw, &value) != nil || value != identity {
			return provider.Invalid("Object " + key + " does not match the patch target")
		}
		return nil
	}
	switch {
	case !patchedObjectFields[key]:
		return gap("unsupported object patch field " + key)
	case key == keyMetadata:
		// null clears every key, and a null value removes one key.
		if json.Unmarshal(raw, &patch.metadata) != nil {
			return provider.Invalid("metadata values must be strings or null")
		}
		patch.setMetadata, patch.clearMetadata = true, patch.metadata == nil
		return nil
	case overlayFields[key]:
		return patch.decodeProtection(key, raw, now)
	}
	return patch.decodeString(key, raw)
}

// decodeString records one string field. The backend keeps a field it is
// sent empty, so clearing one (null, which the Go client sends for "") is not
// applied here.
func (patch *objectPatch) decodeString(key string, raw json.RawMessage) error {
	var value *string
	if json.Unmarshal(raw, &value) != nil {
		return provider.Invalid(key + " must be a string")
	}
	if value == nil || *value == "" {
		return gap("clearing object field " + key)
	}
	patch.strings[key] = *value
	return nil
}

// decodeCustomTime records a new customTime. GCP ignores null: customTime
// can't be removed (recorded: storage-lifecycle-rules "custom-time-removed").
func (patch *objectPatch) decodeCustomTime(raw json.RawMessage) error {
	var text *string
	if json.Unmarshal(raw, &text) != nil {
		return provider.Invalid(keyCustomTime + " must be a string")
	}
	if text == nil {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *text)
	if err != nil {
		return gap(keyCustomTime + " that is not an RFC 3339 time")
	}
	patch.customTime = parsed
	return nil
}

// decodeProtection records a hold, object retention, or customTime field.
func (patch *objectPatch) decodeProtection(key string, raw json.RawMessage, now time.Time) error {
	if key == keyCustomTime {
		return patch.decodeCustomTime(raw)
	}
	if key == keyRetention {
		retention, err := bucketconfig.ParseRetention(raw, now)
		patch.setRetention, patch.retention = true, retention
		return err //nolint:wrapcheck // GCP's refusal, rendered as is
	}
	var hold *bool
	if json.Unmarshal(raw, &hold) != nil || hold == nil {
		return gap(key + " values other than true and false")
	}
	patch.holds[key] = *hold
	return nil
}

// apply records the patch's provider-owned fields in a generation's overlay
// at now. Releasing an event-based hold starts a bucket retention period.
func (patch *objectPatch) apply(overlay *objectOverlay, now time.Time) {
	for key, value := range patch.holds {
		held := value
		if key == keyTemporaryHold {
			overlay.temporaryHold = &held
			continue
		}
		if !held && overlay.eventBasedHold != nil && *overlay.eventBasedHold {
			overlay.released = now
		}
		overlay.eventBasedHold = &held
	}
	if patch.setRetention {
		overlay.retention = patch.retention
	}
	if !patch.customTime.IsZero() {
		overlay.customTime = patch.customTime
	}
}

// refuseDecrease refuses moving customTime earlier (recorded:
// storage-lifecycle-rules "custom-time-earlier").
func (patch *objectPatch) refuseDecrease(current time.Time) error {
	if patch.customTime.IsZero() || !patch.customTime.Before(current) {
		return nil
	}
	offset := func(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.999999") + "+00:00" }
	return bucketconfig.Invalid("Custom time cannot be decreased. Previously: " +
		offset(current) + ". Attempting to set: " + offset(patch.customTime) + ".")
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
// object read under the backend gate, the backend writes, and only then do
// the provider-owned fields, the metageneration, and updated change.
func (p *Provider) patchObject(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	// Storage reports updated in milliseconds; rounding up keeps an update at
	// or after the object's creation.
	now := p.now().UTC()
	if rounded := now.Truncate(time.Millisecond); rounded.Before(now) {
		now = rounded.Add(time.Millisecond)
	}
	patch, err := decodeObjectPatch(req.rest.JSON, map[string]string{keyBucket: req.bucket, keyName: req.object}, now)
	if err != nil {
		return nil, err
	}
	current, refused, err := patchTarget(ctx, b, req)
	if refused != nil || err != nil {
		return refused, err
	}
	generation, _ := current[keyGeneration].(string)
	if err := patch.refuseDecrease(b.overlay(req.bucket, req.object, generation).customTime); err != nil {
		return nil, err
	}
	if err := p.changeRetention(b, req, patch, generation); err != nil {
		return nil, err
	}
	if response, err := writeObjectPatch(ctx, b, req, patch, current); response != nil || err != nil {
		return response, err
	}
	b.updateOverlay(req.bucket, req.object, generation, now, func(overlay *objectOverlay) { patch.apply(overlay, now) })
	return p.patched(ctx, b, req)
}

// patched reads the patched object back and sends its OBJECT_METADATA_UPDATE
// notifications.
func (p *Provider) patched(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	response, err := b.forward(ctx, http.MethodGet, objectPath(req.bucket, req.object), nil, nil, nil)
	if err == nil && response.Status == http.StatusOK {
		p.notify(req.scope, req.bucket, bucketconfig.EventMetadataUpdate, createdObject(response), nil)
	}
	return response, err
}

// patchTarget reads the generation a patch names, or the live one, and checks
// the patch's preconditions against it. Patching a noncurrent generation is
// a gap.
func patchTarget(ctx context.Context, b *memoryBackend, req request) (map[string]any, *gateway.RESTResponse, error) {
	current, refused, err := currentObject(ctx, b, req.bucket, req.object, req.rest.Query.Get(keyGeneration))
	if refused != nil || err != nil {
		return nil, refused, err
	}
	if response, err := preconditions(req.rest, current, MethodObjectsPatch); response != nil || err != nil {
		return nil, response, err
	}
	if _, noncurrent := current["timeDeleted"]; noncurrent {
		return nil, nil, gap("patching a noncurrent object generation")
	}
	return current, nil, nil
}

// changeRetention checks a patch's object retention change against the
// generation's current retention. Setting one needs a bucket created with
// object retention; removing one an object lacks changes nothing.
func (p *Provider) changeRetention(b *memoryBackend, req request, patch objectPatch, generation string) error {
	if !patch.setRetention {
		return nil
	}
	if patch.retention != nil && !p.bucketSettings(req.scope, req.bucket).objectRetention {
		return gap("object retention in a bucket created without enableObjectRetention")
	}
	override, _ := booleanOption(req.rest.Query, queryOverrideRetention)
	return bucketconfig.ChangeRetention(b.overlay(req.bucket, req.object, generation).retention, patch.retention, override) //nolint:wrapcheck // GCP's refusal, rendered as is
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
		response, err := b.forward(ctx, write.verb, objectPath(req.bucket, req.object), nil, http.Header{headerContentType: {contentJSON}}, payload)
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
	id, current, refused, err := p.authorizeDestination(ctx, b, req)
	if refused != nil || err != nil {
		return refused, err
	}
	if response, err := p.replaceable(b, req.scope, req.bucket, req.rest, current); response != nil || err != nil {
		return response, err
	}
	response, err := p.copyRetaining(ctx, b, req, source)
	if err == nil {
		p.created(b, req.scope, req.bucket, createdObject(response))
		p.notifyWrite(ctx, b, req.scope, req.bucket, createdObject(response), current)
		p.decorate(b, req.scope, req.bucket, response)
	}
	finishResponse(response, id, req.rest.Host)
	return response, err
}

// authorizeDestination authorizes the destination write with the insert plan
// and returns the decision ID and the destination object it replaces.
func (p *Provider) authorizeDestination(ctx context.Context, b *memoryBackend, req rewriteRequest) (string, map[string]any, *gateway.RESTResponse, error) {
	write := req.call
	write.Method, write.Resource = MethodObjectsInsert, req.destination.Name
	features, current, err := p.objectFeatures(ctx, b, write, req.bucket, req.object)
	if err != nil {
		return "", nil, nil, err
	}
	denied, id, err := p.authorize(write, req.destination, features, true)
	return id, current, denied, err
}

// copyRetaining copies the source over the destination, soft-deleting the
// destination generation it replaces when the bucket keeps them.
func (p *Provider) copyRetaining(ctx context.Context, b *memoryBackend, req rewriteRequest, source map[string]any) (*gateway.RESTResponse, error) {
	versioned, err := b.versioned(ctx, req.bucket)
	if err != nil {
		return nil, err
	}
	keep, err := p.retainRemoved(ctx, b, req.scope, req.bucket, req.object, "", !versioned)
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
	source, refused, err := currentObject(ctx, b, req.sourceBucket, req.sourceKey, req.rest.Query.Get("sourceGeneration"))
	if refused != nil || err != nil {
		return nil, refused, err
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
		keyKind: "storage#rewriteResponse", "totalBytesRewritten": size, "objectSize": size, "done": true, keyResource: stored.JSON}}, nil
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
		return nil, fault("Unable to read the rewrite source")
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
		refused, err := missingXMLBucket(bucket, signed)
		return request{}, refused, err
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

// missingXMLBucket answers an XML request for a bucket that does not exist.
// GCP answers 404 NoSuchBucket (recorded: storage-go-client-projection
// "xml-read-missing-bucket"); a 501 would make the Go client, which reads
// media over XML, retry forever. The XML route matches almost any path, so an
// unsigned path whose first segment cannot name a bucket is a missing route,
// as the gateway reports.
func missingXMLBucket(bucket string, signed bool) (*gateway.RESTResponse, error) {
	if !signed && !bucketName.MatchString(bucket) {
		return nil, status.Error(codes.Unimplemented, "No provider implements this REST API method, and no bucket "+bucket+" exists for the XML API")
	}
	return xmlapi.Error(404, "NoSuchBucket", "The specified bucket does not exist.", ""), nil
}

// bucketName matches a name GCP accepts for a bucket without dots: 3 to 63
// lowercase letters, digits, dashes, and underscores, starting and ending
// with a letter or digit (storage/docs/buckets#naming). Dotted names, which
// may be longer, are matched up to the same length.
var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$`)

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

// xmlGet reads an object's media with the response-header overrides applied.
func xmlGet(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	generation := req.rest.Query.Get(keyGeneration)
	if _, refused, err := currentObject(ctx, b, req.bucket, req.object, generation); refused != nil || err != nil {
		return refused, err
	}
	query := url.Values{queryAlt: {altMedia}}
	if generation != "" {
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

// bucketPolicy is the provider-owned configuration of one bucket that
// object requests consult.
type bucketPolicy struct {
	// softDelete is the soft-delete retention in seconds; 0 disables it.
	softDelete int64
	public     string
	// retention is the retention policy's period in seconds; 0 when the bucket
	// has none.
	retention       int64
	defaultHold     bool
	objectRetention bool
	// namespace is whether the bucket has hierarchical namespace.
	namespace bool
}

// bucketSettings reads one bucket's provider-owned policy.
func (p *Provider) bucketSettings(scope state.Scope, bucket string) bucketPolicy {
	stored, found, err := p.catalog.Bucket(scope, bucket)
	if err != nil || !found {
		return bucketPolicy{}
	}
	var metadata struct {
		SoftDelete struct {
			Seconds json.Number `json:"retentionDurationSeconds"`
		} `json:"softDeletePolicy"`
		IAM struct {
			Public string `json:"publicAccessPrevention"`
		} `json:"iamConfiguration"`
		Retention *struct {
			Period json.Number `json:"retentionPeriod"`
		} `json:"retentionPolicy"`
		DefaultHold     bool `json:"defaultEventBasedHold"`
		ObjectRetention *struct {
			Mode string `json:"mode"`
		} `json:"objectRetention"`
		Namespace struct {
			Enabled bool `json:"enabled"`
		} `json:"hierarchicalNamespace"`
	}
	_ = json.Unmarshal([]byte(stored.Metadata), &metadata)
	policy := bucketPolicy{public: metadata.IAM.Public, defaultHold: metadata.DefaultHold, objectRetention: metadata.ObjectRetention != nil,
		namespace: metadata.Namespace.Enabled}
	policy.softDelete, _ = strconv.ParseInt(string(metadata.SoftDelete.Seconds), 10, 64)
	if metadata.Retention != nil {
		policy.retention, _ = strconv.ParseInt(string(metadata.Retention.Period), 10, 64)
	}
	return policy
}

// retentionExpiry is when a bucket retention policy stops protecting an
// object generation: the period after its creation, or after its
// event-based hold was released; none while that hold is on (recorded:
// storage-retention-holds "retained-upload", "event-hold-with-policy").
func retentionExpiry(policy bucketPolicy, overlay objectOverlay, object map[string]any) (time.Time, bool) {
	if policy.retention == 0 || isSet(overlay.eventBasedHold) {
		return time.Time{}, false
	}
	start := overlay.released
	if start.IsZero() {
		created, _ := object["timeCreated"].(string)
		parsed, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return time.Time{}, false
		}
		start = parsed
	}
	return start.Add(time.Duration(policy.retention) * time.Second), true
}

// isSet reports whether an optional hold is on.
func isSet(hold *bool) bool { return hold != nil && *hold }

// decorate adds retentionExpirationTime to each object a successful
// response carries when the bucket has a retention policy.
func (p *Provider) decorate(b *memoryBackend, scope state.Scope, bucket string, response *gateway.RESTResponse) {
	document, ok := response.JSON.(map[string]any)
	if !ok || response.Status >= http.StatusMultipleChoices {
		return
	}
	policy := p.bucketSettings(scope, bucket)
	if policy.retention == 0 {
		return
	}
	for _, object := range objectDocuments(document) {
		name, _ := object[keyName].(string)
		generation, _ := object[keyGeneration].(string)
		if expiry, ok := retentionExpiry(policy, b.overlay(bucket, name, generation), object); ok {
			object["retentionExpirationTime"] = expiry.UTC().Format(time.RFC3339Nano)
		}
	}
}

// objectDocuments returns the object documents a response carries: itself,
// a listing's items, or a rewrite's resource.
func objectDocuments(document map[string]any) []map[string]any {
	if document[keyKind] == kindObjectDocument {
		return []map[string]any{document}
	}
	var found []map[string]any
	for _, key := range []string{"items", keyResource} {
		for _, nested := range nestedDocuments(document[key]) {
			if nested[keyKind] == kindObjectDocument {
				found = append(found, nested)
			}
		}
	}
	return found
}

// refuseRemoval returns GCP's refusal for deleting, replacing, or making
// noncurrent the object generation current (nil for none): a hold, a bucket
// retention policy, or the object's own retention protects it.
func (p *Provider) refuseRemoval(b *memoryBackend, scope state.Scope, bucket string, current map[string]any) error {
	if current == nil {
		return nil
	}
	name, _ := current[keyName].(string)
	generation, _ := current[keyGeneration].(string)
	overlay := b.overlay(bucket, name, generation)
	protection := bucketconfig.Protection{TemporaryHold: isSet(overlay.temporaryHold), EventBasedHold: isSet(overlay.eventBasedHold)}
	if expiry, ok := retentionExpiry(p.bucketSettings(scope, bucket), overlay, current); ok {
		protection.Until = expiry
	}
	if overlay.retention != nil && overlay.retention.Until.After(protection.Until) {
		protection.Until = overlay.retention.Until
	}
	if refusal := protection.Refuse(bucket, name, p.now()); refusal != nil {
		return refusal
	}
	return nil
}

// created applies the bucket's default event-based hold to a generation an
// upload, rewrite, or restore has just created, and to its document, and
// creates the folders above it in a hierarchical-namespace bucket.
func (p *Provider) created(b *memoryBackend, scope state.Scope, bucket string, object map[string]any) {
	if object == nil {
		return
	}
	p.createParentFolders(scope, bucket, documentName(object))
	if !p.bucketSettings(scope, bucket).defaultHold {
		return
	}
	name, _ := object[keyName].(string)
	generation, _ := object[keyGeneration].(string)
	held := true
	b.setOverlay(bucket, name, generation, func(overlay *objectOverlay) { overlay.eventBasedHold = &held })
	object[keyEventBasedHold] = true
}

// createdObject is the object document a successful write response carries.
func createdObject(response *gateway.RESTResponse) map[string]any {
	document, ok := response.JSON.(map[string]any)
	if !ok || response.Status >= http.StatusMultipleChoices {
		return nil
	}
	if resource, ok := document[keyResource].(map[string]any); ok {
		return resource
	}
	return document
}

// retainRemoved snapshots the generation a mutation may remove (the named
// one, or the live one when generation is empty) when the bucket keeps
// soft-deleted objects and the mutation removes it permanently: a versioned
// bucket keeps an overwritten or deleted live generation as noncurrent
// instead. The returned keep records the snapshot once the mutation has
// succeeded; it does nothing for a failed mutation, a bucket without a
// policy, a generation that stays, or an object that did not exist.
func (p *Provider) retainRemoved(ctx context.Context, b *memoryBackend, scope state.Scope, bucket, name, generation string, permanent bool) (func(*gateway.RESTResponse) error, error) {
	skip := func(*gateway.RESTResponse) error { return nil }
	retention := p.bucketSettings(scope, bucket).softDelete
	if retention == 0 || !permanent {
		return skip, nil
	}
	current, missing, err := currentObject(ctx, b, bucket, name, generation)
	if err != nil {
		return nil, err
	}
	if missing != nil {
		return skip, nil
	}
	generation, _ = current[keyGeneration].(string)
	data, err := readSource(ctx, b, bucket, name, generation)
	if err != nil {
		return nil, err
	}
	return func(response *gateway.RESTResponse) error {
		// An unset status is the gateway's 200.
		if response == nil || (response.Status != 0 && (response.Status < 200 || response.Status >= 300)) {
			return nil
		}
		now := p.now()
		hard := now.Add(time.Duration(retention) * time.Second)
		current["softDeleteTime"], current["hardDeleteTime"] = storageTime(now), storageTime(hard)
		return p.catalog.retainObject(scope, softObject{Bucket: bucket, Name: name, Generation: generation,
			Object: current, Data: data, Hard: hard})
	}, nil
}

// versioned reports whether a bucket keeps noncurrent generations. The
// backend owns the setting, as it owns the generations it keeps.
func (b *memoryBackend) versioned(ctx context.Context, bucket string) (bool, error) {
	response, err := b.forward(ctx, http.MethodGet, "/storage/v1/b/"+url.PathEscape(bucket), nil, nil, nil)
	if err != nil {
		return false, err
	}
	if response.Status != http.StatusOK {
		return false, fault("Unable to read the bucket's versioning")
	}
	body, err := jsonObject(response.JSON)
	if err != nil {
		return false, err
	}
	versioning, _ := body["versioning"].(map[string]any)
	enabled, _ := versioning[keyEnabled].(bool)
	return enabled, nil
}

// prune forgets the overlay of a generation the backend no longer holds.
func (b *memoryBackend) prune(ctx context.Context, bucket, name, generation string) error {
	if generation == "" {
		return nil
	}
	response, err := b.forward(ctx, http.MethodGet, objectPath(bucket, name), url.Values{keyGeneration: {generation}}, nil, nil)
	if err != nil {
		return err
	}
	if response.Status == http.StatusNotFound {
		b.forgetGeneration(bucket, name, generation)
	}
	return nil
}

// listObjects serves objects.list, live or soft-deleted.
func (p *Provider) listObjects(ctx context.Context, b *memoryBackend, req request, softDeleted bool) (*gateway.RESTResponse, error) {
	if softDeleted {
		return p.listSoftDeleted(req)
	}
	folders, err := p.foldersAsPrefixes(req)
	if err != nil {
		return nil, err
	}
	query := req.rest.Query
	limit, refused := listSize(query.Get(queryMaxResults), 0)
	if refused != nil {
		return refused, nil
	}
	after, valid := listAfter(objectPages, query.Get(queryPageToken))
	if !valid {
		// Recorded: storage-list-paging "invalid-page-token".
		return wireError(400, "invalid", "Next page token not valid: "+query.Get(queryPageToken)), nil
	}
	objects, refused, err := listedObjects(ctx, b, req.bucket, query)
	if refused != nil || err != nil {
		return refused, err
	}
	page, next := pageAfter(mergeEntries(objectEntries(objects, query), folders), func(entry listEntry) string { return entry.key }, after, limit, objectPages)
	items, prefixes := []map[string]any{}, []string{}
	for _, entry := range page {
		if entry.object == nil {
			prefixes = append(prefixes, entry.name)
		} else {
			items = append(items, entry.object)
		}
	}
	result := provider.ListBody("items", items, next)
	result[keyKind] = kindObjects
	if len(prefixes) > 0 {
		result["prefixes"] = prefixes
	}
	return &gateway.RESTResponse{JSON: result}, nil
}

// listedObjects reads every object of a bucket under the listing's prefix
// from the backend, in name order. Paging, offsets, and the delimiter are
// the provider's (objectEntries, pageAfter), as GCP applies them.
func listedObjects(ctx context.Context, b *memoryBackend, bucket string, query url.Values) ([]map[string]any, *gateway.RESTResponse, error) {
	backendQuery := url.Values{}
	if prefix := query.Get(queryPrefix); prefix != "" {
		backendQuery.Set(queryPrefix, prefix)
	}
	// The backend reads only a lowercase true; GCP reads booleans in any case.
	if versions, _ := booleanOption(query, queryVersions); versions {
		backendQuery.Set(queryVersions, valueTrue)
	}
	listed, err := b.forward(ctx, http.MethodGet, "/storage/v1/b/"+url.PathEscape(bucket)+"/o", backendQuery, nil, nil)
	if err != nil || listed.Status != http.StatusOK {
		return nil, listed, err
	}
	body, err := jsonObject(listed.JSON)
	if err != nil {
		return nil, nil, err
	}
	objects := itemList(body["items"])
	// GCP lists versions of one name oldest first (recorded:
	// storage-object-versioning "versions-listing").
	slices.SortStableFunc(objects, func(left, right map[string]any) int {
		return cmp.Or(cmp.Compare(documentName(left), documentName(right)), cmp.Compare(generationOf(left), generationOf(right)))
	})
	return objects, nil, nil
}

// generationOf is an object document's generation number.
func generationOf(document map[string]any) int64 {
	text, _ := document[keyGeneration].(string)
	generation, _ := strconv.ParseInt(text, 10, 64)
	return generation
}

// documentName is the name of a bucket or object document.
func documentName(document map[string]any) string {
	name, _ := document[keyName].(string)
	return name
}

// listEntry is one objects.list entry: an object, or a prefix that stands for
// every object under it when the listing has a delimiter. key orders entries
// and resumes a page: the name, plus, in a versions=true listing, the
// zero-padded generation, so a page token can resume between two versions of
// one name (recorded: storage-object-versioning "versions-pages").
type listEntry struct {
	name, key string
	object    map[string]any
}

// objectEntries applies startOffset, endOffset, and the delimiter to a
// prefix's objects, in name order. As GCP does, the offsets select objects
// before they collapse: a prefix is listed when any object under it is in
// range, and it is one entry toward maxResults (recorded:
// storage-list-paging "delimited-pages", "offsets-inside-prefix").
func objectEntries(objects []map[string]any, query url.Values) []listEntry {
	prefix, delimiter := query.Get(queryPrefix), query.Get("delimiter")
	start, end := query.Get("startOffset"), query.Get("endOffset")
	versions, _ := booleanOption(query, queryVersions)
	entries := make([]listEntry, 0, len(objects))
	for _, object := range objects {
		name := documentName(object)
		rest, matched := strings.CutPrefix(name, prefix)
		if !matched || name < start || (end != "" && name >= end) {
			continue
		}
		if index := strings.Index(rest, delimiter); delimiter != "" && index >= 0 {
			// Objects under one prefix are adjacent in name order.
			collapsed := name[:len(prefix)+index+len(delimiter)]
			if len(entries) == 0 || entries[len(entries)-1].name != collapsed {
				entries = append(entries, listEntry{name: collapsed, key: collapsed})
			}
			continue
		}
		entries = append(entries, listEntry{name: name, key: entryKey(object, versions), object: object})
	}
	return entries
}

// mergeEntries adds folder prefixes to a listing's entries, in key order and
// without repeating a prefix the objects already list.
func mergeEntries(entries, folders []listEntry) []listEntry {
	for _, folder := range folders {
		if !slices.ContainsFunc(entries, func(entry listEntry) bool { return entry.object == nil && entry.name == folder.name }) {
			entries = append(entries, folder)
		}
	}
	slices.SortStableFunc(entries, func(left, right listEntry) int { return cmp.Compare(left.key, right.key) })
	return entries
}

// entryKey is an object entry's listEntry key.
func entryKey(object map[string]any, versions bool) string {
	if !versions {
		return documentName(object)
	}
	return fmt.Sprintf("%s\x00%020d", documentName(object), generationOf(object))
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
	if p.bucketSettings(req.scope, req.bucket).softDelete == 0 {
		return wireError(400, "invalid", "Soft delete policy is required to list soft-deleted versions"), nil
	}
	prefix := req.rest.Query.Get(queryPrefix)
	items := []any{}
	for _, object := range p.catalog.softDeletedObjects(req.scope, req.bucket, p.now()) {
		if strings.HasPrefix(object.Name, prefix) {
			items = append(items, maps.Clone(object.Object))
		}
	}
	if len(items) > limit {
		if limit == 0 {
			// maxResults=0 lists nothing, as a live listing does.
			return &gateway.RESTResponse{JSON: map[string]any{keyKind: kindObjects}}, nil
		}
		return nil, gap("soft-deleted listings longer than one page")
	}
	body := map[string]any{keyKind: kindObjects}
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
	limit, refused := listSize(query.Get(queryMaxResults), 0)
	return limit, refused, nil
}

// maxListResults is the largest page of a Storage list.
const maxListResults = 1000

// kindObjects is the kind of an objects.list response.
const kindObjects = "storage#objects"

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
	object, found := p.catalog.softDeletedObject(req.scope, req.bucket, req.object, generation, p.now())
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
	if p.bucketSettings(req.scope, req.bucket).softDelete == 0 {
		return nil, gap("restore in a bucket without a soft-delete policy")
	}
	object, refused := p.softDeletedGeneration(req, wireError(400, "required", "Required parameter: generation"))
	if refused != nil {
		return refused, nil
	}
	if response, err := p.replaceable(b, req.scope, req.bucket, req.rest, current); response != nil || err != nil {
		return response, err
	}
	keep, err := p.retainWrite(ctx, b, req)
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
	if err := keep(stored); err != nil {
		return nil, err
	}
	p.created(b, req.scope, req.bucket, createdObject(stored))
	p.notifyWrite(ctx, b, req.scope, req.bucket, createdObject(stored), current)
	return stored, nil
}
