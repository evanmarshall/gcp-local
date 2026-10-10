package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Provider struct {
	index   *state.Index
	catalog *Catalog
	iam     *iam.Engine
	manager *backend.Manager
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("storage requires registry, lifecycle, index, and iam")

var methods = []string{MethodBucketsInsert, MethodBucketsList, MethodBucketsGet, MethodBucketsPatch, MethodBucketsDelete, MethodBucketsGetIamPolicy, MethodBucketsSetIamPolicy, MethodObjectsInsert, MethodObjectsList, MethodObjectsGet, MethodObjectsDelete}

func New(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	p := &Provider{index: index, catalog: NewCatalog(index), iam: engine, manager: manager}
	if err := RegisterPermissions(engine); err != nil {
		return nil, err
	}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      func(state.Scope) (backend.Backend, error) { return &memoryBackend{}, nil },
		AfterReset: func(scope state.Scope) error {
			if err := p.catalog.Reset(scope); err != nil {
				return err
			}
			engine.ClearScope(serviceName, scope)
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the storage backend: %w", err)
	}
	for _, method := range methods {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}
func projectResource(project string) string {
	return "//cloudresourcemanager.googleapis.com/projects/" + project
}
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Storage coverage gap: "+message)
}
func wireError(code int, reason, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{"error": map[string]any{"code": code, "message": message, "errors": []any{map[string]any{"domain": "global", "reason": reason, "message": message}}}}}
}
func notFound(bucket, object string) *gateway.RESTResponse {
	if object != "" {
		return wireError(404, "notFound", "No such object: "+bucket+"/"+object)
	}
	return wireError(404, "notFound", "The specified bucket does not exist.")
}
func (p *Provider) authorize(call gateway.Call, target iam.Target, features map[string]bool, complete bool) (*gateway.RESTResponse, string, error) {
	var d iam.Decision
	var err error
	if complete {
		d, err = p.iam.Authorize(call, target, features)
	} else {
		d, err = p.iam.Baseline(call, target)
	}
	if err == nil {
		return nil, d.ID, nil
	}
	code := status.Code(err)
	if code != codes.PermissionDenied && code != codes.Unauthenticated {
		return nil, d.ID, fmt.Errorf("authorize %s: %w", call.Method, err)
	}
	permission := ""
	for _, check := range d.Checks {
		if !check.Allowed {
			permission = check.Permission
			break
		}
	}
	if permission == "" {
		return nil, d.ID, fmt.Errorf("identify the denied permission for %s: %w", call.Method, err)
	}
	actor := strings.TrimPrefix(strings.TrimPrefix(call.Principal.Subject, "serviceAccount:"), "user:")
	httpCode, reason := 403, "forbidden"
	if call.Principal.State == gateway.Anonymous {
		actor = "Anonymous caller"
		httpCode = 401
		reason = "required"
	}
	kind := kindBucket
	if strings.HasPrefix(permission, "storage.objects.") {
		kind = "object"
	}
	message := fmt.Sprintf("%s does not have %s access to the Google Cloud Storage %s. Permission '%s' denied on resource '%s' (or it may not exist).", actor, permission, kind, permission, target.Name)
	if call.Method == MethodObjectsInsert && permission == MethodObjectsDelete {
		message = actor + " does not have storage.objects.delete access to the Google Cloud Storage object."
	}
	response := wireError(httpCode, reason, message)
	response.Headers = http.Header{"X-Gcp-Local-Decision-Id": {d.ID}}
	if call.Method == MethodObjectsInsert && permission != MethodObjectsDelete {
		response.Headers.Set("Content-Type", "text/html")
	}
	return response, d.ID, nil
}
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	r, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Storage request")
	}
	if err := validateQuery(call.Method, r); err != nil {
		return nil, err
	}
	bucket, object := r.Path["bucket"], r.Path["object"]
	var upload *uploadRequest
	if call.Method == MethodObjectsInsert {
		var err error
		upload, err = parseUpload(r)
		if err != nil {
			return nil, err
		}
		object = upload.name
	}
	target, scope, projectMethod, refused, err := p.resolveTarget(call, bucket, object)
	if refused != nil || err != nil {
		return refused, err
	}
	call.Resource = target.Name
	call.Location = state.Global
	// This preflight permits lazy startup/state resolution only. Every operation
	// reauthorizes under the backend gate before data access or mutation.
	denied, _, err := p.authorize(call, target, nil, false)
	if denied != nil || err != nil {
		return denied, err
	}
	var response *gateway.RESTResponse
	err = p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(session context.Context, driver backend.Backend) error {
		call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		b, backendErr := backendOf(driver)
		if backendErr != nil {
			return backendErr
		}
		if !projectMethod {
			resource, found := p.index.Get(BucketResource(bucket))
			if !found || resource.Scope != scope {
				response = notFound(bucket, "")
				return nil
			}
		}
		features, current, featureErr := p.objectFeatures(session, b, call, bucket, object)
		if featureErr != nil {
			return featureErr
		}
		denied, id, authorizeErr := p.authorize(call, target, features, true)
		if authorizeErr != nil {
			return authorizeErr
		}
		if denied != nil {
			response = denied
			return nil
		}
		response, err = p.execute(session, b, call, r, scope, bucket, object, upload, current)
		if response != nil {
			if response.Headers == nil {
				response.Headers = http.Header{}
			}
			response.Headers.Set("X-Gcp-Local-Decision-Id", id)
		}
		return err
	})
	if err != nil {
		return response, fmt.Errorf("forward %s: %w", call.Method, err)
	}
	return response, nil
}

// resolveTarget derives the IAM target and backend scope for one request. A
// project-scoped method addresses the project; every other method addresses the
// bucket, whose recorded scope decides the backend instance. An unknown bucket
// is refused with a notFound response.
func (p *Provider) resolveTarget(call gateway.Call, bucket, object string) (iam.Target, state.Scope, bool, *gateway.RESTResponse, error) {
	target := iam.Target{Name: BucketResource(bucket)}
	scope := state.Scope{Location: state.Global}
	projectMethod := call.Method == MethodBucketsInsert || call.Method == MethodBucketsList
	if projectMethod {
		scope.Project = call.Project
		target.Name = projectResource(call.Project)
		project, ok := p.index.Get(target.Name)
		if !ok || project.Kind != kindProjectRef || !numericProjectNumber.MatchString(project.Number) {
			return iam.Target{}, state.Scope{}, projectMethod, nil, gap("configure the explicit project and numeric project number")
		}
		return target, scope, projectMethod, nil, nil
	}
	resource, found := p.index.Get(target.Name)
	if !found {
		return iam.Target{}, state.Scope{}, projectMethod, notFound(bucket, ""), nil
	}
	scope = resource.Scope
	call.Project = scope.Project
	if object != "" {
		target.Parent = target.Name
		target.Name += "/objects/" + object
	}
	return target, scope, projectMethod, nil, nil
}

// objectFeatures resolves whether an inserted object already exists, which the
// object-exists permission plan depends on.
func (p *Provider) objectFeatures(ctx context.Context, b *memoryBackend, call gateway.Call, bucket, object string) (map[string]bool, map[string]any, error) {
	features := map[string]bool{}
	if call.Method != MethodObjectsInsert {
		return features, nil, nil
	}
	result, err := b.forward(ctx, "GET", objectPath(bucket, object), nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	switch result.Status {
	case 200:
		payload, payloadErr := jsonObject(result.JSON)
		if payloadErr != nil {
			return nil, nil, payloadErr
		}
		features[objectExistsCondition] = true
		return features, payload, nil
	case 404:
		features[objectExistsCondition] = false
		return features, nil, nil
	default:
		return nil, nil, status.Error(codes.Unavailable, "Unable to resolve existing object before authorization")
	}
}

// kindProjectRef is the index kind that marks a resource-manager project.
const kindProjectRef = "project"

// numericProjectNumber pins the project-number shape Storage requires.
var numericProjectNumber = regexp.MustCompile(`^[0-9]{1,20}$`)

func objectPath(bucket, object string) string {
	return "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(object)
}

// Forward only a provider-selected route over upstream's memory transport. A
// caller URL, authorization header, proxy, or notification endpoint never enters it.
func (b *memoryBackend) forward(ctx context.Context, method, path string, query url.Values, headers http.Header, body []byte) (*gateway.RESTResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://storage.invalid"+path+"?"+query.Encode(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build backend request: %w", err)
	}
	for _, key := range []string{"Content-Type", "Range", "Accept-Encoding"} {
		if value := headers.Get(key); value != "" {
			req.Header.Set(key, value)
		}
	}
	result, err := b.request(req)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Storage backend request failed")
	}
	defer result.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(result.Body, maxObjectSize+(1<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read backend response: %w", err)
	}
	if len(payload) > maxObjectSize+(1<<20) {
		return nil, status.Error(codes.ResourceExhausted, "Storage response exceeds supported size")
	}
	response := &gateway.RESTResponse{Status: result.StatusCode, Headers: result.Header.Clone()}
	response.Headers.Del("Content-Length")
	if strings.Contains(result.Header.Get("Content-Type"), "application/json") && (result.StatusCode >= 300 || query.Get("alt") != altMedia) {
		var value map[string]any
		if len(payload) > 0 && json.Unmarshal(payload, &value) != nil {
			return nil, status.Error(codes.Internal, "Invalid backend JSON")
		}
		response.JSON = value
		removeBackendLinks(value)
		if etag, ok := value["etag"].(string); ok {
			response.Headers.Set("ETag", `"`+etag+`"`)
		}
	} else {
		response.Body = bytes.NewReader(payload)
		if generation := response.Headers.Get("X-Goog-Generation"); generation != "" {
			response.Headers.Set("ETag", `"`+objectETag(generation, "1")+`"`)
			response.Headers.Set("X-Goog-Metageneration", "1")
		}
	}
	return response, nil
}

// Upstream links use its private origin. SDKs derive their URLs from the
// configured endpoint when these optional fields are absent.
func removeBackendLinks(value map[string]any) {
	delete(value, "mediaLink")
	delete(value, "selfLink")
	delete(value, "acl")
	delete(value, "defaultObjectAcl")
	if generation, ok := value["generation"].(string); ok {
		metageneration, _ := value["metageneration"].(string)
		value["etag"] = objectETag(generation, metageneration)
	}
	if items, ok := value["items"].([]any); ok {
		for _, item := range items {
			if object, ok := item.(map[string]any); ok {
				removeBackendLinks(object)
			}
		}
	}
}

// The backend uses the content MD5 as its etag. GCP changes JSON etags on a
// replacement even with identical bytes, so derive an opaque tag from the
// backend-owned generation/metageneration. This adds no duplicate state.
func objectETag(generation, metageneration string) string {
	return base64.StdEncoding.EncodeToString([]byte(generation + ":" + metageneration))
}

func (p *Provider) execute(ctx context.Context, b *memoryBackend, call gateway.Call, r *gateway.RESTRequest, scope state.Scope, bucket, object string, upload *uploadRequest, current map[string]any) (*gateway.RESTResponse, error) {
	path := "/storage/v1/b/" + url.PathEscape(bucket)
	switch call.Method {
	case MethodBucketsInsert:
		return p.createBucket(ctx, b, r, scope)
	case MethodBucketsPatch:
		return p.patchBucket(ctx, b, r, scope, bucket)
	case MethodBucketsGetIamPolicy:
		return p.readBucketPolicy(bucket)
	case MethodBucketsSetIamPolicy:
		return p.writeBucketPolicy(bucket, r)
	case MethodBucketsList:
		return p.listBuckets(ctx, b, r, scope)
	case MethodBucketsGet:
		return p.getBucket(ctx, b, r, scope, path)
	case MethodBucketsDelete:
		return p.deleteBucket(ctx, b, r, scope, path, bucket)
	case MethodObjectsList:
		return b.forward(ctx, "GET", path+"/o", r.Query, nil, nil)
	}
	return p.executeObject(ctx, b, call, r, bucket, object, upload, current)
}

// executeObject handles the object methods, which share one precondition pass.
func (p *Provider) executeObject(ctx context.Context, b *memoryBackend, call gateway.Call, r *gateway.RESTRequest, bucket, object string, upload *uploadRequest, current map[string]any) (*gateway.RESTResponse, error) {
	if call.Method != MethodObjectsInsert {
		response, err := b.forward(ctx, "GET", objectPath(bucket, object), nil, nil, nil)
		if err != nil {
			return nil, err
		}
		if response.Status == 404 {
			return notFound(bucket, object), nil
		}
		if response.Status != 200 {
			return response, nil
		}
		body, bodyErr := jsonObject(response.JSON)
		if bodyErr != nil {
			return nil, bodyErr
		}
		current = body
	}
	if response, err := preconditions(r, current, call.Method); response != nil || err != nil {
		return response, err
	}
	if call.Method == MethodObjectsInsert {
		return b.forward(ctx, "POST", "/upload/storage/v1/b/"+url.PathEscape(bucket)+"/o", upload.query, upload.headers, upload.body)
	}
	if generation := r.Query.Get("generation"); generation != "" && current["generation"] != generation {
		return notFound(bucket, object), nil
	}
	if call.Method == MethodObjectsGet {
		return b.forward(ctx, "GET", objectPath(bucket, object), r.Query, r.Headers, nil)
	}
	response, err := b.forward(ctx, "DELETE", objectPath(bucket, object), r.Query, nil, nil)
	if err == nil && response.Status == 200 {
		response.Status = 204
		response.JSON = nil
		response.Body = nil
	}
	return response, err
}

// readBucketPolicy returns the IAM policy for one bucket.
func (p *Provider) readBucketPolicy(bucket string) (*gateway.RESTResponse, error) {
	policy, err := p.iam.GetPolicy(BucketResource(bucket))
	if err != nil {
		return nil, fmt.Errorf("read bucket policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// writeBucketPolicy validates and stores one bucket IAM policy.
func (p *Provider) writeBucketPolicy(bucket string, r *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var request struct {
		iam.Policy
		ResourceID string `json:"resourceId"`
		Kind       string `json:"kind"`
	}
	decoder := json.NewDecoder(bytes.NewReader(r.JSON))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	if request.ResourceID != "" && request.ResourceID != "/b/"+bucket && request.ResourceID != "projects/_/buckets/"+bucket {
		return nil, status.Error(codes.InvalidArgument, "Policy resource disagrees with bucket")
	}
	if request.Kind != "" && request.Kind != "storage#policy" {
		return nil, status.Error(codes.InvalidArgument, "Invalid policy kind")
	}
	for _, binding := range request.Bindings {
		for _, member := range binding.Members {
			if member == "allUsers" || member == "allAuthenticatedUsers" {
				return wireError(412, "conditionNotMet", "The bucket is subject to public access prevention."), nil
			}
		}
	}
	result, err := p.iam.SetPolicy(BucketResource(bucket), request.Policy)
	if err != nil {
		return nil, fmt.Errorf("write bucket policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: result}, nil
}

// listBuckets mirrors every returned bucket into the provider-owned catalog.
func (p *Provider) listBuckets(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	response, err := b.forward(ctx, "GET", "/storage/v1/b", r.Query, nil, nil)
	if err != nil {
		return nil, err
	}
	if response.Status == 200 {
		body, bodyErr := jsonObject(response.JSON)
		if bodyErr != nil {
			return nil, bodyErr
		}
		for _, item := range itemList(body["items"]) {
			p.bucketMetadata(scope, item)
		}
	}
	return response, nil
}

// getBucket reads one bucket and mirrors its metadata into the catalog.
func (p *Provider) getBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, path string) (*gateway.RESTResponse, error) {
	response, err := b.forward(ctx, "GET", path, r.Query, nil, nil)
	if err != nil || response.Status != 200 {
		return response, err
	}
	body, bodyErr := jsonObject(response.JSON)
	if bodyErr != nil {
		return nil, bodyErr
	}
	p.bucketMetadata(scope, body)
	return response, nil
}

// deleteBucket deletes one bucket, then releases its catalog and IAM state.
func (p *Provider) deleteBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, path, bucket string) (*gateway.RESTResponse, error) {
	response, err := b.forward(ctx, "DELETE", path, r.Query, nil, nil)
	if err != nil {
		return response, err
	}
	if response.Status == 200 {
		response.Status = 204
		response.JSON = nil
		response.Body = nil
	}
	if response.Status == 204 {
		err = p.catalog.ForgetBucket(scope, bucket)
		p.iam.Forget(BucketResource(bucket))
	}
	return response, err
}
