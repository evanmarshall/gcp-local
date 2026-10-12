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
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	for _, method := range append(methods, "storage.folders.insert", "storage.folders.get", "storage.folders.list", "storage.folders.delete") {
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Storage coverage gap: "+message)
}

// wireError renders the Storage JSON API body, which carries an errors[] reason
// list and no status field, unlike the shared Google API body.
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
	permission := deniedPermission(d)
	if permission == "" {
		return nil, d.ID, fmt.Errorf("identify the denied permission for %s: %w", call.Method, err)
	}
	return renderDenial(call, target, d.ID, permission), d.ID, nil
}

// deniedPermission returns "" when no check failed so authorize can treat an
// unexplained denial as an internal error instead of rendering "unspecified".
func deniedPermission(d iam.Decision) string {
	for _, check := range d.Checks {
		if !check.Allowed {
			return check.Permission
		}
	}
	return ""
}

// renderDenial builds the wire denial response Google Storage clients expect,
// including the header-only upload fallback message on object create.
func renderDenial(call gateway.Call, target iam.Target, decisionID, permission string) *gateway.RESTResponse {
	httpCode, reason, message := denialText(call, target.Name, permission)
	response := wireError(httpCode, reason, message)
	response.Headers = http.Header{"X-Gcp-Local-Decision-Id": {decisionID}}
	if call.Method == MethodObjectsInsert && permission != MethodObjectsDelete {
		response.Headers.Set("Content-Type", "text/html")
	}
	return response
}

// denialText renders the actor, resource kind, and message shape for a denial.
func denialText(call gateway.Call, target, permission string) (int, string, string) {
	httpCode, reason := 403, "forbidden"
	actor := strings.TrimPrefix(strings.TrimPrefix(call.Principal.Subject, "serviceAccount:"), "user:")
	if call.Principal.State == gateway.Anonymous {
		actor, httpCode, reason = "Anonymous caller", 401, "required"
	}
	kind := kindBucket
	if strings.HasPrefix(permission, "storage.objects.") {
		kind = "object"
	}
	message := fmt.Sprintf("%s does not have %s access to the Google Cloud Storage %s. Permission '%s' denied on resource '%s' (or it may not exist).", actor, permission, kind, permission, target)
	if call.Method == MethodObjectsInsert && permission == MethodObjectsDelete {
		message = actor + " does not have storage.objects.delete access to the Google Cloud Storage object."
	}
	return httpCode, reason, message
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
		target.Name = provider.ProjectResource(call.Project)
		project, ok := p.index.Get(target.Name)
		if !ok || project.Kind != kindProjectRef || !numericProjectNumber.MatchString(project.Number) {
			return iam.Target{}, state.Scope{}, projectMethod, nil, gap("configure the explicit project and numeric project number")
		}
		return target, scope, projectMethod, nil, nil
	}
	if strings.HasPrefix(call.Method, "storage.folders.") || call.Method == "storage.folders.insert" {
		resource, found := p.index.Get(target.Name)
		if !found {
			return iam.Target{}, state.Scope{}, projectMethod, notFound(bucket, ""), nil
		}
		return target, resource.Scope, projectMethod, nil, nil
	}
	resource, found := p.index.Get(target.Name)
	if !found {
		return iam.Target{}, state.Scope{}, projectMethod, notFound(bucket, ""), nil
	}
	scope = resource.Scope
	call.Project = scope.Project
	// IAM condition attributes use Storage's documented resource names.
	target.Attributes = iam.ResourceAttributes{Name: "projects/_/buckets/" + bucket,
		Type: "storage.googleapis.com/Bucket", Service: "storage.googleapis.com"}
	if object != "" {
		target.Parent = target.Name
		target.Name += "/objects/" + object
		target.Attributes.Name += "/objects/" + object
		target.Attributes.Type = "storage.googleapis.com/Object"
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
// decodeBackendJSON re-materializes an upstream JSON payload as the response
// body, stripping the upstream's pagination links and quoting its etag.
func decodeBackendJSON(payload []byte, response *gateway.RESTResponse) {
	var value map[string]any
	if len(payload) > 0 && json.Unmarshal(payload, &value) != nil {
		response.JSON = map[string]any{}
		return
	}
	if value == nil {
		// An empty 2xx payload carries no document.
		response.JSON = map[string]any{}
		return
	}
	response.JSON = value
	removeBackendLinks(value)
	// Real GCS reports rpo only for multi-region and dual-region buckets;
	// the recorded single-region snapshot carries rpo null. The fake backend
	// echoes the create body, so null the field here: the Terraform provider
	// treats an explicit null as unset, while an absent key coerces to "".
	// The multi-region shape is an explicit gap until it has its own recording.
	value["rpo"] = nil
	if etag, ok := value["etag"].(string); ok {
		response.Headers.Set("ETag", `"`+etag+`"`)
	}
}

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
		decodeBackendJSON(payload, response)
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
	if strings.HasPrefix(call.Method, "storage.folders.") {
		return p.executeFolder(call, r, scope, bucket)
	}
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
	request, err := decodeBucketPolicy(bucket, r.JSON)
	if err != nil {
		return nil, err
	}
	if public, refused := policyBindsPublic(request.Bindings); refused {
		return public, nil
	}
	result, err := p.iam.SetPolicy(BucketResource(bucket), request.Policy)
	if status.Code(err) == codes.InvalidArgument {
		// Recorded: version and condition errors are 400 "invalid" in Storage's format.
		return wireError(400, "invalid", status.Convert(err).Message()), nil
	}
	if err != nil {
		return nil, fmt.Errorf("write bucket policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: result}, nil
}

// bucketPolicyEnvelope is the wire shape of a bucket IAM policy write.
type bucketPolicyEnvelope struct {
	iam.Policy
	ResourceID string `json:"resourceId"`
	Kind       string `json:"kind"`
}

// decodeBucketPolicy reads and shapes one bucket IAM policy body.
func decodeBucketPolicy(bucket string, body []byte) (bucketPolicyEnvelope, error) {
	var request bucketPolicyEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		return bucketPolicyEnvelope{}, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	if request.ResourceID != "" && request.ResourceID != "/b/"+bucket && request.ResourceID != "projects/_/buckets/"+bucket {
		return bucketPolicyEnvelope{}, status.Error(codes.InvalidArgument, "Policy resource disagrees with bucket")
	}
	if request.Kind != "" && request.Kind != "storage#policy" {
		return bucketPolicyEnvelope{}, status.Error(codes.InvalidArgument, "Invalid policy kind")
	}
	return request, nil
}

// policyBindsPublic reports whether a policy grants allUsers or all
// authenticated users, which public access prevention forbids.
func policyBindsPublic(bindings []iam.Binding) (*gateway.RESTResponse, bool) {
	for _, binding := range bindings {
		for _, member := range binding.Members {
			if member == "allUsers" || member == "allAuthenticatedUsers" {
				return wireError(412, "conditionNotMet", "The bucket is subject to public access prevention."), true
			}
		}
	}
	return nil, false
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

// executeFolder serves the Storage Control folders surface. Folders are
// provider-owned because the storage backend does not model them; state lives
// in the catalog and clears with the scope.
func (p *Provider) executeFolder(call gateway.Call, r *gateway.RESTRequest, scope state.Scope, bucket string) (*gateway.RESTResponse, error) {
	folderID := r.Path["folder"]
	folderWire := func(record Folder) map[string]any {
		return map[string]any{"name": record.Name, "bucket": bucket}
	}
	folderMissing := func() *gateway.RESTResponse {
		return &gateway.RESTResponse{Status: 404, JSON: map[string]any{"error": map[string]any{"code": 404, "message": "No such object: folders/" + folderID}}}
	}
	switch call.Method {
	case MethodFoldersCreate, "storage.folders.insert":
		var body struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(r.JSON, &body) != nil || body.Name == "" {
			return nil, provider.Invalid("Expected a folder name")
		}
		folderID = body.Name
		if _, ok, _ := p.catalog.Folder(scope, bucket, folderID); ok {
			return &gateway.RESTResponse{Status: 409, JSON: map[string]any{"error": map[string]any{"code": 409, "message": "Folder already exists"}}}, nil
		}
		if err := p.catalog.PublishFolder(scope, Folder{Bucket: bucket, Name: folderID}); err != nil {
			return nil, fmt.Errorf("publish folder: %w", err)
		}
		return &gateway.RESTResponse{JSON: folderWire(Folder{Bucket: bucket, Name: folderID})}, nil
	case MethodFoldersGet:
		record, ok, err := p.catalog.Folder(scope, bucket, folderID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return folderMissing(), nil
		}
		return &gateway.RESTResponse{JSON: folderWire(record)}, nil
	case MethodFoldersList:
		items, err := p.catalog.Folders(scope, bucket)
		if err != nil {
			return nil, err
		}
		wired := []any{}
		for _, record := range items {
			wired = append(wired, folderWire(record))
		}
		return &gateway.RESTResponse{JSON: map[string]any{"folders": wired}}, nil
	default:
		removed, err := p.catalog.DeleteFolder(scope, bucket, folderID)
		if err != nil {
			return nil, err
		}
		if !removed {
			return folderMissing(), nil
		}
		return &gateway.RESTResponse{Status: 204}, nil
	}
}
