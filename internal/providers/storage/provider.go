package storage

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	storageapi "github.com/evanmarshall/gcp-local/api/generated/storage"
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

type Provider struct {
	index    *state.Index
	catalog  *Catalog
	iam      *iam.Engine
	manager  *backend.Manager
	registry *gateway.Registry
	// now is the clock lifecycle conditions, retention, and holds read.
	now func() time.Time
}

// Config carries the provider's injectable dependencies. A nil Now is wall
// time; tests substitute a manual clock to evaluate lifecycle rules
// deterministically.
type Config struct {
	Now func() time.Time
}

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("storage requires registry, lifecycle, index, and iam")

// registeredMethods are the gateway routes this provider serves. Folder create
// arrives as storage.folders.insert from the generated route table.
var registeredMethods = []string{
	MethodBucketsInsert, MethodBucketsList, MethodBucketsGet, MethodBucketsPatch, MethodBucketsDelete,
	MethodBucketsGetIamPolicy, MethodBucketsSetIamPolicy, MethodBucketsGetLayout, MethodBucketsLock,
	MethodObjectsInsert, MethodObjectsList, MethodObjectsGet, MethodObjectsDelete,
	MethodFoldersInsert, MethodFoldersGet, MethodFoldersList, MethodFoldersDelete,
	MethodXMLGet, MethodXMLPut, MethodObjectsRewrite, MethodObjectsPatch, MethodObjectsRestore,
}

// New registers the Storage provider. Its background work stops when ctx ends.
func New(ctx context.Context, config Config, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	p := &Provider{index: index, catalog: NewCatalog(index), iam: engine, manager: manager, registry: registry, now: config.Now}
	if p.now == nil {
		p.now = time.Now
	}
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
	if err := p.registerRoutes(registry); err != nil {
		return nil, err
	}
	_ = ctx
	return p, nil
}

// registerRoutes binds the served methods to handle and every other generated
// Storage method to an explicit 4xx gap.
func (p *Provider) registerRoutes(registry *gateway.Registry) error {
	handlers := map[string]gateway.Handler{}
	for _, route := range storageapi.StorageRoutes {
		handlers[route.Method] = storageErrors(unsupported)
	}
	for _, method := range registeredMethods {
		handlers[method] = storageErrors(p.handle)
	}
	for method, handler := range handlers {
		if err := registry.Register(method, handler); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// gap refuses an unimplemented Storage feature with 400, not UNIMPLEMENTED:
// the Go client retries every 5xx without an attempt limit, so a 501 hangs it.
// This is a deliberate exception to the gateway's UNIMPLEMENTED convention.
func gap(message string) error {
	return status.Error(codes.InvalidArgument, storageGap+message)
}

const storageGap = "Storage coverage gap: "

// fault reports a failure inside the emulator for a request GCP would serve,
// such as a broken backend invariant. storageErrors renders it as a 400, not a
// 5xx: the Go client retries every 5xx without a limit, and nothing here
// clears on a retry.
func fault(message string) error {
	return status.Error(codes.Internal, message)
}

// storageErrors renders every error a Storage handler returns in the API's
// own shape: the JSON API's errors[] body, which the shared gateway body
// lacks, or the XML API's error document. Only 4xx codes reach the wire;
// faults and wrapped non-status errors become 400 "Storage emulator fault".
// UNIMPLEMENTED (a path no route or bucket claims) and context errors keep the
// gateway's rendering.
func storageErrors(handler gateway.Handler) gateway.Handler {
	return func(call gateway.Call, raw any) (any, error) {
		response, err := handler(call, raw)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return response, err
		}
		refusal := statusRefusal(err)
		if refusal == nil {
			return nil, err
		}
		if call.Method == MethodXMLGet || call.Method == MethodXMLPut {
			return xmlapi.Result(refusal), nil
		}
		return refusal, nil
	}
}

// statusRefusal is the Storage JSON API response for one handler error, or
// nil for UNIMPLEMENTED. Reasons are GCP's for the same HTTP codes.
func statusRefusal(err error) *gateway.RESTResponse {
	if refusal := configRefusal(err); refusal != nil {
		return refusal
	}
	var wrapped interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &wrapped) {
		// A non-status error can carry backend payloads; report none of it.
		return wireError(400, "invalid", "Storage emulator fault: internal provider error")
	}
	refused := wrapped.GRPCStatus()
	message := refused.Message()
	switch refused.Code() {
	case codes.Unimplemented:
		return nil
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return wireError(400, "invalid", message)
	case codes.Unauthenticated:
		return wireError(401, "required", message)
	case codes.PermissionDenied:
		return wireError(403, "forbidden", message)
	case codes.NotFound:
		return wireError(404, "notFound", message)
	case codes.AlreadyExists, codes.Aborted:
		return wireError(409, "conflict", message)
	default:
		return wireError(400, "invalid", "Storage emulator fault: "+message)
	}
}

// configRefusal renders a bucketconfig refusal or gap, or returns nil.
func configRefusal(err error) *gateway.RESTResponse {
	if refusal, ok := errors.AsType[*bucketconfig.RefusalError](err); ok {
		return wireError(refusal.Status, refusal.Reason, refusal.Message)
	}
	if missing, ok := errors.AsType[bucketconfig.GapError](err); ok {
		return wireError(400, "invalid", storageGap+string(missing))
	}
	return nil
}

// unsupported answers every generated Storage route this provider does not serve.
func unsupported(call gateway.Call, _ any) (any, error) {
	return nil, gap(call.Method + " is not implemented")
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
	if code == codes.Unimplemented {
		// A concurrent delete can unindex the bucket between resolveTarget and
		// this check; report it missing, as resolveTarget would now.
		bucket := cmp.Or(target.Parent, target.Name)
		if name, ok := strings.CutPrefix(bucket, BucketResource("")); ok && name != "" {
			if _, found := p.index.Get(bucket); !found {
				return notFound(name, ""), d.ID, nil
			}
		}
		// An unsupported authorization decision fails closed, as a 4xx gap.
		return nil, d.ID, gap(status.Convert(err).Message())
	}
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
		response.Headers.Set(headerContentType, "text/html")
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
	// The message names the resource checked: a listing checks
	// storage.objects.list on the bucket and GCP names the bucket.
	kind := kindBucket
	if strings.Contains(target, "/objects/") {
		kind = "object"
	}
	message := fmt.Sprintf("%s does not have %s access to the Google Cloud Storage %s. Permission '%s' denied on resource '%s' (or it may not exist).", actor, permission, kind, permission, target)
	// Some denials omit the permission and resource sentence (recorded:
	// storage-object-replacement, storage-object-retention "unlocked-set-denied").
	if (call.Method == MethodObjectsInsert && permission == MethodObjectsDelete) || permission == permissionSetRetention {
		message = fmt.Sprintf("%s does not have %s access to the Google Cloud Storage object.", actor, permission)
	}
	return httpCode, reason, message
}

// request carries one Storage call from target resolution through execution.
type request struct {
	call   gateway.Call
	rest   *gateway.RESTRequest
	target iam.Target
	scope  state.Scope
	bucket string
	object string
	upload *uploadRequest
	// xml marks an XML API request; it authorizes as objects.get or
	// objects.insert and answers in the XML API's shapes.
	xml bool
}

func isProjectMethod(method string) bool {
	return method == MethodBucketsInsert || method == MethodBucketsList
}

func isFolderMethod(method string) bool { return strings.HasPrefix(method, "storage.folders.") }

func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	r, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Storage request")
	}
	switch {
	// Python sends chunks with PUT, the Go client with POST; both carry upload_id.
	case call.Method == MethodObjectsInsert && (r.Verb != http.MethodPost || r.Query.Has("upload_id")):
		return p.continueUpload(call, r)
	case call.Method == MethodObjectsRewrite:
		return p.rewrite(call, r)
	case call.Method == MethodXMLGet || call.Method == MethodXMLPut:
		return p.handleRequest(call, r, p.newXMLRequest)
	}
	return p.handleRequest(call, r, p.newRequest)
}

// handleRequest validates, preflights, and serves one single-target request.
func (p *Provider) handleRequest(call gateway.Call, r *gateway.RESTRequest, newRequest func(gateway.Call, *gateway.RESTRequest) (request, *gateway.RESTResponse, error)) (any, error) {
	req, refused, err := newRequest(call, r)
	if refused != nil || err != nil {
		return refused, err
	}
	response, err := p.run(req)
	if req.xml && response != nil {
		response = xmlapi.Result(response)
	}
	return response, err
}

// run preflights one request, then serves it under the backend gate.
func (p *Provider) run(req request) (*gateway.RESTResponse, error) {
	// This preflight permits lazy startup/state resolution only. Every operation
	// reauthorizes under the backend gate before data access or mutation.
	denied, _, err := p.authorize(req.call, req.target, nil, false)
	if denied != nil || err != nil {
		return denied, err
	}
	var response *gateway.RESTResponse
	err = p.manager.Use(req.call.Context, backend.Key{Service: serviceName, Scope: req.scope}, func(session context.Context, driver backend.Backend) error {
		req.call.Context = session //nolint:fatcontext // one request-scoped handoff of the manager session
		var serveErr error
		response, serveErr = p.serve(session, driver, req)
		return serveErr
	})
	if err != nil {
		return response, fmt.Errorf("forward %s: %w", req.call.Method, err)
	}
	return response, nil
}

// newRequest validates the REST input and resolves the IAM target and backend
// scope. A refusal is a wire response; an error is a gateway status.
func (p *Provider) newRequest(call gateway.Call, r *gateway.RESTRequest) (request, *gateway.RESTResponse, error) {
	if err := validateQuery(call.Method, r); err != nil {
		return request{}, nil, err
	}
	req := request{call: call, rest: r, bucket: r.Path[keyBucket], object: r.Path["object"]}
	if call.Method == MethodObjectsInsert {
		upload, err := parseUpload(r)
		if err != nil {
			return request{}, nil, err
		}
		req.upload, req.object = upload, upload.name
	}
	return p.resolveRequest(req)
}

// resolveRequest fills the IAM target and backend scope of a validated request.
func (p *Provider) resolveRequest(req request) (request, *gateway.RESTResponse, error) {
	target, scope, refused, err := p.resolveTarget(req.call, req.bucket, req.object)
	if refused != nil || err != nil {
		return request{}, refused, err
	}
	req.target, req.scope = target, scope
	req.call.Resource = target.Name
	req.call.Location = state.Global
	return req, nil, nil
}

// serve runs one request under the backend gate: it rechecks bucket ownership,
// resolves object features, authorizes completely, and only then executes.
func (p *Provider) serve(ctx context.Context, driver backend.Backend, req request) (*gateway.RESTResponse, error) {
	b, err := backendOf(driver)
	if err != nil {
		return nil, err
	}
	if !isProjectMethod(req.call.Method) {
		if resource, found := p.index.Get(BucketResource(req.bucket)); !found || resource.Scope != req.scope {
			return notFound(req.bucket, ""), nil
		}
	}
	features, current, err := p.requestFeatures(ctx, b, req)
	if err != nil {
		return nil, err
	}
	denied, id, err := p.authorize(req.call, req.target, features, true)
	if err != nil {
		return nil, err
	}
	if denied != nil {
		return denied, nil
	}
	response, err := p.execute(ctx, b, req, current)
	if err == nil {
		p.decorate(b, req.scope, req.bucket, response)
	}
	finishResponse(response, id, req.rest.Host)
	return response, err
}

// requestFeatures resolves the request and state features of a request's
// permission plan, and the existing object an insert or restore replaces.
func (p *Provider) requestFeatures(ctx context.Context, b *memoryBackend, req request) (map[string]bool, map[string]any, error) {
	features, current, err := p.objectFeatures(ctx, b, req.call, req.bucket, req.object)
	if err != nil || req.call.Method != MethodObjectsPatch {
		return features, current, err
	}
	return features, current, p.patchFeatures(req.rest, features)
}

// patchFeatures resolves objects.patch's retention features. A retention value
// is validated here, after the baseline preflight and before the complete
// authorization, as GCP refuses an invalid one before checking
// storage.objects.setRetention (recorded: storage-object-retention
// "mode-unknown" for a caller without it). The rest of the body is decoded
// after authorization.
func (p *Provider) patchFeatures(r *gateway.RESTRequest, features map[string]bool) error {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(r.JSON, &fields)
	raw, named := fields[keyRetention]
	set := named && string(bytes.TrimSpace(raw)) != jsonNull
	if set {
		if _, err := bucketconfig.ParseRetention(raw, p.now()); err != nil {
			return err //nolint:wrapcheck // GCP's refusal, rendered as is
		}
	}
	override, _ := booleanOption(r.Query, queryOverrideRetention)
	features[featureRetentionSet], features[featureRetentionOverride] = set, override
	return nil
}

// finishResponse attaches the decision ID and publishes resource links.
func finishResponse(response *gateway.RESTResponse, decision, host string) {
	if response == nil {
		return
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision)
	if document, ok := response.JSON.(map[string]any); ok && response.Status < 300 {
		publishLinks(document, "http://"+host)
	}
}

// publishLinks sets selfLink and mediaLink on every bucket and object in a
// response, at the emulator's public origin. GCP returns both, and clients
// such as rclone download from mediaLink instead of deriving a URL.
func publishLinks(value map[string]any, origin string) {
	bucket, _ := value[keyBucket].(string)
	name, _ := value[keyName].(string)
	switch value[keyKind] {
	case kindObjectDocument:
		self := "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(name)
		value["selfLink"] = origin + self
		generation, _ := value[keyGeneration].(string)
		// GCP orders the query generation first (storage-rclone-object-requests).
		value["mediaLink"] = origin + "/download" + self + "?" + keyGeneration + "=" + url.QueryEscape(generation) + "&" + queryAlt + "=" + altMedia
	case kindBucketDocument:
		value["selfLink"] = origin + "/storage/v1/b/" + url.PathEscape(name)
	}
	for _, key := range []string{"items", keyResource} {
		for _, document := range nestedDocuments(value[key]) {
			publishLinks(document, origin)
		}
	}
}

// nestedDocuments returns the documents a response field holds: a backend
// list ([]any), a list the provider pages itself ([]map[string]any, from
// provider.ListBody), or one document.
func nestedDocuments(value any) []map[string]any {
	switch nested := value.(type) {
	case []any:
		return itemList(nested)
	case []map[string]any:
		return nested
	case map[string]any:
		return []map[string]any{nested}
	}
	return nil
}

// resolveTarget derives the IAM target and backend scope for one request. A
// project-scoped method addresses the project; every other method addresses the
// bucket, whose recorded scope decides the backend instance. An unknown bucket
// is refused with a notFound response.
func (p *Provider) resolveTarget(call gateway.Call, bucket, object string) (iam.Target, state.Scope, *gateway.RESTResponse, error) {
	if isProjectMethod(call.Method) {
		return p.projectTarget(call.Project)
	}
	target := iam.Target{Name: BucketResource(bucket)}
	resource, found := p.index.Get(target.Name)
	if !found {
		return iam.Target{}, state.Scope{}, notFound(bucket, ""), nil
	}
	if isFolderMethod(call.Method) {
		return target, resource.Scope, nil, nil
	}
	// IAM condition attributes use Storage's documented resource names.
	target.Attributes = iam.ResourceAttributes{Name: "projects/_/buckets/" + bucket,
		Type: "storage.googleapis.com/Bucket", Service: "storage.googleapis.com"}
	// Enforced public access prevention overrides public grants on the
	// bucket and its objects (storage/docs/public-access-prevention).
	target.PublicPrevented = p.bucketSettings(resource.Scope, bucket).public == publicEnforced
	if object != "" {
		target.Parent = target.Name
		target.Name += "/objects/" + object
		target.Attributes.Name += "/objects/" + object
		target.Attributes.Type = "storage.googleapis.com/Object"
	}
	return target, resource.Scope, nil, nil
}

// projectTarget resolves a project-scoped method, which needs the explicit
// project and its numeric project number.
func (p *Provider) projectTarget(project string) (iam.Target, state.Scope, *gateway.RESTResponse, error) {
	name := provider.ProjectResource(project)
	ref, ok := p.index.Get(name)
	if !ok || ref.Kind != kindProjectRef || !numericProjectNumber.MatchString(ref.Number) {
		return iam.Target{}, state.Scope{}, nil, gap("configure the explicit project and numeric project number")
	}
	return iam.Target{Name: name}, state.Scope{Project: project, Location: state.Global}, nil, nil
}

// objectFeatures resolves whether an inserted object already exists, which the
// object-exists permission plan depends on.
func (p *Provider) objectFeatures(ctx context.Context, b *memoryBackend, call gateway.Call, bucket, object string) (map[string]bool, map[string]any, error) {
	features := map[string]bool{}
	if call.Method != MethodObjectsInsert && call.Method != MethodObjectsRestore {
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
		return nil, nil, fault("Unable to resolve existing object before authorization")
	}
}

// kindProjectRef is the index kind that marks a resource-manager project.
const kindProjectRef = "project"

// numericProjectNumber pins the project-number shape Storage requires.
var numericProjectNumber = regexp.MustCompile(`^[0-9]{1,20}$`)

func objectPath(bucket, object string) string {
	return "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(object)
}

// decodeBackendJSON re-materializes an upstream JSON payload as the response
// body, stripping the upstream's pagination links and quoting its etag.
func (b *memoryBackend) decodeBackendJSON(payload []byte, response *gateway.RESTResponse) {
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
	b.removeBackendLinks(value)
	// Real GCS reports rpo only for multi-region and dual-region buckets;
	// the recorded single-region snapshot carries rpo null. The fake backend
	// echoes the create body, so null the field here: the Terraform provider
	// treats an explicit null as unset, while an absent key coerces to "".
	// The multi-region shape is an explicit gap until it has its own recording.
	// Objects and lists have no rpo.
	if value[keyKind] == kindBucketDocument {
		value["rpo"] = nil
	}
	if etag, ok := value["etag"].(string); ok {
		response.Headers.Set("ETag", `"`+etag+`"`)
	}
}

// maxBackendPayload bounds a backend response: the largest object plus metadata.
const maxBackendPayload = maxObjectSize + (1 << 20)

// forward sends only a provider-selected route over upstream's memory
// transport. A caller URL, authorization header, proxy, or notification
// endpoint never enters it.
func (b *memoryBackend) forward(ctx context.Context, method, path string, query url.Values, headers http.Header, body []byte) (*gateway.RESTResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://storage.invalid"+path+"?"+query.Encode(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build backend request: %w", err)
	}
	for _, key := range []string{headerContentType, "Range", "Accept-Encoding"} {
		if value := headers.Get(key); value != "" {
			req.Header.Set(key, value)
		}
	}
	result, err := b.request(req)
	if err != nil {
		return nil, fault("Storage backend request failed")
	}
	payload, err := readBackendPayload(result, method+" "+path)
	if err != nil {
		return nil, err
	}
	response := &gateway.RESTResponse{Status: result.StatusCode, Headers: result.Header.Clone()}
	response.Headers.Del("Content-Length")
	if strings.Contains(result.Header.Get(headerContentType), contentJSON) && (result.StatusCode >= 300 || query.Get(queryAlt) != altMedia) {
		b.decodeBackendJSON(payload, response)
		return response, nil
	}
	response.Body = bytes.NewReader(payload)
	// Media is read whole, so its length is known; the Go client checks it.
	response.Headers.Set("Content-Length", strconv.Itoa(len(payload)))
	if generation := response.Headers.Get("X-Goog-Generation"); generation != "" {
		bucket, name := objectOfPath(path)
		metageneration := b.metageneration(bucket, name, generation)
		response.Headers.Set("ETag", `"`+objectETag(generation, metageneration)+`"`)
		response.Headers.Set("X-Goog-Metageneration", metageneration)
	}
	return response, nil
}

// objectOfPath reads the bucket and object of a backend object path built by
// objectPath.
func objectOfPath(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, "/storage/v1/b/")
	bucket, object, _ := strings.Cut(rest, "/o/")
	bucket, _ = url.PathUnescape(bucket)
	object, _ = url.PathUnescape(object)
	return bucket, object
}

// readBackendPayload reads and closes the backend's answer to request,
// refusing a body over maxBackendPayload. The provider validates requests
// before forwarding them, so a backend 5xx is an emulator fault, not a
// transient failure; relaying it would make clients retry forever.
func readBackendPayload(result *http.Response, request string) ([]byte, error) {
	defer result.Body.Close()
	if result.StatusCode >= http.StatusInternalServerError {
		return nil, fault(fmt.Sprintf("Storage backend answered %s with HTTP %d", request, result.StatusCode))
	}
	payload, err := io.ReadAll(io.LimitReader(result.Body, maxBackendPayload+1))
	if err != nil {
		return nil, fmt.Errorf("read backend response: %w", err)
	}
	if len(payload) > maxBackendPayload {
		return nil, fault("Storage backend response exceeds the supported size")
	}
	return payload, nil
}

// Upstream links use its private origin; serve republishes them at the
// emulator's origin with publishLinks.
func (b *memoryBackend) removeBackendLinks(value map[string]any) {
	delete(value, "mediaLink")
	delete(value, "selfLink")
	delete(value, "acl")
	delete(value, "defaultObjectAcl")
	if generation, ok := value[keyGeneration].(string); ok {
		if value[keyKind] == kindObjectDocument {
			bucket, _ := value[keyBucket].(string)
			name, _ := value[keyName].(string)
			value["metageneration"] = b.metageneration(bucket, name, generation)
			b.overlay(bucket, name, generation).render(value)
		}
		metageneration, _ := value["metageneration"].(string)
		value["etag"] = objectETag(generation, metageneration)
	}
	if items, ok := value["items"].([]any); ok {
		for _, item := range items {
			if object, ok := item.(map[string]any); ok {
				b.removeBackendLinks(object)
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

func (p *Provider) execute(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	switch {
	case isFolderMethod(req.call.Method):
		return p.executeFolder(req.call.Method, req.rest, req.scope, req.bucket)
	case strings.HasPrefix(req.call.Method, "storage.buckets."):
		return p.executeBucket(ctx, b, req)
	}
	return p.executeObject(ctx, b, req, current)
}

// executeBucket serves the bucket methods.
func (p *Provider) executeBucket(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	path := "/storage/v1/b/" + url.PathEscape(req.bucket)
	switch req.call.Method {
	case MethodBucketsInsert:
		return p.createBucket(ctx, b, req.rest, req.scope)
	case MethodBucketsPatch:
		return p.patchBucket(ctx, b, req.rest, req.scope, req.bucket)
	case MethodBucketsLock:
		return p.lockRetention(ctx, b, req.rest, req.scope, req.bucket)
	case MethodBucketsGetIamPolicy:
		return p.readBucketPolicy(req.bucket)
	case MethodBucketsSetIamPolicy:
		return p.writeBucketPolicy(req.scope, req.bucket, req.rest)
	case MethodBucketsList:
		return p.listBuckets(ctx, b, req.rest, req.scope)
	case MethodBucketsGet, MethodBucketsGetLayout:
		return p.readBucket(ctx, b, req, path)
	}
	return p.deleteBucket(ctx, b, req.rest, req.scope, path, req.bucket)
}

// executeObject serves the object methods, including the XML API's and a
// resumable initiation.
func (p *Provider) executeObject(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	softDeleted, _ := booleanOption(req.rest.Query, querySoftDeleted)
	switch req.call.Method {
	case MethodObjectsPatch:
		return p.patchObject(ctx, b, req)
	case MethodObjectsRestore:
		return p.restoreObject(ctx, b, req, current)
	case MethodObjectsList:
		return p.listObjects(ctx, b, req, softDeleted)
	case MethodObjectsGet:
		switch {
		case req.xml:
			return xmlGet(ctx, b, req)
		case softDeleted:
			return p.getSoftDeleted(req)
		}
		return p.readOrDeleteObject(ctx, b, req)
	case MethodObjectsInsert:
		if req.upload.resumable && !req.xml {
			return p.startUpload(req, current)
		}
	}
	return p.writeObject(ctx, b, req, current)
}

// writeObject serves inserts (JSON, XML, and a resumable commit) and deletes.
// Each may remove a generation, which a bucket with a soft-delete policy
// keeps: the live one an overwrite or delete replaces, unless versioning
// keeps it as noncurrent, or the generation a delete names. An insert's
// preconditions and the replaced generation's protection are checked first.
func (p *Provider) writeObject(ctx context.Context, b *memoryBackend, req request, current map[string]any) (*gateway.RESTResponse, error) {
	insert := req.call.Method == MethodObjectsInsert
	if insert {
		if response, err := p.replaceable(b, req.scope, req.bucket, req.rest, current); response != nil || err != nil {
			return response, err
		}
	}
	keep, err := p.retainWrite(ctx, b, req)
	if err != nil {
		return nil, err
	}
	var response *gateway.RESTResponse
	if insert {
		response, err = b.forward(ctx, "POST", "/upload/storage/v1/b/"+url.PathEscape(req.bucket)+"/o", req.upload.query, req.upload.headers, req.upload.body)
	} else {
		response, err = p.readOrDeleteObject(ctx, b, req)
	}
	if err != nil {
		return response, err
	}
	if err := keep(response); err != nil {
		return nil, err
	}
	return p.written(b, req, response), nil
}

// replaceable checks an insert's preconditions against the live object it
// replaces, then that object's holds and retention.
func (p *Provider) replaceable(b *memoryBackend, scope state.Scope, bucket string, r *gateway.RESTRequest, current map[string]any) (*gateway.RESTResponse, error) {
	if response, err := preconditions(r, current, MethodObjectsInsert); response != nil || err != nil {
		return response, err
	}
	return nil, p.refuseRemoval(b, scope, bucket, current)
}

// retainWrite plans the soft-delete copy of the generation an insert,
// restore, or delete removes for good: the one a delete names, or the live
// one unless a versioned bucket keeps it as noncurrent.
func (p *Provider) retainWrite(ctx context.Context, b *memoryBackend, req request) (func(*gateway.RESTResponse) error, error) {
	versioned, err := b.versioned(ctx, req.bucket)
	if err != nil {
		return nil, err
	}
	generation := ""
	if req.call.Method == MethodObjectsDelete {
		generation = req.rest.Query.Get(keyGeneration)
	}
	return p.retainRemoved(ctx, b, req.scope, req.bucket, req.object, generation, generation != "" || !versioned)
}

// written finishes a successful insert: the new generation takes the
// bucket's default hold, and an XML API insert answers with its empty body
// and object headers.
func (p *Provider) written(b *memoryBackend, req request, response *gateway.RESTResponse) *gateway.RESTResponse {
	if req.call.Method != MethodObjectsInsert || response.Status != http.StatusOK {
		return response
	}
	object := createdObject(response)
	p.created(b, req.scope, req.bucket, object)
	if req.xml && object != nil {
		return xmlapi.Stored(object)
	}
	return response
}

// readOrDeleteObject serves object get and delete. Both address the live
// generation, or the generation the request names, live or noncurrent, and
// evaluate the preconditions against it (recorded: storage-object-versioning
// "noncurrent-read-precondition", "noncurrent-delete-precondition"). A delete
// is refused while a hold or retention protects the generation.
func (p *Provider) readOrDeleteObject(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	generation := req.rest.Query.Get(keyGeneration)
	current, refused, err := currentObject(ctx, b, req.bucket, req.object, generation)
	if refused != nil || err != nil {
		return refused, err
	}
	if response, err := preconditions(req.rest, current, req.call.Method); response != nil || err != nil {
		return response, err
	}
	if req.call.Method == MethodObjectsGet {
		return b.forward(ctx, "GET", objectPath(req.bucket, req.object), req.rest.Query, req.rest.Headers, nil)
	}
	if err := p.refuseRemoval(b, req.scope, req.bucket, current); err != nil {
		return nil, err
	}
	response, err := b.forward(ctx, "DELETE", objectPath(req.bucket, req.object), req.rest.Query, nil, nil)
	if err != nil {
		return response, err
	}
	noContentOnSuccess(response)
	if removed, _ := current[keyGeneration].(string); response.Status == http.StatusNoContent {
		return response, b.prune(ctx, req.bucket, req.object, removed)
	}
	return response, nil
}

// currentObject reads the metadata of an object's live generation, or of the
// named generation, live or noncurrent. A missing one becomes a notFound
// refusal and any other non-200 backend response is returned as is.
func currentObject(ctx context.Context, b *memoryBackend, bucket, object, generation string) (map[string]any, *gateway.RESTResponse, error) {
	var query url.Values
	if generation != "" {
		if !positiveInteger(generation) {
			return nil, notFound(bucket, object), nil
		}
		query = url.Values{keyGeneration: {generation}}
	}
	response, err := b.forward(ctx, "GET", objectPath(bucket, object), query, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	switch response.Status {
	case 200:
	case 404:
		return nil, notFound(bucket, object), nil
	default:
		return nil, response, nil
	}
	body, err := jsonObject(response.JSON)
	if err != nil {
		return nil, nil, err
	}
	return body, nil, nil
}

// positiveInteger reports whether text is a generation number.
func positiveInteger(text string) bool {
	value, err := strconv.ParseInt(text, 10, 64)
	return err == nil && value > 0
}

// noContentOnSuccess turns the backend's 200 delete reply into Storage's empty 204.
func noContentOnSuccess(response *gateway.RESTResponse) {
	if response.Status == 200 {
		response.Status = 204
		response.JSON = nil
		response.Body = nil
	}
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
func (p *Provider) writeBucketPolicy(scope state.Scope, bucket string, r *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	request, err := decodeBucketPolicy(bucket, r.JSON)
	if err != nil {
		return nil, err
	}
	// Enforced prevention refuses public members (recorded:
	// storage-public-access-enforced). Inherited prevention follows the
	// storage.publicAccessPrevention organization policy; the emulator models
	// no organization policies, so, as for a hierarchy that sets none, it does
	// not apply and public members are accepted and grant access.
	if p.bucketSettings(scope, bucket).public != publicInherited && policyBindsPublic(request.Bindings) {
		return wireError(412, "conditionNotMet", "The member bindings allUsers and allAuthenticatedUsers are not allowed since public access prevention is enforced."), nil
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
// authenticated users, which enforced public access prevention forbids.
func policyBindsPublic(bindings []iam.Binding) bool {
	for _, binding := range bindings {
		for _, member := range binding.Members {
			if member == "allUsers" || member == "allAuthenticatedUsers" {
				return true
			}
		}
	}
	return false
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

// foldersAsPrefixes accepts includeFoldersAsPrefixes=true, which adds the
// bucket's folders and managed folders to a "/"-delimited listing's prefixes
// (objects/list). Managed folders are not modeled, so none exist; a bucket
// without folders lists the same either way. A bucket with folders refuses
// it rather than omit them.
func (p *Provider) foldersAsPrefixes(req request) error {
	include, _ := booleanOption(req.rest.Query, queryFoldersAsPrefixes)
	req.rest.Query.Del(queryFoldersAsPrefixes)
	if !include || req.rest.Query.Get("delimiter") != "/" {
		return nil
	}
	folders, err := p.catalog.Folders(req.scope, req.bucket)
	if err != nil {
		return err
	}
	if len(folders) > 0 {
		return gap("includeFoldersAsPrefixes in a bucket with folders")
	}
	return nil
}

// readBucket serves buckets.get and buckets.getStorageLayout.
func (p *Provider) readBucket(ctx context.Context, b *memoryBackend, req request, path string) (*gateway.RESTResponse, error) {
	if req.call.Method == MethodBucketsGetLayout {
		return p.storageLayout(ctx, b, req.rest, req.scope, path)
	}
	return p.getBucket(ctx, b, req.rest, req.scope, path)
}

// storageLayout serves buckets.getStorageLayout. gcloud reads it before
// object commands: locationType "zone" selects its zonal-bucket client, and
// hierarchicalNamespace its folder handling. Buckets here have neither, and
// GCP omits hierarchicalNamespace when it is off (recorded:
// storage-gcloud-object-requests "storage-layout").
func (p *Provider) storageLayout(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, path string) (*gateway.RESTResponse, error) {
	response, err := p.getBucket(ctx, b, r, scope, path)
	if err != nil || response.Status != 200 {
		return response, err
	}
	bucket, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		keyKind: "storage#storageLayout", keyBucket: bucket[keyName], "location": bucket["location"],
		"locationType": bucket["locationType"],
	}}, nil
}

// deleteBucket deletes one bucket, then releases its catalog and IAM state.
// Live and noncurrent generations both keep a bucket from deleting; the
// backend counts only live ones, so the provider checks (recorded:
// storage-object-versioning "bucket-delete-with-noncurrent").
func (p *Provider) deleteBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, path, bucket string) (*gateway.RESTResponse, error) {
	listed, err := b.forward(ctx, http.MethodGet, path+"/o", url.Values{queryVersions: {"true"}, queryMaxResults: {"1"}}, nil, nil)
	if err != nil || listed.Status != http.StatusOK {
		return listed, err
	}
	if body, err := jsonObject(listed.JSON); err != nil || len(itemList(body["items"])) > 0 {
		return wireError(409, "conflict", "The bucket you tried to delete is not empty."), err
	}
	response, err := b.forward(ctx, "DELETE", path, r.Query, nil, nil)
	if err != nil {
		return response, err
	}
	noContentOnSuccess(response)
	if response.Status == 204 {
		b.forgetBucket(bucket)
		// Unindexing the bucket also drops its policy.
		err = p.catalog.ForgetBucket(scope, bucket)
	}
	return response, err
}

// executeFolder serves the Storage Control folders surface. Folders are
// provider-owned because the storage backend does not model them; state lives
// in the catalog and clears with the scope.
func (p *Provider) executeFolder(method string, r *gateway.RESTRequest, scope state.Scope, bucket string) (*gateway.RESTResponse, error) {
	switch method {
	case MethodFoldersCreate, MethodFoldersInsert:
		return p.createFolder(r, scope, bucket)
	case MethodFoldersGet:
		return p.getFolder(scope, bucket, r.Path["folder"])
	case MethodFoldersList:
		return p.listFolders(r, scope, bucket)
	default:
		return p.deleteFolder(scope, bucket, r.Path["folder"])
	}
}

// folderDocument is the wire shape of one folder (folders resource).
func folderDocument(folder Folder) map[string]any {
	return map[string]any{keyKind: "storage#folder", keyName: folder.Name, keyBucket: folder.Bucket}
}

func folderMissing(id string) *gateway.RESTResponse {
	return wireError(404, "notFound", "No such object: folders/"+id)
}

// folderRequest is a folders.insert body. The folders resource's other
// fields are output-only.
type folderRequest struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

func (p *Provider) createFolder(r *gateway.RESTRequest, scope state.Scope, bucket string) (*gateway.RESTResponse, error) {
	var body folderRequest
	if err := provider.DecodeJSON(r.JSON, "", &body, gap, provider.Fields{OutputOnly: []string{
		"createTime", "id", keyKind, "metageneration", "pendingRenameInfo", "selfLink", "updateTime",
	}}); err != nil {
		return nil, err
	}
	if body.Name == "" {
		return nil, provider.Invalid("Expected a folder name")
	}
	if body.Bucket != "" && body.Bucket != bucket {
		return nil, provider.Invalid("Folder bucket does not match the request")
	}
	if _, exists, err := p.catalog.Folder(scope, bucket, body.Name); err != nil {
		return nil, err
	} else if exists {
		return wireError(409, "conflict", "Folder already exists"), nil
	}
	folder := Folder{Bucket: bucket, Name: body.Name}
	if err := p.catalog.PublishFolder(scope, folder); err != nil {
		return nil, fmt.Errorf("publish folder: %w", err)
	}
	return &gateway.RESTResponse{JSON: folderDocument(folder)}, nil
}

func (p *Provider) getFolder(scope state.Scope, bucket, id string) (*gateway.RESTResponse, error) {
	folder, found, err := p.catalog.Folder(scope, bucket, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return folderMissing(id), nil
	}
	return &gateway.RESTResponse{JSON: folderDocument(folder)}, nil
}

// listFolders serves folders.list in name order, paged as the other Storage
// lists are; pageSize is the folders resource's page size.
func (p *Provider) listFolders(r *gateway.RESTRequest, scope state.Scope, bucket string) (*gateway.RESTResponse, error) {
	limit, refused := listSize(r.Query.Get("pageSize"), maxListResults)
	if refused != nil {
		return refused, nil
	}
	after, valid := listAfter(folderPages, r.Query.Get(queryPageToken))
	if !valid {
		return wireError(400, "invalid", "Next page token not valid: "+r.Query.Get(queryPageToken)), nil
	}
	folders, err := p.catalog.Folders(scope, bucket)
	if err != nil {
		return nil, err
	}
	folders = slices.DeleteFunc(folders, func(folder Folder) bool {
		return !strings.HasPrefix(folder.Name, r.Query.Get(queryPrefix))
	})
	page, next := pageAfter(folders, func(folder Folder) string { return folder.Name }, after, limit, folderPages)
	documents := make([]map[string]any, 0, len(page))
	for _, folder := range page {
		documents = append(documents, folderDocument(folder))
	}
	result := provider.ListBody("items", documents, next)
	result[keyKind] = "storage#folders"
	return &gateway.RESTResponse{JSON: result}, nil
}

func (p *Provider) deleteFolder(scope state.Scope, bucket, id string) (*gateway.RESTResponse, error) {
	removed, err := p.catalog.DeleteFolder(scope, bucket, id)
	if err != nil {
		return nil, err
	}
	if !removed {
		return folderMissing(id), nil
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// RegisterPermissions describes the initial UBLA permission plans. Registering
// these rules does not register API providers or claim service conformance.
func RegisterPermissions(engine *iam.Engine) error {
	rules := map[string]iam.Rule{
		MethodFoldersInsert:       {Baseline: []string{MethodFoldersCreate}},
		MethodFoldersCreate:       {Baseline: []string{MethodFoldersCreate}},
		MethodFoldersGet:          {Baseline: []string{"storage.folders.get"}},
		MethodFoldersList:         {Baseline: []string{"storage.folders.list"}},
		MethodFoldersDelete:       {Baseline: []string{"storage.folders.delete"}},
		MethodBucketsList:         {Baseline: []string{MethodBucketsList}},
		MethodBucketsInsert:       {Baseline: []string{permissionBucketsCreate}},
		MethodBucketsGet:          {Baseline: []string{MethodBucketsGet}},
		MethodBucketsDelete:       {Baseline: []string{MethodBucketsDelete}},
		MethodBucketsPatch:        {Baseline: []string{permissionBucketsUpdate}},
		MethodBucketsGetIamPolicy: {Baseline: []string{MethodBucketsGetIamPolicy}},
		MethodBucketsSetIamPolicy: {Baseline: []string{MethodBucketsSetIamPolicy}},
		// buckets/lockRetentionPolicy needs storage.buckets.update; GCP's
		// denial names storage.buckets.get first (recorded: storage-bucket-lock
		// "lock-denied").
		MethodBucketsLock: {Baseline: []string{permissionBucketsGet, permissionBucketsUpdate}},
		// buckets/getStorageLayout requires storage.objects.list.
		MethodBucketsGetLayout: {Baseline: []string{permissionObjectsList}},
		MethodObjectsList:      {Baseline: []string{MethodObjectsList}},
		MethodObjectsGet:       {Baseline: []string{MethodObjectsGet}},
		MethodObjectsDelete:    {Baseline: []string{MethodObjectsDelete}},
		// objects/patch: setting retention needs setRetention, and
		// overrideUnlockedRetention=true needs that permission, whatever the
		// object's retention (recorded: storage-object-retention
		// "unlocked-set-denied", "unlocked-overridden-denied").
		MethodObjectsPatch: {Baseline: []string{permissionObjectsUpdate}, When: map[string][]string{
			featureRetentionSet: {permissionSetRetention}, featureRetentionOverride: {permissionOverrideRetention}}},
		MethodObjectsInsert: {Baseline: []string{"storage.objects.create"}, When: map[string][]string{objectExistsCondition: {MethodObjectsDelete}}},
		// objects/restore: restore and create, plus delete when a live object
		// is overwritten. The recorded denial names storage.objects.restore
		// first (storage-bucket-defaults "restore-denied").
		MethodObjectsRestore: {Baseline: []string{permissionObjectsRestore, permissionObjectsCreate}, When: map[string][]string{objectExistsCondition: {MethodObjectsDelete}}},
	}
	for method, rule := range rules {
		if err := engine.Register(method, rule); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// serviceName is the registered service key for Storage backends and index entries.
const serviceName = "storage"

// Storage JSON API method names. Naming them keeps the dispatch table,
// validation, and permission rules from drifting apart; a typo in any one
// place used to disable a method silently.
const (
	MethodBucketsInsert       = "storage.buckets.insert"
	MethodBucketsList         = "storage.buckets.list"
	MethodBucketsGet          = "storage.buckets.get"
	MethodBucketsPatch        = "storage.buckets.patch"
	MethodBucketsDelete       = "storage.buckets.delete"
	MethodBucketsGetIamPolicy = "storage.buckets.getIamPolicy"
	MethodBucketsSetIamPolicy = "storage.buckets.setIamPolicy"
	MethodBucketsGetLayout    = "storage.buckets.getStorageLayout"
	MethodBucketsLock         = "storage.buckets.lockRetentionPolicy"
	MethodFoldersCreate       = "storage.folders.create"
	MethodFoldersInsert       = "storage.folders.insert"
	MethodFoldersGet          = "storage.folders.get"
	MethodFoldersList         = "storage.folders.list"
	MethodFoldersDelete       = "storage.folders.delete"
	MethodObjectsInsert       = "storage.objects.insert"
	MethodObjectsList         = "storage.objects.list"
	MethodObjectsGet          = "storage.objects.get"
	MethodObjectsDelete       = "storage.objects.delete"
	MethodObjectsRewrite      = "storage.objects.rewrite"
	MethodObjectsPatch        = "storage.objects.patch"
	MethodObjectsRestore      = "storage.objects.restore"
	// MethodXMLGet and MethodXMLPut are the XML API object methods; they
	// authorize as objects.get and objects.insert.
	MethodXMLGet = "storage.xml.objects.get"
	MethodXMLPut = "storage.xml.objects.put"
)

// Permission names used by the UBLA baseline plans.
const (
	permissionBucketsCreate       = "storage.buckets.create"
	permissionBucketsDelete       = "storage.buckets.delete"
	permissionBucketsGet          = "storage.buckets.get"
	permissionBucketsGetIamPolicy = "storage.buckets.getIamPolicy"
	permissionBucketsList         = "storage.buckets.list"
	permissionBucketsSetIamPolicy = "storage.buckets.setIamPolicy"
	permissionBucketsUpdate       = "storage.buckets.update"
	permissionObjectsCreate       = "storage.objects.create"
	permissionObjectsDelete       = "storage.objects.delete"
	permissionObjectsGet          = "storage.objects.get"
	permissionObjectsList         = "storage.objects.list"
	permissionObjectsRestore      = "storage.objects.restore"
	permissionObjectsUpdate       = "storage.objects.update"
	permissionSetRetention        = "storage.objects.setRetention"
	permissionOverrideRetention   = "storage.objects.overrideUnlockedRetention"
)

// Query and payload literals that repeat across validation and dispatch.
const (
	objectExistsCondition = "object_exists"
	// featureRetentionSet and featureRetentionOverride are objects.patch's
	// request features: a non-null retention and overrideUnlockedRetention=true.
	featureRetentionSet      = "retention_set"
	featureRetentionOverride = "retention_override"
	kindBucket               = "bucket"
	altMedia                 = "media"
	uploadResumable          = "resumable"
	keyBucket                = "bucket"
	keyGeneration            = "generation"
	keyCacheControl          = "cacheControl"
	keyDisposition           = "contentDisposition"
	keyContentType           = "contentType"
	keyLanguage              = "contentLanguage"
	keyMetadata              = "metadata"
	queryAlt                 = "alt"
	queryUploadType          = "uploadType"
	querySoftDeleted         = "softDeleted"
	queryMaxResults          = "maxResults"
	queryPrefix              = "prefix"
	queryVersions            = "versions"
	keyKind                  = "kind"
	// queryFoldersAsPrefixes is objects.list includeFoldersAsPrefixes, which
	// gcloud storage ls sends as True.
	queryFoldersAsPrefixes = "includeFoldersAsPrefixes"
	valueFalse             = "false"
	headerContentType      = "Content-Type"
	headerPayloadHash      = "x-goog-content-sha256"
	storageClassStandard   = "STANDARD"
	// keyName is the name key shared by query options, upload metadata, and
	// bucket/folder documents.
	keyName           = "name"
	keyEnabled        = "enabled"
	keyTemporaryHold  = "temporaryHold"
	keyEventBasedHold = "eventBasedHold"
	keyRetention      = "retention"
	// queryOverrideRetention is objects.patch overrideUnlockedRetention.
	queryOverrideRetention = "overrideUnlockedRetention"
	// queryObjectRetention is buckets.insert enableObjectRetention.
	queryObjectRetention = "enableObjectRetention"
	keyDefaultHold       = "defaultEventBasedHold"
	kindBucketDocument   = "storage#bucket"
	kindObjectDocument   = "storage#object"
	contentJSON          = "application/json"
	jsonNull             = "null"
	keyResource          = "resource"
)
