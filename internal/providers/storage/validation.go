package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxObjectSize bounds one object. State is in memory, and each upload is
// buffered whole before it reaches the backend, so the limit is a memory bound
// rather than a GCP one (GCP allows 5 TiB). It is above the Python client's
// 100 MiB default resumable chunk.
const maxObjectSize = 128 << 20

type uploadRequest struct {
	name    string
	query   url.Values
	headers http.Header
	body    []byte
	// resumable marks an initiation, whose media arrives in later requests;
	// metadata is its object metadata and total is -1 until declared.
	resumable bool
	metadata  map[string]json.RawMessage
	total     int64
}

// Query parameter names that appear in more than one option list.
const (
	queryProject              = "project"
	queryIfGenerationMatch    = "ifGenerationMatch"
	queryIfGenerationNotMatch = "ifGenerationNotMatch"
	queryIfMetaMatch          = "ifMetagenerationMatch"
	queryIfMetaNotMatch       = "ifMetagenerationNotMatch"
)

// queryOptions lists the documented query parameters each method may carry.
var queryOptions = map[string][]string{
	MethodBucketsInsert:       {queryProject},
	MethodBucketsList:         {queryProject, "prefix", "maxResults", "pageToken"},
	MethodBucketsGetIamPolicy: {"optionsRequestedPolicyVersion"},
	MethodBucketsPatch:        {queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsInsert:       {queryUploadType, keyName, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsGet:          {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsDelete:       {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	// Rewrite preconditions apply to the destination. maxBytesRewrittenPerCall
	// is accepted and not enforced: every rewrite here finishes in one call.
	MethodObjectsRewrite: {"sourceGeneration", "maxBytesRewrittenPerCall", queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsList:    {"prefix", "delimiter", "maxResults", "pageToken", "startOffset", "endOffset", "versions"},
}

// defaultOnlyOptions are options accepted only at their documented default,
// which is the behavior served; any other value is a gap. The Go client sends
// them on every call (storage/docs/json_api/v1/objects/list, buckets/list).
var defaultOnlyOptions = map[string]map[string]string{
	MethodBucketsList: {"returnPartialSuccess": valueFalse},
	MethodObjectsList: {"matchGlob": "", "includeTrailingDelimiter": valueFalse, "includeFoldersAsPrefixes": valueFalse, "softDeleted": valueFalse},
	MethodObjectsGet:  {"softDeleted": valueFalse},
}

// universalQueryOptions apply to every method.
var universalQueryOptions = []string{"prettyPrint", "fields", queryAlt, "projection"}

func validateQuery(method string, r *gateway.RESTRequest) error {
	for key, values := range r.Query {
		if err := validateOption(method, key, values); err != nil {
			return err
		}
	}
	if err := validateQueryValues(method, r.Query); err != nil {
		return err
	}
	for key := range r.Headers {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-goog-encryption") || lower == "x-goog-acl" {
			return gap("encryption keys and ACLs")
		}
	}
	return nil
}

// validateOption accepts one query option the method documents, or a
// default-only option at its default.
func validateOption(method, key string, values []string) error {
	if len(values) != 1 {
		return status.Error(codes.InvalidArgument, "Query options must not repeat")
	}
	if fallback, ok := defaultOnlyOptions[method][key]; ok {
		if value := values[0]; value != fallback && (fallback != valueFalse || value != "") {
			return gap(key + "=" + value)
		}
		return nil
	}
	if !slices.Contains(universalQueryOptions, key) && !slices.Contains(queryOptions[method], key) {
		return gap("unsupported query option " + key)
	}
	return nil
}

// validateQueryValues rejects option values this emulator does not implement.
func validateQueryValues(method string, query url.Values) error {
	if alt := query.Get(queryAlt); alt != "" && alt != "json" && (alt != altMedia || method != MethodObjectsGet) {
		return gap("unsupported response representation")
	}
	return validateOptionValues(query)
}

// validateOptionValues checks the method-independent option values.
func validateOptionValues(query url.Values) error {
	if v := query.Get("optionsRequestedPolicyVersion"); v != "" && v != "1" && v != "3" {
		return gap("requested IAM policy version")
	}
	// The Go client sends projection=full on bucket and object reads, lists,
	// patches, and inserts. Every bucket here uses uniform bucket-level
	// access, which has no ACLs to add, so full returns the noAcl shape
	// (storage/docs/json_api/v1/buckets/get, "projection").
	if projection := query.Get("projection"); projection != "" && projection != "noAcl" && projection != "full" {
		return gap("projection " + projection)
	}
	if versions := query.Get("versions"); versions != "" && versions != "true" && versions != valueFalse {
		return provider.Invalid("versions must be a boolean")
	}
	return nil
}

// maxUploadMetadata bounds the multipart metadata part that accompanies an object.
const maxUploadMetadata = 64 << 10

var errUploadTooLarge = status.Error(codes.ResourceExhausted, "Objects are limited to 128 MiB")

func parseUpload(r *gateway.RESTRequest) (*uploadRequest, error) {
	if r.Media == nil {
		return nil, gap("use a media or multipart upload")
	}
	payload, err := readUpload(r.Media)
	if err != nil {
		return nil, err
	}
	u := &uploadRequest{name: r.Query.Get(keyName), query: r.Query, headers: r.Headers, body: payload}
	if err := parseUploadBody(r, payload, u); err != nil {
		return nil, err
	}
	if !validObjectName(u.name) {
		return nil, status.Error(codes.InvalidArgument, "Invalid object name")
	}
	return u, nil
}

// parseUploadBody reads the body of one upload protocol.
func parseUploadBody(r *gateway.RESTRequest, payload []byte, u *uploadRequest) error {
	switch r.Query.Get(queryUploadType) {
	case altMedia:
		if len(payload) > maxObjectSize {
			return errUploadTooLarge
		}
		return nil
	case "multipart":
		return parseMultipart(r, payload, u)
	case uploadResumable:
		return parseResumable(r, payload, u)
	}
	return gap("upload protocol " + r.Query.Get(queryUploadType))
}

// parseResumable reads a resumable initiation: optional JSON object metadata
// plus the declared media type and size. The media arrives in later requests.
func parseResumable(r *gateway.RESTRequest, payload []byte, u *uploadRequest) error {
	if len(payload) > maxUploadMetadata {
		return status.Error(codes.InvalidArgument, "Object metadata exceeds supported size")
	}
	value := map[string]json.RawMessage{}
	if len(bytes.TrimSpace(payload)) > 0 && (json.Unmarshal(payload, &value) != nil || value == nil) {
		return status.Error(codes.InvalidArgument, "Invalid object metadata")
	}
	if err := applyUploadMetadata(r, value, u); err != nil {
		return err
	}
	if kind := r.Headers.Get("X-Upload-Content-Type"); kind != "" && value[keyContentType] == nil {
		value[keyContentType] = rawString(kind)
	}
	total, err := declaredLength(r.Headers.Get("X-Upload-Content-Length"))
	u.resumable, u.metadata, u.body, u.total = true, value, nil, total
	return err
}

// declaredLength parses X-Upload-Content-Length; absent is unknown (-1).
func declaredLength(header string) (int64, error) {
	if header == "" {
		return -1, nil
	}
	size, err := strconv.ParseInt(header, 10, 64)
	if err != nil || size < 0 {
		return 0, status.Error(codes.InvalidArgument, "X-Upload-Content-Length must be a nonnegative integer")
	}
	if size > maxObjectSize {
		return 0, errUploadTooLarge
	}
	return size, nil
}

// readUpload reads an upload body: at most one object plus its metadata part.
func readUpload(media io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(media, maxObjectSize+maxUploadMetadata+1))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Cannot read upload")
	}
	if len(payload) > maxObjectSize+maxUploadMetadata {
		return nil, errUploadTooLarge
	}
	return payload, nil
}

func validObjectName(name string) bool {
	return name != "" && len(name) <= 1024 && !strings.ContainsAny(name, "\r\n")
}

// parseMultipart reads a multipart/related upload: one metadata part, then one
// media part. Caller-supplied metadata is restricted to fields the provider
// actually mirrors, and anything else is an explicit coverage gap.
func parseMultipart(r *gateway.RESTRequest, payload []byte, u *uploadRequest) error {
	boundary, err := multipartBoundary(r.Headers.Get(headerContentType))
	if err != nil {
		return err
	}
	reader := multipart.NewReader(bytes.NewReader(payload), boundary)
	metadata, err := reader.NextPart()
	if err != nil {
		return status.Error(codes.InvalidArgument, "Missing object metadata")
	}
	if err := readMetadataPart(r, metadata, u); err != nil {
		return err
	}
	if err := readMediaPart(reader); err != nil {
		return err
	}
	return nil
}

// multipartBoundary extracts the boundary of a multipart/related content type.
func multipartBoundary(contentType string) (string, error) {
	kind, params, err := mime.ParseMediaType(contentType)
	if err != nil || kind != "multipart/related" || params["boundary"] == "" {
		return "", status.Error(codes.InvalidArgument, "Expected multipart/related upload")
	}
	return params["boundary"], nil
}

// readMetadataPart validates the metadata part of a multipart upload.
func readMetadataPart(r *gateway.RESTRequest, metadata *multipart.Part, u *uploadRequest) error {
	data, err := io.ReadAll(io.LimitReader(metadata, maxUploadMetadata+1))
	if err != nil || len(data) > maxUploadMetadata {
		return status.Error(codes.InvalidArgument, "Object metadata exceeds supported size")
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(data, &value) != nil || value == nil {
		return status.Error(codes.InvalidArgument, "Invalid object metadata")
	}
	return applyUploadMetadata(r, value, u)
}

// readMediaPart drains the media part and rejects a third part.
func readMediaPart(reader *multipart.Reader) error {
	media, err := reader.NextPart()
	if err != nil {
		return status.Error(codes.InvalidArgument, "Missing object media")
	}
	size, err := io.Copy(io.Discard, io.LimitReader(media, maxObjectSize+1))
	if err != nil || size > maxObjectSize {
		return errUploadTooLarge
	}
	if _, err := reader.NextPart(); err != io.EOF {
		return status.Error(codes.InvalidArgument, "Expected exactly two upload parts")
	}
	return nil
}

// supportedUploadFields lists the metadata keys a multipart upload may carry.
var supportedUploadFields = map[string]bool{
	keyName: true, keyBucket: true, "storageClass": true, keyContentType: true,
	"metadata": true, "md5Hash": true, "crc32c": true, keyCacheControl: true,
	keyDisposition: true, keyLanguage: true,
}

// applyUploadMetadata validates a multipart metadata part and records the name.
func applyUploadMetadata(r *gateway.RESTRequest, value map[string]json.RawMessage, u *uploadRequest) error {
	for key := range value {
		if !supportedUploadFields[key] {
			return gap("unsupported object metadata " + key)
		}
	}
	if err := uploadClass(value); err != nil {
		return err
	}
	if err := uploadBucket(r, value); err != nil {
		return err
	}
	return uploadName(u, value)
}

// uploadClass accepts only the STANDARD storage class.
func uploadClass(value map[string]json.RawMessage) error {
	raw, ok := value["storageClass"]
	if !ok {
		return nil
	}
	var class string
	if json.Unmarshal(raw, &class) != nil || class != storageClassStandard {
		return gap("only STANDARD object storage class is supported")
	}
	return nil
}

// uploadBucket keeps caller-supplied bucket names consistent with the target.
func uploadBucket(r *gateway.RESTRequest, value map[string]json.RawMessage) error {
	raw, ok := value[keyBucket]
	if !ok {
		return nil
	}
	var bucket string
	if json.Unmarshal(raw, &bucket) != nil || bucket != r.Path[keyBucket] {
		return status.Error(codes.InvalidArgument, "Object bucket does not match the upload target")
	}
	return nil
}

// uploadName records the metadata-supplied name, or the query parameter's.
func uploadName(u *uploadRequest, value map[string]json.RawMessage) error {
	raw, ok := value[keyName]
	if !ok {
		return nil
	}
	var name string
	if json.Unmarshal(raw, &name) != nil {
		return status.Error(codes.InvalidArgument, "Invalid object name")
	}
	if u.name != "" && u.name != name {
		return status.Error(codes.InvalidArgument, "Object names disagree")
	}
	u.name = name
	return nil
}

// checkNumericPreconditions applies the ifGeneration*/ifMetageneration* pair for
// one numeric field. A non-numeric or negative value is a caller error.
func checkNumericPreconditions(r *gateway.RESTRequest, current map[string]any, field string, fail func() *gateway.RESTResponse) (*gateway.RESTResponse, error) {
	actual := "0"
	if current != nil {
		actual, _ = current[strings.ToLower(field)].(string)
	}
	for _, match := range []bool{true, false} {
		suffix := "Match"
		if !match {
			suffix = "NotMatch"
		}
		value := r.Query.Get("if" + field + suffix)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return nil, status.Error(codes.InvalidArgument, "Precondition must be a nonnegative integer")
		}
		if (value == actual) != match {
			return fail(), nil
		}
	}
	return nil, nil //nolint:nilnil // a nil response with a nil error means "no precondition failed"
}

// etagMatcher matches an If-Match/If-None-Match header against the current etag.
func etagMatcher(current map[string]any) func(string) bool {
	etag := ""
	if current != nil {
		etag, _ = current["etag"].(string)
	}
	return func(header string) bool {
		for value := range strings.SplitSeq(header, ",") {
			value = strings.TrimSpace(value)
			if value == "*" && current != nil || strings.Trim(value, `"`) == etag && etag != "" {
				return true
			}
		}
		return false
	}
}

func preconditions(r *gateway.RESTRequest, current map[string]any, method string) (*gateway.RESTResponse, error) {
	fail := func() *gateway.RESTResponse {
		return wireError(412, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
	}
	for _, field := range []string{"Generation", "Metageneration"} {
		if response, err := checkNumericPreconditions(r, current, field, fail); response != nil || err != nil {
			return response, err
		}
	}
	matches := etagMatcher(current)
	if h := r.Headers.Get("If-Match"); h != "" && !matches(h) {
		return fail(), nil
	}
	if h := r.Headers.Get("If-None-Match"); h != "" && matches(h) {
		if method == MethodObjectsGet {
			return &gateway.RESTResponse{Status: 304}, nil
		}
		return fail(), nil
	}
	return nil, nil //nolint:nilnil // a nil response with a nil error means "no precondition failure"
}

type bucketRequest struct {
	Name         string            `json:"name"`
	Location     string            `json:"location"`
	StorageClass string            `json:"storageClass"`
	Labels       map[string]string `json:"labels,omitempty"`
	IAM          struct {
		Uniform struct {
			Enabled bool `json:"enabled"`
		} `json:"uniformBucketLevelAccess"`
		Public string `json:"publicAccessPrevention"`
	} `json:"iamConfiguration"`
	SoftDelete struct {
		Seconds       json.Number `json:"retentionDurationSeconds"`
		EffectiveTime *string     `json:"effectiveTime,omitempty"`
	} `json:"softDeletePolicy"`
	Versioning struct {
		Enabled bool `json:"enabled"`
	} `json:"versioning"`
	Hold      bool `json:"defaultEventBasedHold"`
	Lifecycle *struct {
		Rule []json.RawMessage `json:"rule"`
	} `json:"lifecycle,omitempty"`
}

func (p *Provider) createBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	request, err := decodeBucketRequest(r.JSON)
	if err != nil {
		return nil, err
	}
	if _, exists := p.index.Get(BucketResource(request.Name)); exists {
		return bucketNameOwned(), nil
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the bucket request")
	}
	response, err := b.forward(ctx, "POST", "/storage/v1/b", r.Query, http.Header{headerContentType: {"application/json"}}, payload)
	if err != nil || response.Status != 200 {
		return response, err
	}
	metadata, err := p.createdBucketMetadata(scope, request, payload)
	if err != nil {
		return nil, err
	}
	if err := p.catalog.PublishBucket(scope, Bucket{Name: request.Name, Location: request.Location, Metadata: metadata}); err != nil {
		return rollBackBucket(ctx, b, request.Name)
	}
	body, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	p.bucketMetadata(scope, body)
	return response, nil
}

func bucketNameOwned() *gateway.RESTResponse {
	return wireError(409, "conflict", "Bucket name is already owned")
}

// decodeBucketRequest reads a bucket create body and refuses every
// configuration this emulator does not model, then normalizes the rest.
func decodeBucketRequest(body []byte) (bucketRequest, error) {
	var request bucketRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return request, gap("unsupported bucket field " + field)
		}
		return request, gap("unsupported bucket configuration")
	}
	if err := unsupportedBucketPolicy(request); err != nil {
		return request, err
	}
	if request.StorageClass == "" {
		request.StorageClass = storageClassStandard
	}
	if request.StorageClass != storageClassStandard {
		return request, gap("only STANDARD storage class")
	}
	if request.Location == "" {
		return request, status.Error(codes.InvalidArgument, "Explicit bucket location is required")
	}
	request.Location = strings.ToUpper(request.Location)
	return request, nil
}

// unsupportedBucketPolicy requires the access, retention, and lifecycle
// settings the emulator implements to be stated explicitly.
func unsupportedBucketPolicy(request bucketRequest) error {
	if !request.IAM.Uniform.Enabled || request.IAM.Public != "enforced" {
		return gap("explicit uniform bucket-level access and enforced public access prevention are required")
	}
	if request.Versioning.Enabled || request.Hold || request.SoftDelete.Seconds != "0" {
		return gap("explicitly disable versioning, default holds, and soft delete")
	}
	if request.SoftDelete.EffectiveTime != nil {
		return gap("soft-delete effective time is server-owned")
	}
	if request.Lifecycle != nil && len(request.Lifecycle.Rule) > 0 {
		return gap("bucket lifecycle rules")
	}
	return nil
}

// createdBucketMetadata derives the provider-owned metadata of a new bucket
// from the request body forwarded to the backend.
func (p *Provider) createdBucketMetadata(scope state.Scope, request bucketRequest, payload []byte) (string, error) {
	metadata := map[string]any{}
	_ = json.Unmarshal(payload, &metadata)
	metadata["softDeletePolicy"] = map[string]string{"retentionDurationSeconds": string(request.SoftDelete.Seconds)}
	project, _ := p.index.Get(provider.ProjectResource(scope.Project))
	metadata["projectNumber"] = project.Number
	metadata["metageneration"] = "1"
	metadata["etag"] = objectETag(request.Name, "1")
	// Real GCS reports rpo only for multi-region and dual-region buckets;
	// the recorded single-region snapshot carries rpo null, so the mirrored
	// metadata drops the request echo. The multi-region shape is a gap.
	delete(metadata, "rpo")
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return "", status.Error(codes.Internal, "Unable to encode the bucket metadata")
	}
	return string(encoded), nil
}

// rollBackBucket handles a lost publication race: a competing project claimed
// the global name while this backend created its empty bucket. Only this
// scope's bucket is deleted before the conflict is returned.
func rollBackBucket(ctx context.Context, b *memoryBackend, name string) (*gateway.RESTResponse, error) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	rollback, err := b.forward(cleanupCtx, "DELETE", "/storage/v1/b/"+url.PathEscape(name), nil, nil, nil)
	if err != nil || rollback.Status < 200 || rollback.Status >= 300 {
		return nil, status.Error(codes.Internal, "Bucket publication failed; reset this Storage scope before retrying")
	}
	return bucketNameOwned(), nil
}

func (p *Provider) bucketMetadata(scope state.Scope, value map[string]any) {
	name, _ := value[keyName].(string)
	bucket, found, err := p.catalog.Bucket(scope, name)
	if err != nil || !found {
		return
	}
	var metadata map[string]any
	_ = json.Unmarshal([]byte(bucket.Metadata), &metadata)
	maps.Copy(value, metadata)
	value["id"] = name
	value["kind"] = "storage#bucket"
}
