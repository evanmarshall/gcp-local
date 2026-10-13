package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	storageapi "github.com/evanmarshall/gcp-local/api/generated/storage"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/bucketconfig"
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
	queryPartialSuccess       = "returnPartialSuccess"
	queryIfGenerationMatch    = "ifGenerationMatch"
	queryIfGenerationNotMatch = "ifGenerationNotMatch"
	queryIfMetaMatch          = "ifMetagenerationMatch"
	queryIfMetaNotMatch       = "ifMetagenerationNotMatch"
)

// queryOptions lists the documented query parameters each method may carry.
var queryOptions = map[string][]string{
	MethodBucketsInsert:       {queryProject, queryObjectRetention},
	MethodBucketsList:         {queryProject, queryPrefix, queryMaxResults, queryPageToken, queryPartialSuccess},
	MethodBucketsGetIamPolicy: {"optionsRequestedPolicyVersion"},
	MethodBucketsPatch:        {queryIfMetaMatch, queryIfMetaNotMatch},
	MethodBucketsLock:         {queryIfMetaMatch},
	MethodObjectsInsert:       {queryUploadType, keyName, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsGet:          {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch, querySoftDeleted},
	MethodObjectsDelete:       {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsPatch:        {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch, queryOverrideRetention},
	// Rewrite preconditions apply to the destination. maxBytesRewrittenPerCall
	// is accepted and not enforced: every rewrite here finishes in one call.
	MethodObjectsRewrite: {"sourceGeneration", "maxBytesRewrittenPerCall", queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
	MethodObjectsList:    {queryPrefix, "delimiter", queryMaxResults, queryPageToken, "startOffset", "endOffset", queryVersions, querySoftDeleted, queryFoldersAsPrefixes},
	// folders/list: delimiter and the offsets are gaps.
	MethodFoldersList: {queryPrefix, "pageSize", queryPageToken},
	// folders/insert: recursive creates the missing parents.
	MethodFoldersInsert: {"recursive"},
	// objects/restore: generation names the soft-deleted version; the
	// preconditions apply to the live object it would replace.
	MethodObjectsRestore: {keyGeneration, queryIfGenerationMatch, queryIfGenerationNotMatch, queryIfMetaMatch, queryIfMetaNotMatch},
}

// defaultOnlyOptions are options accepted only at their documented default,
// which is the behavior served; any other value is a gap. The Go client sends
// them on every call (storage/docs/json_api/v1/objects/list),
// and gcloud sends enableObjectRetention=False on every bucket insert
// (buckets/insert: true permanently enables object retention, which is not
// modeled). copySourceAcl copies ACLs, which uniform buckets do not have.
var defaultOnlyOptions = map[string]map[string]string{
	MethodObjectsList:    {"matchGlob": "", "includeTrailingDelimiter": valueFalse},
	MethodObjectsRestore: {"copySourceAcl": valueFalse},
}

// isFalse reports whether a boolean option is false. Google's API front end
// reads booleans case-insensitively; gcloud sends "False" (recorded:
// storage-gcloud-bucket-defaults). An empty value is the default, false.
func isFalse(value string) bool {
	return value == "" || strings.EqualFold(value, valueFalse)
}

// booleanOption reads one boolean query option, refusing a non-boolean value.
func booleanOption(query url.Values, key string) (bool, error) {
	value := query.Get(key)
	switch {
	case isFalse(value):
		return false, nil
	case strings.EqualFold(value, valueTrue):
		return true, nil
	}
	return false, provider.Invalid(key + " must be a boolean")
}

// universalQueryOptions apply to every method.
var universalQueryOptions = []string{"prettyPrint", "fields", queryAlt, "projection"}

// validateQuery checks a request's options and headers. An option Google's
// API does not define for the method is dropped, as GCP ignores it (recorded:
// storage-go-client-projection "unknown-option"), so it never reaches the
// backend. A defined option this emulator does not serve, such as
// userProject, stays an explicit gap.
func validateQuery(method string, r *gateway.RESTRequest) error {
	for key, values := range r.Query {
		if !definedOption(method, key) {
			r.Query.Del(key)
			continue
		}
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
		if value := values[0]; value != fallback && (fallback != valueFalse || !isFalse(value)) {
			return gap(key + "=" + value)
		}
		return nil
	}
	if !slices.Contains(universalQueryOptions, key) && !slices.Contains(queryOptions[method], key) {
		return gap("unsupported query option " + key)
	}
	return nil
}

// credentialOptions are system parameters GCP honors on every method though
// discovery does not list them. access_token carries a credential, so it is
// refused rather than ignored: ignoring it would answer as a different caller
// than GCP does.
var credentialOptions = []string{"access_token"}

// definedOption reports whether the Storage discovery document defines key
// for method, or this provider serves it.
func definedOption(method, key string) bool {
	return slices.Contains(storageapi.StorageQueryParameters[method], key) ||
		slices.Contains(storageapi.StorageGlobalParameters, key) ||
		slices.Contains(credentialOptions, key) ||
		slices.Contains(universalQueryOptions, key) || slices.Contains(queryOptions[method], key)
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
	for _, key := range []string{queryVersions, querySoftDeleted, queryFoldersAsPrefixes, queryPartialSuccess, queryOverrideRetention, queryObjectRetention} {
		if _, err := booleanOption(query, key); err != nil {
			return err
		}
	}
	return nil
}

// maxUploadMetadata bounds the multipart metadata part that accompanies an object.
const maxUploadMetadata = 64 << 10

// errUploadTooLarge refuses an object over maxObjectSize as a gap, not 429:
// clients retry 429, and GCP accepts objects up to 5 TiB.
var errUploadTooLarge = gap("objects over 128 MiB")

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
	if err != nil {
		kind, params = singleQuotedBoundary(contentType)
	}
	if kind != "multipart/related" || params["boundary"] == "" {
		return "", status.Error(codes.InvalidArgument, "Expected multipart/related upload")
	}
	return params["boundary"], nil
}

// singleQuotedBoundary reads the content type apitools (gcloud) sends for a
// multipart upload: it renders the boundary with Python's %r, so it arrives
// single-quoted, as in boundary='===============0123456789=='. RFC 2045 has
// no single quotes, so mime.ParseMediaType refuses it; GCP accepts it.
func singleQuotedBoundary(contentType string) (string, map[string]string) {
	kind, rest, _ := strings.Cut(contentType, ";")
	value, ok := strings.CutPrefix(strings.TrimSpace(rest), "boundary='")
	value, closed := strings.CutSuffix(value, "'")
	if !ok || !closed || value == "" || strings.ContainsAny(value, "';") {
		return "", nil
	}
	return strings.ToLower(strings.TrimSpace(kind)), map[string]string{"boundary": value}
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
	keyMetadata: true, "md5Hash": true, "crc32c": true, keyCacheControl: true,
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
		Uniform *bucketAccess `json:"uniformBucketLevelAccess,omitempty"`
		// The feature's former name. The Bucket reference: "For backward
		// compatibility, this field will be populated with identical
		// information as the uniformBucketLevelAccess field."
		BucketPolicyOnly *bucketAccess `json:"bucketPolicyOnly,omitempty"`
		Public           string        `json:"publicAccessPrevention"`
	} `json:"iamConfiguration"`
	SoftDelete struct {
		Seconds       json.Number `json:"retentionDurationSeconds"`
		EffectiveTime *string     `json:"effectiveTime,omitempty"`
	} `json:"softDeletePolicy"`
	Versioning struct {
		Enabled bool `json:"enabled"`
	} `json:"versioning"`
	Hold            bool            `json:"defaultEventBasedHold"`
	RetentionPolicy json.RawMessage `json:"retentionPolicy,omitempty"`
	Namespace       *bucketAccess   `json:"hierarchicalNamespace,omitempty"`
	Lifecycle       json.RawMessage `json:"lifecycle,omitempty"`
}

// Public access prevention values (buckets resource, iamConfiguration).
const (
	publicInherited = "inherited"
	publicEnforced  = "enforced"
)

// Soft-delete retention bounds (storage/docs/soft-delete): 0 disables soft
// delete; otherwise 7 to 90 days. A bucket created without a policy gets the
// 7-day default (recorded: storage-bucket-defaults).
const (
	minRetention     = 7 * 24 * 60 * 60
	maxRetention     = 90 * 24 * 60 * 60
	defaultRetention = minRetention
)

func (p *Provider) createBucket(ctx context.Context, b *memoryBackend, r *gateway.RESTRequest, scope state.Scope, decision string) (*gateway.RESTResponse, error) {
	request, rules, refused, err := p.decodeBucketCreate(r.JSON, decision, scope)
	if refused != nil || err != nil {
		return refused, err
	}
	if _, exists := p.index.Get(BucketResource(request.Name)); exists {
		return bucketNameOwned(), nil
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to encode the bucket request")
	}
	response, err := b.forward(ctx, "POST", "/storage/v1/b", r.Query, http.Header{headerContentType: {contentJSON}}, payload)
	if err != nil || response.Status != 200 {
		return response, err
	}
	body, err := jsonObject(response.JSON)
	if err != nil {
		return nil, err
	}
	objectRetention, _ := booleanOption(r.Query, queryObjectRetention)
	metadata, err := p.createdBucketMetadata(scope, request, payload, body, objectRetention, rules)
	if err != nil {
		return nil, err
	}
	if err := p.catalog.PublishBucket(scope, Bucket{Name: request.Name, Location: request.Location, Metadata: metadata}); err != nil {
		return rollBackBucket(ctx, b, request.Name)
	}
	p.bucketMetadata(scope, body)
	return response, nil
}

// decodeBucketCreate decodes a bucket create and its lifecycle rules under
// the project's organization policies: uniform access applies to the access
// setting, and gcp.resourceLocations to the location (modeled, not recorded:
// the sandbox allows every location).
func (p *Provider) decodeBucketCreate(body []byte, decision string, scope state.Scope) (bucketRequest, []bucketconfig.Rule, *gateway.RESTResponse, error) {
	name := provider.ProjectResource(scope.Project)
	project, _ := p.index.Get(name)
	request, err := decodeBucketRequest(body, func(request *bucketRequest) error {
		return p.uniformPolicy(decision, name, request)
	})
	if _, violated := errors.AsType[*iam.OrgPolicyError](err); err != nil && !violated {
		return request, nil, nil, err
	}
	if err == nil {
		err = p.iam.CheckOrgPolicy(decision, name, orgpolicy.ResourceLocations, request.Location)
	}
	if refused, err := orgPolicyRefusal(err); refused != nil || err != nil {
		return request, nil, refused, err
	}
	rules, err := bucketconfig.ParseLifecycle(request.Lifecycle, request.Location, project.Number)
	return request, rules, nil, err //nolint:wrapcheck // GCP's refusal, rendered as is
}

func bucketNameOwned() *gateway.RESTResponse {
	return wireError(409, "conflict", "Bucket name is already owned")
}

// decodeBucketRequest reads a bucket create body and refuses every
// configuration this emulator does not model, then normalizes the rest.
// uniform applies the organization's uniform access policy to the request's
// access setting before the access model is checked.
func decodeBucketRequest(body []byte, uniform func(*bucketRequest) error) (bucketRequest, error) {
	var request bucketRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return request, gap("unsupported bucket field " + field)
		}
		return request, gap("unsupported bucket configuration")
	}
	for _, check := range []func(*bucketRequest) error{uniformAccess, namespaceAccess, uniform, unsupportedBucketPolicy, bucketRetention, bucketPlacement} {
		if err := check(&request); err != nil {
			return request, err
		}
	}
	return request, nil
}

// namespaceAccess refuses hierarchical namespace without uniform
// bucket-level access, as GCP does before any other check (recorded:
// storage-hierarchical-namespace "namespace-without-uniform").
func namespaceAccess(request *bucketRequest) error {
	if request.Namespace != nil && request.Namespace.Enabled && (request.IAM.Uniform == nil || !request.IAM.Uniform.Enabled) {
		return bucketconfig.Invalid("Hierarchical namespace buckets must use uniform bucket-level access.")
	}
	return nil
}

// bucketRetention fills in the default soft-delete policy and checks both
// retention settings.
func bucketRetention(request *bucketRequest) error {
	if request.SoftDelete.Seconds == "" {
		request.SoftDelete.Seconds = json.Number(strconv.Itoa(defaultRetention))
	}
	if _, err := softDeleteRetention(request.SoftDelete.Seconds); err != nil {
		return err
	}
	if raw := request.RetentionPolicy; len(raw) > 0 && string(raw) != jsonNull {
		if _, err := bucketconfig.ParseRetentionPeriod(raw); err != nil {
			return err //nolint:wrapcheck // GCP's refusal, rendered as is
		}
	}
	return nil
}

// bucketPlacement checks the storage class and the explicit location.
func bucketPlacement(request *bucketRequest) error {
	if request.StorageClass == "" {
		request.StorageClass = storageClassStandard
	}
	if request.StorageClass != storageClassStandard {
		return gap("only STANDARD storage class")
	}
	if request.Location == "" {
		return status.Error(codes.InvalidArgument, "Explicit bucket location is required")
	}
	request.Location = strings.ToUpper(request.Location)
	return nil
}

// softDeleteRetention parses a soft-delete retention duration and refuses one
// outside GCP's bounds with its recorded error (storage-gcloud-bucket-defaults
// "soft-delete-below-minimum").
func softDeleteRetention(value json.Number) (int64, error) {
	seconds, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil || (seconds != 0 && (seconds < minRetention || seconds > maxRetention)) {
		return 0, bucketconfig.Invalid("Soft delete policy must have a retention duration between 7 days and 90 days.")
	}
	return seconds, nil
}

// bucketAccess is one uniform bucket-level access setting.
type bucketAccess struct {
	Enabled bool `json:"enabled"`
}

// uniformAccess accepts bucketPolicyOnly as the alias of
// uniformBucketLevelAccess (rclone sends only the alias) and sets both, so the
// created bucket reports both as GCP does.
func uniformAccess(request *bucketRequest) error {
	uniform, alias := request.IAM.Uniform, request.IAM.BucketPolicyOnly
	if uniform != nil && alias != nil && uniform.Enabled != alias.Enabled {
		return status.Error(codes.InvalidArgument, "uniformBucketLevelAccess and bucketPolicyOnly disagree")
	}
	if uniform == nil {
		uniform = alias
	}
	if uniform != nil {
		access := *uniform
		request.IAM.Uniform, request.IAM.BucketPolicyOnly = &access, &access
	}
	return nil
}

// uniformPolicy applies storage.uniformBucketLevelAccess to a new bucket
// (recorded: orgpolicy-constraint-enforcement): under the constraint an
// omitted setting becomes enabled and a disabled one is refused.
func (p *Provider) uniformPolicy(decision, project string, request *bucketRequest) error {
	uniform := request.IAM.Uniform
	if uniform != nil {
		if uniform.Enabled {
			return nil
		}
		if err := p.iam.CheckOrgPolicy(decision, project, orgpolicy.UniformBucketAccess); err != nil {
			return fmt.Errorf("check uniform bucket-level access: %w", err)
		}
		return nil
	}
	enforced, err := p.iam.OrgPolicyEnforced(project, orgpolicy.UniformBucketAccess)
	if err != nil {
		return fmt.Errorf("read uniform bucket-level access: %w", err)
	}
	if enforced {
		access := bucketAccess{Enabled: true}
		request.IAM.Uniform, request.IAM.BucketPolicyOnly = &access, &access
	}
	return nil
}

// orgPolicyRefusal renders an organization policy refusal as Storage does:
// 412 conditionNotMet naming the constraint, or for a policy member outside
// the permitted customers GCP's member message (both recorded:
// orgpolicy-constraint-enforcement). Any other error is returned wrapped.
func orgPolicyRefusal(err error) (*gateway.RESTResponse, error) {
	violation, ok := errors.AsType[*iam.OrgPolicyError](err)
	if !ok {
		if err != nil {
			return nil, fmt.Errorf("check the organization policy: %w", err)
		}
		return nil, nil //nolint:nilnil // no refusal and no error: the request may proceed
	}
	message := "Request violates constraint 'constraints/" + violation.Constraint + "'"
	if violation.Constraint == orgpolicy.MemberDomains {
		message = iam.MemberDomainsMessage
	}
	return wireError(412, "conditionNotMet", message), nil
}

// unsupportedBucketPolicy requires uniform bucket-level access, the only
// access model here, and refuses the settings the
// emulator does not implement. An omitted public access prevention is GCP's
// default, inherited (recorded: storage-bucket-defaults).
func unsupportedBucketPolicy(request *bucketRequest) error {
	if request.IAM.Uniform == nil || !request.IAM.Uniform.Enabled {
		return gap("explicit uniform bucket-level access is required; ACLs are not modeled")
	}
	if err := publicAccessPrevention(request); err != nil {
		return err
	}
	if request.SoftDelete.EffectiveTime != nil {
		return gap("soft-delete effective time is server-owned")
	}
	return nil
}

// publicAccessPrevention accepts GCP's two settings and fills in the
// default, inherited, when the request omits it.
func publicAccessPrevention(request *bucketRequest) error {
	switch request.IAM.Public {
	case "":
		request.IAM.Public = publicInherited
	case publicInherited, publicEnforced:
	default:
		return gap("publicAccessPrevention " + request.IAM.Public)
	}
	return nil
}

// softDeletePolicy renders a policy as GCP reports it: an active policy
// carries the time it took effect, a disabled one only its zero duration
// (recorded: storage-gcloud-bucket-defaults).
func softDeletePolicy(seconds int64, effective string) map[string]string {
	policy := map[string]string{"retentionDurationSeconds": strconv.FormatInt(seconds, 10)}
	if seconds > 0 {
		policy["effectiveTime"] = effective
	}
	return policy
}

// storageTime formats a time as Storage reports it.
func storageTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// createdBucketMetadata derives the provider-owned metadata of a new bucket
// from the request body forwarded to the backend and the backend's reply.
func (p *Provider) createdBucketMetadata(scope state.Scope, request bucketRequest, payload []byte, created map[string]any, objectRetention bool, rules []bucketconfig.Rule) (string, error) {
	metadata := map[string]any{}
	_ = json.Unmarshal(payload, &metadata)
	seconds, _ := softDeleteRetention(request.SoftDelete.Seconds)
	// A new bucket's policy takes effect when it is created (recorded:
	// storage-bucket-defaults "effective_time_is_creation").
	timeCreated, _ := created["timeCreated"].(string)
	metadata["softDeletePolicy"] = softDeletePolicy(seconds, timeCreated)
	// GCP reports the same lockedTime under both names: the deadline, 90 days
	// out, for disabling uniform access (recorded: storage-bucket-policy-only).
	access := map[string]any{keyEnabled: true, "lockedTime": storageTime(time.Now().Add(90 * 24 * time.Hour))}
	metadata["iamConfiguration"] = map[string]any{
		"uniformBucketLevelAccess": access, "bucketPolicyOnly": access, "publicAccessPrevention": request.IAM.Public,
	}
	// The backend owns versioning: it keeps the noncurrent generations.
	delete(metadata, "versioning")
	createdProtection(metadata, request, timeCreated, objectRetention)
	// A bucket reports hierarchicalNamespace only when it has one.
	if request.Namespace == nil || !request.Namespace.Enabled {
		delete(metadata, keyNamespace)
	}
	setLifecycle(metadata, rules)
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

// createdProtection renders a new bucket's retention configuration: a
// retention policy takes effect at creation, a default event-based hold
// appears only when on (recorded: storage-retention-holds "policy-set"), and
// enableObjectRetention=true reports object retention as Enabled (recorded:
// storage-object-retention "bucket-object-retention").
func createdProtection(metadata map[string]any, request bucketRequest, timeCreated string, objectRetention bool) {
	delete(metadata, "retentionPolicy")
	if period, err := bucketconfig.ParseRetentionPeriod(request.RetentionPolicy); err == nil {
		metadata["retentionPolicy"] = retentionPolicy(period, timeCreated, false)
	}
	if !request.Hold {
		delete(metadata, keyDefaultHold)
	}
	if objectRetention {
		metadata["objectRetention"] = map[string]any{"mode": "Enabled"}
	}
}

// setLifecycle stores validated lifecycle rules as GCP renders them; no rules
// is no lifecycle field (recorded: storage-lifecycle-rules "rules-emptied").
func setLifecycle(metadata map[string]any, rules []bucketconfig.Rule) {
	if len(rules) == 0 {
		delete(metadata, "lifecycle")
		return
	}
	metadata["lifecycle"] = map[string]any{"rule": rules}
}

// retentionPolicy renders a bucket retention policy; isLocked appears only
// once locked (recorded: storage-bucket-lock "policy-set", "locked").
func retentionPolicy(period int64, effective string, locked bool) map[string]any {
	policy := map[string]any{"retentionPeriod": strconv.FormatInt(period, 10), "effectiveTime": effective}
	if locked {
		policy["isLocked"] = true
	}
	return policy
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
	// The catalog owns the default hold; the backend reports its own copy.
	delete(value, keyDefaultHold)
	var metadata map[string]any
	_ = json.Unmarshal([]byte(bucket.Metadata), &metadata)
	maps.Copy(value, metadata)
	value["id"] = name
	value[keyKind] = kindBucketDocument
}
