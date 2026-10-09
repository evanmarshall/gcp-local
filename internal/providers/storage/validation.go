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
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxObjectSize = 8 << 20

type uploadRequest struct {
	name    string
	query   url.Values
	headers http.Header
	body    []byte
}

func validateQuery(method string, r *gateway.RESTRequest) error {
	allowed := map[string]bool{"prettyPrint": true, "fields": true, "alt": true, "projection": true}
	add := func(names ...string) {
		for _, name := range names {
			allowed[name] = true
		}
	}
	switch method {
	case "storage.buckets.insert":
		add("project")
	case "storage.buckets.list":
		add("project", "prefix", "maxResults", "pageToken")
	case "storage.buckets.getIamPolicy":
		add("optionsRequestedPolicyVersion")
	case "storage.buckets.patch":
		add("ifMetagenerationMatch", "ifMetagenerationNotMatch")
	case "storage.objects.insert":
		add("uploadType", "name", "ifGenerationMatch", "ifGenerationNotMatch", "ifMetagenerationMatch", "ifMetagenerationNotMatch")
	case "storage.objects.get", "storage.objects.delete":
		add("generation", "ifGenerationMatch", "ifGenerationNotMatch", "ifMetagenerationMatch", "ifMetagenerationNotMatch")
	case "storage.objects.list":
		add("prefix", "delimiter", "maxResults", "pageToken", "startOffset", "endOffset", "versions")
	}
	for key, values := range r.Query {
		if !allowed[key] {
			return gap("unsupported query option " + key)
		}
		if len(values) != 1 {
			return status.Error(codes.InvalidArgument, "Query options must not repeat")
		}
	}
	if alt := r.Query.Get("alt"); alt != "" && alt != "json" && (alt != "media" || method != "storage.objects.get") {
		return gap("unsupported response representation")
	}
	if v := r.Query.Get("optionsRequestedPolicyVersion"); v != "" && v != "1" {
		return gap("conditional policies")
	}
	if projection := r.Query.Get("projection"); projection != "" && projection != "noAcl" {
		return gap("only the noAcl projection is supported")
	}
	if versions := r.Query.Get("versions"); versions != "" && versions != "true" && versions != "false" {
		return invalid("versions must be a boolean")
	}
	for key := range r.Headers {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-goog-encryption") || lower == "x-goog-acl" {
			return gap("encryption keys and ACLs")
		}
	}
	return nil
}
func parseUpload(r *gateway.RESTRequest) (*uploadRequest, error) {
	if r.Media == nil {
		return nil, gap("use a media or multipart upload")
	}
	payload, err := io.ReadAll(io.LimitReader(r.Media, maxObjectSize+(64<<10)+1))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Cannot read upload")
	}
	if len(payload) > maxObjectSize+(64<<10) {
		return nil, status.Error(codes.ResourceExhausted, "Uploads are limited to eight MiB")
	}
	u := &uploadRequest{name: r.Query.Get("name"), query: r.Query, headers: r.Headers, body: payload}
	switch r.Query.Get("uploadType") {
	case "media":
		if len(payload) > maxObjectSize {
			return nil, status.Error(codes.ResourceExhausted, "Uploads are limited to eight MiB")
		}
	case "multipart":
		kind, params, err := mime.ParseMediaType(r.Headers.Get("Content-Type"))
		if err != nil || kind != "multipart/related" || params["boundary"] == "" {
			return nil, status.Error(codes.InvalidArgument, "Expected multipart/related upload")
		}
		reader := multipart.NewReader(bytes.NewReader(payload), params["boundary"])
		metadata, err := reader.NextPart()
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Missing object metadata")
		}
		data, err := io.ReadAll(io.LimitReader(metadata, (64<<10)+1))
		if err != nil || len(data) > 64<<10 {
			return nil, status.Error(codes.InvalidArgument, "Object metadata exceeds supported size")
		}
		var value map[string]json.RawMessage
		if json.Unmarshal(data, &value) != nil || value == nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid object metadata")
		}
		for key := range value {
			switch key {
			case "name", "bucket", "storageClass", "contentType", "metadata", "md5Hash", "crc32c", "cacheControl", "contentDisposition", "contentLanguage":
			default:
				return nil, gap("unsupported object metadata " + key)
			}
		}
		var name string
		if raw, ok := value["storageClass"]; ok {
			var class string
			if json.Unmarshal(raw, &class) != nil || class != "STANDARD" {
				return nil, gap("only STANDARD object storage class is supported")
			}
		}
		if raw, ok := value["bucket"]; ok {
			var bucket string
			if json.Unmarshal(raw, &bucket) != nil || bucket != r.Path["bucket"] {
				return nil, status.Error(codes.InvalidArgument, "Object bucket does not match the upload target")
			}
		}
		if raw, ok := value["name"]; ok {
			if json.Unmarshal(raw, &name) != nil {
				return nil, status.Error(codes.InvalidArgument, "Invalid object name")
			}
			if u.name != "" && u.name != name {
				return nil, status.Error(codes.InvalidArgument, "Object names disagree")
			}
			u.name = name
		}
		media, err := reader.NextPart()
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Missing object media")
		}
		size, err := io.Copy(io.Discard, io.LimitReader(media, maxObjectSize+1))
		if err != nil || size > maxObjectSize {
			return nil, status.Error(codes.ResourceExhausted, "Uploads are limited to eight MiB")
		}
		if _, err := reader.NextPart(); err != io.EOF {
			return nil, status.Error(codes.InvalidArgument, "Expected exactly two upload parts")
		}
	default:
		return nil, gap("resumable and other upload protocols")
	}
	if u.name == "" || len(u.name) > 1024 || strings.ContainsAny(u.name, "\r\n") {
		return nil, status.Error(codes.InvalidArgument, "Invalid object name")
	}
	return u, nil
}
func preconditions(r *gateway.RESTRequest, current map[string]any, method string) (*gateway.RESTResponse, error) {
	fail := func() *gateway.RESTResponse {
		return wireError(412, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
	}
	for _, field := range []string{"Generation", "Metageneration"} {
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
	}
	etag := ""
	if current != nil {
		etag, _ = current["etag"].(string)
	}
	matches := func(header string) bool {
		for value := range strings.SplitSeq(header, ",") {
			value = strings.TrimSpace(value)
			if value == "*" && current != nil || strings.Trim(value, `"`) == etag && etag != "" {
				return true
			}
		}
		return false
	}
	if h := r.Headers.Get("If-Match"); h != "" && !matches(h) {
		return fail(), nil
	}
	if h := r.Headers.Get("If-None-Match"); h != "" && matches(h) {
		if method == "storage.objects.get" {
			return &gateway.RESTResponse{Status: 304}, nil
		}
		return fail(), nil
	}
	return nil, nil
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
	var request bucketRequest
	decoder := json.NewDecoder(bytes.NewReader(r.JSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		if after, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			return nil, gap("unsupported bucket field " + after)
		}
		return nil, gap("unsupported bucket configuration")
	}
	if !request.IAM.Uniform.Enabled || request.IAM.Public != "enforced" {
		return nil, gap("explicit uniform bucket-level access and enforced public access prevention are required")
	}
	if request.Versioning.Enabled || request.Hold || request.SoftDelete.Seconds != "0" {
		return nil, gap("explicitly disable versioning, default holds, and soft delete")
	}
	if request.SoftDelete.EffectiveTime != nil {
		return nil, gap("soft-delete effective time is server-owned")
	}
	if request.Lifecycle != nil && len(request.Lifecycle.Rule) > 0 {
		return nil, gap("bucket lifecycle rules")
	}
	if request.StorageClass == "" {
		request.StorageClass = "STANDARD"
	}
	if request.StorageClass != "STANDARD" {
		return nil, gap("only STANDARD storage class")
	}
	if request.Location == "" {
		return nil, status.Error(codes.InvalidArgument, "Explicit bucket location is required")
	}
	request.Location = strings.ToUpper(request.Location)
	if _, exists := p.index.Get(BucketResource(request.Name)); exists {
		return wireError(409, "conflict", "Bucket name is already owned"), nil
	}
	payload, _ := json.Marshal(request)
	response, err := b.forward(ctx, "POST", "/storage/v1/b", r.Query, http.Header{"Content-Type": {"application/json"}}, payload)
	if err != nil || response.Status != 200 {
		return response, err
	}
	metadata := map[string]any{}
	_ = json.Unmarshal(payload, &metadata)
	metadata["softDeletePolicy"] = map[string]string{"retentionDurationSeconds": string(request.SoftDelete.Seconds)}
	project, _ := p.index.Get(projectResource(scope.Project))
	metadata["projectNumber"] = project.Number
	metadata["metageneration"] = "1"
	metadata["etag"] = objectETag(request.Name, "1")
	encoded, _ := json.Marshal(metadata)
	if err := p.catalog.PublishBucket(scope, Bucket{Name: request.Name, Location: request.Location, Metadata: string(encoded)}); err != nil {
		// A competing project may have claimed the global name while this backend
		// created its empty bucket. Roll back only this scope before returning conflict.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		rollback, rollbackErr := b.forward(cleanupCtx, "DELETE", "/storage/v1/b/"+url.PathEscape(request.Name), nil, nil, nil)
		if rollbackErr != nil || rollback.Status < 200 || rollback.Status >= 300 {
			return nil, status.Error(codes.Internal, "Bucket publication failed; reset this Storage scope before retrying")
		}
		return wireError(409, "conflict", "Bucket name is already owned"), nil
	}
	p.bucketMetadata(scope, response.JSON.(map[string]any))
	return response, nil
}
func (p *Provider) bucketMetadata(scope state.Scope, value map[string]any) {
	name, _ := value["name"].(string)
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
