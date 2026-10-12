// Package vertexai serves the Vertex AI generative endpoints of Google
// publisher models: generateContent, streamGenerateContent, countTokens, and
// embeddings through predict or embedContent. Answers come from a local
// backend selected by explicit configuration: a deterministic stub (default)
// or an Ollama server. Nothing calls Google, and no answer comes from Gemini.
package vertexai

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Method IDs are the aiplatform discovery IDs the generated routes use.
const (
	methodPrefix   = "aiplatform.projects.locations.publishers.models."
	methodGenerate = methodPrefix + "generateContent"
	methodStream   = methodPrefix + "streamGenerateContent"
	methodCount    = methodPrefix + "countTokens"
	methodPredict  = methodPrefix + "predict"
	methodEmbed    = methodPrefix + "embedContent"
)

// permissionPredict is the documented permission for publisher-model
// inference, checked on the project.
const permissionPredict = "aiplatform.endpoints.predict"

// methods lists every served method.
var methods = []string{methodGenerate, methodStream, methodCount, methodPredict, methodEmbed}

// Backend names accepted by Config.
const (
	BackendStub   = "stub"
	BackendOllama = "ollama"
)

// Config selects the model backend explicitly; the zero value is the stub.
type Config struct {
	Backend   string
	OllamaURL string
	// Models maps Vertex AI model IDs to Ollama models; "*" is a fallback.
	Models map[string]string
}

// Provider construction sentinels.
var (
	errBackend       = errors.New("vertex AI backend must be stub or ollama; Ollama settings require -vertex-backend=ollama")
	errProviderShape = errors.New("vertexai requires registry, index, and iam")
)

// Provider authorizes each call and hands it to the configured backend.
type Provider struct {
	index   *state.Index
	engine  *iam.Engine
	model   Model
	backend string
	clock   func() time.Time
}

// configure builds the configured backend.
func (p *Provider) configure(config Config) error {
	switch config.Backend {
	case "", BackendStub:
		if config.OllamaURL != "" || len(config.Models) > 0 {
			return errBackend
		}
		p.model, p.backend = Stub{}, BackendStub
	case BackendOllama:
		model, err := NewOllama(config.OllamaURL, config.Models)
		if err != nil {
			return err
		}
		p.model, p.backend = model, BackendOllama
	default:
		return errBackend
	}
	return nil
}

// New registers the Vertex AI provider. It holds no resource state.
func New(_ context.Context, registry *gateway.Registry, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	if registry == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	p := &Provider{index: index, engine: engine, clock: time.Now}
	if err := p.configure(config); err != nil {
		return nil, err
	}
	for _, method := range methods {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permissionPredict}}); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
		if err := registry.Register(method, p.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	return p, nil
}

// Path shapes.
var (
	projectPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	locationPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	modelPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9._@-]{0,127}$`)
)

// target is one resolved model path.
type target struct {
	project, location, model string
}

// resource is the model's canonical resource name.
func (t target) resource() string {
	return "//aiplatform.googleapis.com/projects/" + t.project + "/locations/" + t.location + "/publishers/google/models/" + t.model
}

// resolve validates the path and maps a project number to its ID.
func (p *Provider) resolve(request *gateway.RESTRequest) (target, error) {
	t := target{project: request.Path["project"], location: request.Path["location"], model: request.Path[keyModel]}
	if !projectPattern.MatchString(t.project) || !locationPattern.MatchString(t.location) || !modelPattern.MatchString(t.model) {
		return t, status.Error(codes.InvalidArgument, "Invalid project, location, or model in the resource name")
	}
	if resource, found := p.index.ProjectByNumber(t.project); found {
		t.project = resource.Scope.Project
	}
	return t, nil
}

// backendError keeps a backend's wire status without adding a new message.
func backendError(err error) error { return status.Convert(err).Err() }

// handle authorizes, then runs one inference method.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Vertex AI request")
	}
	t, err := p.resolve(request)
	if err != nil {
		return nil, err
	}
	call.Project, call.Location, call.Resource = t.project, t.location, t.resource()
	decision, err := p.engine.Authorize(call, iam.Target{Name: "//cloudresourcemanager.googleapis.com/projects/" + t.project}, nil)
	if err != nil {
		return nil, denial(call, t, err)
	}
	response, err := p.execute(call, request, t)
	if err != nil {
		return nil, err
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers.Set("X-Gcp-Local-Decision-Id", decision.ID)
	response.Headers.Set("X-Gcp-Local-Model-Backend", p.backend)
	return response, nil
}

// denial renders an authorization failure in the Vertex AI shape.
func denial(call gateway.Call, t target, err error) error {
	switch status.Code(err) {
	case codes.PermissionDenied:
		return status.Errorf(codes.PermissionDenied, "Permission '%s' denied on resource '%s' (or it may not exist).", permissionPredict, t.resource())
	case codes.Unauthenticated:
		return status.Error(codes.Unauthenticated, "Request is missing required authentication credential. Expected OAuth 2 access token, login cookie or other valid authentication credential.")
	default:
		return fmt.Errorf("authorize %s: %w", call.Method, err)
	}
}

// execute dispatches an authorized call.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, t target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodGenerate, methodStream, methodCount:
		return p.generate(call, request, t)
	case methodPredict:
		return p.predict(call.Context, request, t)
	case methodEmbed:
		return p.embedContent(call.Context, request, t)
	default:
		return nil, gap("this method is not implemented")
	}
}

// generate serves generateContent, streamGenerateContent, and countTokens.
func (p *Provider) generate(call gateway.Call, request *gateway.RESTRequest, t target) (*gateway.RESTResponse, error) {
	var body GenerateRequest
	if err := decodeBody(request.JSON, &body); err != nil {
		return nil, err
	}
	if err := body.validate(); err != nil {
		return nil, err
	}
	version, err := p.model.Version(t.model)
	if err != nil {
		return nil, backendError(err)
	}
	switch call.Method {
	case methodCount:
		return countResponse(&body), nil
	case methodStream:
		return p.stream(call.Context, request.Query.Get("alt") == "sse", t, version, &body)
	}
	result, err := p.model.Generate(call.Context, t.model, &body, nil)
	if err != nil {
		return nil, backendError(err)
	}
	return &gateway.RESTResponse{JSON: p.render(result.Parts, &result, version, newResponseID())}, nil
}

// newResponseID returns a fresh response identifier.
func newResponseID() string {
	raw := make([]byte, 12)
	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

// render builds one response; a non-nil result marks the final chunk.
func (p *Provider) render(parts []Part, result *Result, version, id string) GenerateResponse {
	candidate := Candidate{Content: Content{Role: roleModel, Parts: parts}}
	response := GenerateResponse{Candidates: []Candidate{candidate}, ModelVersion: version, CreateTime: p.clock().UTC().Format(time.RFC3339Nano), ResponseID: id}
	if result != nil {
		response.Candidates[0].FinishReason = result.FinishReason
		response.UsageMetadata = &Usage{PromptTokenCount: result.PromptTokens, CandidatesTokenCount: result.OutputTokens, TotalTokenCount: result.PromptTokens + result.OutputTokens, TrafficType: "ON_DEMAND"}
	}
	return response
}

// countResponse renders countTokens with the local approximation.
func countResponse(body *GenerateRequest) *gateway.RESTResponse {
	total := countRequestTokens(body)
	return &gateway.RESTResponse{JSON: map[string]any{
		"totalTokens":             total,
		"totalBillableCharacters": billableCharacters(body),
		"promptTokensDetails":     []any{map[string]any{"modality": "TEXT", "tokenCount": total}},
	}}
}

// embeddingModel reports whether a model ID names a text embedding model.
func embeddingModel(model string) bool { return strings.Contains(model, "embedding") }

// maxDimensions is the largest supported output size.
const maxDimensions = 3072

// predictRequest is the text-embedding predict body.
type predictRequest struct {
	Instances []struct {
		Content  *string `json:"content"`
		TaskType string  `json:"task_type,omitempty"`
		Title    string  `json:"title,omitempty"`
	} `json:"instances"`
	Parameters struct {
		OutputDimensionality int   `json:"outputDimensionality,omitempty"`
		AutoTruncate         *bool `json:"autoTruncate,omitempty"`
	} `json:"parameters"`
}

// texts validates the instances and returns their content.
func (r *predictRequest) texts() ([]string, error) {
	if len(r.Instances) == 0 {
		return nil, invalid("instances must not be empty")
	}
	texts := make([]string, len(r.Instances))
	for i, instance := range r.Instances {
		if instance.Content == nil {
			return nil, invalid("Each instance needs a content string")
		}
		texts[i] = *instance.Content
	}
	return texts, nil
}

// checkDimensions rejects an output size outside the supported range.
func checkDimensions(dimensions int) error {
	if dimensions < 0 || dimensions > maxDimensions {
		return invalid("outputDimensionality is out of range")
	}
	return nil
}

// embed runs the backend for texts after confirming the model is served.
func (p *Provider) embed(ctx context.Context, model string, texts []string, dimensions int) ([][]float64, error) {
	if err := checkDimensions(dimensions); err != nil {
		return nil, err
	}
	if _, err := p.model.Version(model); err != nil {
		return nil, backendError(err)
	}
	vectors, err := p.model.Embed(ctx, model, texts, dimensions)
	if err != nil {
		return nil, backendError(err)
	}
	return vectors, nil
}

// predict serves text-embedding models through the predict method.
func (p *Provider) predict(ctx context.Context, request *gateway.RESTRequest, t target) (*gateway.RESTResponse, error) {
	if !embeddingModel(t.model) {
		return nil, gap("predict is served only for text embedding models")
	}
	var body predictRequest
	if err := json.Unmarshal(request.JSON, &body); err != nil {
		return nil, invalid("Invalid JSON payload received.")
	}
	texts, err := body.texts()
	if err != nil {
		return nil, err
	}
	vectors, err := p.embed(ctx, t.model, texts, body.Parameters.OutputDimensionality)
	if err != nil {
		return nil, err
	}
	predictions := make([]any, len(vectors))
	characters := 0
	for i, vector := range vectors {
		characters += len([]rune(strings.Join(strings.Fields(texts[i]), "")))
		predictions[i] = map[string]any{"embeddings": map[string]any{
			"values":     vector,
			"statistics": map[string]any{"truncated": false, "token_count": textTokens(texts[i])},
		}}
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"predictions": predictions,
		"metadata":    map[string]any{"billableCharacterCount": characters},
	}}, nil
}

// embedConfig carries the embedContent options, top-level or nested.
type embedConfig struct {
	TaskType             string `json:"taskType,omitempty"`
	Title                string `json:"title,omitempty"`
	OutputDimensionality int    `json:"outputDimensionality,omitempty"`
	AutoTruncate         *bool  `json:"autoTruncate,omitempty"`
}

// embedRequest is the embedContent body.
type embedRequest struct {
	Content *Content `json:"content"`
	embedConfig
	EmbedContentConfig *embedConfig `json:"embedContentConfig,omitempty"`
}

// text validates the content and returns its text and requested size.
func (r *embedRequest) text() (string, int, error) {
	if r.Content == nil || len(r.Content.Parts) == 0 {
		return "", 0, invalid("content must include at least one part")
	}
	for _, part := range r.Content.Parts {
		if part.Text == "" {
			return "", 0, gap("embedContent supports text parts only")
		}
	}
	dimensions := r.OutputDimensionality
	if r.EmbedContentConfig != nil && r.EmbedContentConfig.OutputDimensionality != 0 {
		dimensions = r.EmbedContentConfig.OutputDimensionality
	}
	return joinText(r.Content.Parts), dimensions, nil
}

// embedContent serves the Gemini embedding method.
func (p *Provider) embedContent(ctx context.Context, request *gateway.RESTRequest, t target) (*gateway.RESTResponse, error) {
	var body embedRequest
	if err := decodeBody(request.JSON, &body); err != nil {
		return nil, err
	}
	text, dimensions, err := body.text()
	if err != nil {
		return nil, err
	}
	vectors, err := p.embed(ctx, t.model, []string{text}, dimensions)
	if err != nil {
		return nil, err
	}
	tokens := textTokens(text)
	return &gateway.RESTResponse{JSON: map[string]any{
		"embedding":     map[string]any{"values": vectors[0]},
		"usageMetadata": map[string]any{"promptTokenCount": tokens, "totalTokenCount": tokens},
		"truncated":     false,
	}}, nil
}
