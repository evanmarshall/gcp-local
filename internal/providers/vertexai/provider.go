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
	"regexp"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
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
	gate    provider.Gate
	model   Model
	backend string
	clock   func() time.Time
}

// The recorded denial (vertexai-inference-authorization) names the model, with
// an ErrorInfo; GCP's troubleshooter link is not reproduced. Vertex AI
// supplies no resource attributes to IAM Conditions (recorded:
// logging-conditional-access), so none are set.
const (
	modelDenial = "Permission '%s' denied on resource '//aiplatform.googleapis.com/%s' (or it may not exist)."
	modelDomain = "aiplatform.googleapis.com"
)

// The publisher models GCP serves in us-central1 (recorded: countTokens on
// each in vertexai-inference-authorization). Any other ID is GCP's 404.
var (
	generativeModels = names("gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.5-flash-image")
	embeddingModels  = names("text-embedding-005", "text-embedding-004", "text-multilingual-embedding-002", "gemini-embedding-001")
)

// modelNotFound is GCP's 404 for a model it doesn't serve (recorded).
func modelNotFound(t target) error {
	return status.Errorf(codes.NotFound, "Publisher model `%s` was not found or your project does not have access to it. "+
		"Ensure you are using a valid model name and that the model is available in the specified region. "+
		"For more information, see: https://docs.cloud.google.com/gemini-enterprise-agent-platform/resources/locations.", t.name())
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
	p := &Provider{index: index, engine: engine, clock: time.Now,
		gate: provider.Gate{Engine: engine, Message: modelDenial, Domain: modelDomain}}
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

// name is the model's relative resource name.
func (t target) name() string {
	return "projects/" + t.project + "/locations/" + t.location + "/publishers/google/models/" + t.model
}

// resource is the model's canonical resource name.
func (t target) resource() string { return "//aiplatform.googleapis.com/" + t.name() }

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
	decision, err := p.gate.Check(call, iam.Target{Name: provider.ProjectResource(t.project)}, t.name())
	if err != nil {
		return nil, err
	}
	if !generativeModels[t.model] && !embeddingModels[t.model] {
		return nil, modelNotFound(t)
	}
	response, err := p.execute(call, request, t)
	if err != nil {
		return nil, err
	}
	response = provider.WithDecision(response, decision)
	response.Headers.Set("X-Gcp-Local-Model-Backend", p.backend)
	return response, nil
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

// errEmbeddingCount is GCP's countTokens refusal on an embedding model given
// contents (recorded).
var errEmbeddingCount = provider.Invalid("Should provide instances for text model prediction.")

// generationBody decodes a generation or countTokens body. On an embedding
// model, generation is a gap and countTokens is GCP's refusal (recorded).
func generationBody(method string, raw []byte, model string) (GenerateRequest, error) {
	if method != methodCount {
		var body GenerateRequest
		if embeddingModels[model] {
			return body, gap(strings.TrimPrefix(method, methodPrefix) + " on an embedding model")
		}
		return body, decodeBody(raw, &body, generateFields)
	}
	var counted countRequest
	if err := decodeBody(raw, &counted, countFields); err != nil {
		return GenerateRequest{}, err
	}
	if embeddingModels[model] {
		return GenerateRequest{}, errEmbeddingCount
	}
	return counted.generate(), nil
}

// generate serves generateContent, streamGenerateContent, and countTokens.
func (p *Provider) generate(call gateway.Call, request *gateway.RESTRequest, t target) (*gateway.RESTResponse, error) {
	body, err := generationBody(call.Method, request.JSON, t.model)
	if err != nil {
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

// maxDimensions is the largest supported output size.
const maxDimensions = 3072

// predictRequest is the PredictRequest body. Instances and parameters are
// google.protobuf.Value: GCP accepts keys a model doesn't read (recorded), so
// they are read leniently; the embedding stub reads content and
// outputDimensionality, and Ollama the content.
type predictRequest struct {
	Instances  []json.RawMessage `json:"instances"`
	Parameters json.RawMessage   `json:"parameters"`
}

// texts validates the instances and returns their content.
func (r *predictRequest) texts() ([]string, error) {
	if len(r.Instances) == 0 {
		return nil, provider.Invalid("instances must not be empty")
	}
	texts := make([]string, len(r.Instances))
	for i, raw := range r.Instances {
		var instance struct {
			Content *string `json:"content"`
		}
		if json.Unmarshal(raw, &instance) != nil || instance.Content == nil {
			return nil, provider.Invalid("Each instance needs a content string")
		}
		texts[i] = *instance.Content
	}
	return texts, nil
}

// dimensions reads parameters.outputDimensionality; 0 means the model's size.
func (r *predictRequest) dimensions() (int, error) {
	var parameters struct {
		OutputDimensionality int `json:"outputDimensionality"`
	}
	if len(r.Parameters) > 0 && json.Unmarshal(r.Parameters, &parameters) != nil {
		return 0, provider.Invalid("Invalid parameters")
	}
	return parameters.OutputDimensionality, nil
}

// checkDimensions rejects an output size outside the supported range.
func checkDimensions(dimensions int) error {
	if dimensions < 0 || dimensions > maxDimensions {
		return provider.Invalid("outputDimensionality is out of range")
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
	if !embeddingModels[t.model] {
		return nil, gap("predict is served only for text embedding models")
	}
	var body predictRequest
	if err := provider.DecodeJSON(request.JSON, "", &body, gap, provider.Fields{}); err != nil {
		return nil, err
	}
	texts, err := body.texts()
	if err != nil {
		return nil, err
	}
	dimensions, err := body.dimensions()
	if err != nil {
		return nil, err
	}
	vectors, err := p.embed(ctx, t.model, texts, dimensions)
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

// embedConfig carries the embedContent options, top-level or nested. The
// stub's vectors don't depend on the task type or title, and it never
// truncates, so it reads only the size.
type embedConfig struct {
	TaskType             string `json:"taskType,omitempty"`
	Title                string `json:"title,omitempty"`
	OutputDimensionality int    `json:"outputDimensionality,omitempty"`
	AutoTruncate         *bool  `json:"autoTruncate,omitempty"`
}

// embedFields are the embedContent fields embedRequest lacks.
var embedFields = provider.Fields{Unmodeled: append(contentFields("content"),
	"embedContentConfig.documentOcr", "embedContentConfig.audioTrackExtraction", "embedContentConfig.mimeType")}

// embedRequest is the embedContent body.
type embedRequest struct {
	Content *Content `json:"content"`
	embedConfig
	EmbedContentConfig *embedConfig `json:"embedContentConfig,omitempty"`
}

// text validates the content and returns its text and requested size.
func (r *embedRequest) text() (string, int, error) {
	if r.Content == nil || len(r.Content.Parts) == 0 {
		return "", 0, provider.Invalid("content must include at least one part")
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
	if !embeddingModels[t.model] {
		return nil, gap("embedContent is served only for embedding models")
	}
	var body embedRequest
	if err := decodeBody(request.JSON, &body, embedFields); err != nil {
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
