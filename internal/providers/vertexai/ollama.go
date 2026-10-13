package vertexai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ollama sends inference to an explicitly configured Ollama server. Vertex AI
// model IDs map to Ollama models through an explicit alias table; "*" is an
// optional explicit fallback.
type Ollama struct {
	base   *url.URL
	models map[string]string
	client *http.Client
}

// Ollama configuration sentinels.
var (
	errOllamaURL    = errors.New("the Ollama URL must be an absolute http(s) URL without credentials, query, or a Google host")
	errOllamaModels = errors.New("the Ollama backend needs at least one model alias")
)

// ollamaPrefix marks the model version of an Ollama answer.
const ollamaPrefix = "ollama/"

// Ollama wire keys.
const (
	keyFunction = "function"
	keyType     = "type"
)

// ollamaURL validates the server URL.
func ollamaURL(raw string) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, errOllamaURL
	}
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" || googleHost(base.Hostname()) {
		return nil, errOllamaURL
	}
	return base, nil
}

// NewOllama validates the server URL and alias table.
func NewOllama(raw string, models map[string]string) (*Ollama, error) {
	base, err := ollamaURL(raw)
	if err != nil {
		return nil, err
	}
	if len(models) == 0 {
		return nil, errOllamaModels
	}
	aliases := map[string]string{}
	for model, alias := range models {
		if model == "" || alias == "" {
			return nil, errOllamaModels
		}
		aliases[model] = alias
	}
	transport := &http.Transport{
		// Never route through an ambient proxy.
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		ResponseHeaderTimeout: 5 * time.Minute, // first-use model loads are slow
		MaxIdleConnsPerHost:   4,
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	return &Ollama{base: base, models: aliases, client: client}, nil
}

// googleHost refuses Google API hosts: the provider never calls real Google.
func googleHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, suffix := range []string{"googleapis.com", "google.com", "googleusercontent.com", "gstatic.com"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

// Version maps a Vertex AI model ID to its Ollama model.
func (o *Ollama) Version(model string) (string, error) {
	if alias, ok := o.models[model]; ok {
		return ollamaPrefix + alias, nil
	}
	if alias, ok := o.models["*"]; ok {
		return ollamaPrefix + alias, nil
	}
	return "", status.Errorf(codes.NotFound, "Publisher Model `publishers/google/models/%s` has no local Ollama alias. Map it with -vertex-model %s=OLLAMA_MODEL.", model, model)
}

// alias returns the bare Ollama model name.
func (o *Ollama) alias(model string) (string, error) {
	version, err := o.Version(model)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(version, ollamaPrefix), nil
}

// ollamaMessage is one Ollama chat message.
type ollamaMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content"`
	Images    []string         `json:"images,omitempty"`
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
	ToolName  string           `json:"tool_name,omitempty"`
}

// ollamaToolCall is one function call in Ollama's shape.
type ollamaToolCall struct {
	Function struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	} `json:"function"`
}

// ollamaChat is one /api/chat response or stream line.
type ollamaChat struct {
	Message         ollamaMessage `json:"message"`
	Done            bool          `json:"done"`
	DoneReason      string        `json:"done_reason"`
	PromptEvalCount int           `json:"prompt_eval_count"`
	EvalCount       int           `json:"eval_count"`
	Error           string        `json:"error"`
}

// chatBody converts a Vertex AI request into an Ollama chat request.
func chatBody(alias string, request *GenerateRequest, stream bool) map[string]any {
	var messages []ollamaMessage
	if request.SystemInstruction != nil {
		messages = append(messages, ollamaMessage{Role: "system", Content: joinText(request.SystemInstruction.Parts)})
	}
	for _, content := range request.Contents {
		messages = append(messages, convertTurn(content)...)
	}
	body := map[string]any{keyModel: alias, "messages": messages, "stream": stream}
	if tools := ollamaTools(request); len(tools) > 0 {
		body["tools"] = tools
	}
	config := request.config()
	if options := ollamaOptions(config); len(options) > 0 {
		body["options"] = options
	}
	if format, ok := ollamaFormat(config); ok {
		body["format"] = format
	}
	return body
}

// ollamaTools converts the callable function declarations.
func ollamaTools(request *GenerateRequest) []map[string]any {
	mode, allowed := request.callingMode()
	if mode == modeNone {
		return nil
	}
	var tools []map[string]any
	for _, declaration := range request.declarations() {
		if len(allowed) > 0 && !slices.Contains(allowed, declaration.Name) {
			continue
		}
		function := map[string]any{"name": declaration.Name, "description": declaration.Description}
		if schema := declaration.schema(); schema != nil {
			function["parameters"] = lowerSchema(schema)
		}
		tools = append(tools, map[string]any{keyType: keyFunction, keyFunction: function})
	}
	return tools
}

// ollamaOptions maps the sampling options.
func ollamaOptions(config GenerationConfig) map[string]any {
	options := map[string]any{}
	if config.Temperature != nil {
		options["temperature"] = *config.Temperature
	}
	if config.TopP != nil {
		options["top_p"] = *config.TopP
	}
	if config.TopK != nil {
		options["top_k"] = *config.TopK
	}
	if config.MaxOutputTokens > 0 {
		options["num_predict"] = config.MaxOutputTokens
	}
	if len(config.StopSequences) > 0 {
		options["stop"] = config.StopSequences
	}
	if config.Seed != nil {
		options["seed"] = *config.Seed
	}
	return options
}

// ollamaFormat maps JSON output mode to Ollama's format field.
func ollamaFormat(config GenerationConfig) (any, bool) {
	if !strings.EqualFold(config.ResponseMimeType, "application/json") {
		return nil, false
	}
	switch {
	case config.ResponseJSONSchema != nil:
		return config.ResponseJSONSchema, true
	case config.ResponseSchema != nil:
		return lowerSchema(config.ResponseSchema), true
	default:
		return "json", true
	}
}

// convertTurn maps one Vertex AI turn to Ollama messages.
func convertTurn(content Content) []ollamaMessage {
	var messages []ollamaMessage
	message := ollamaMessage{Role: roleUser, Content: joinText(content.Parts)}
	if content.Role == roleModel {
		message.Role = "assistant"
	}
	for _, part := range content.Parts {
		switch {
		case part.FunctionCall != nil:
			var call ollamaToolCall
			call.Function.Name, call.Function.Arguments = part.FunctionCall.Name, part.FunctionCall.Args
			message.ToolCalls = append(message.ToolCalls, call)
		case part.FunctionResponse != nil:
			messages = append(messages, ollamaMessage{Role: "tool", Content: jsonText(part.FunctionResponse.Response), ToolName: part.FunctionResponse.Name})
		case part.InlineData != nil && strings.HasPrefix(part.InlineData.MimeType, "image/"):
			message.Images = append(message.Images, part.InlineData.Data)
		}
	}
	if message.Content != "" || len(message.ToolCalls) > 0 || len(message.Images) > 0 {
		messages = append([]ollamaMessage{message}, messages...)
	}
	return messages
}

// joinText concatenates the non-thought text parts.
func joinText(parts []Part) string {
	var texts []string
	for _, part := range parts {
		if part.Text != "" && !part.Thought {
			texts = append(texts, part.Text)
		}
	}
	return strings.Join(texts, "\n")
}

// lowerProperties converts every property schema.
func lowerProperties(properties map[string]any) map[string]any {
	converted := map[string]any{}
	for name, child := range properties {
		if schema, ok := child.(map[string]any); ok {
			converted[name] = lowerSchema(schema)
		}
	}
	return converted
}

// lowerSchema converts OpenAPI upper-case type names to JSON Schema.
func lowerSchema(schema map[string]any) map[string]any {
	converted := map[string]any{}
	for key, value := range schema {
		switch typed := value.(type) {
		case string:
			if key == keyType {
				typed = strings.ToLower(typed)
			}
			converted[key] = typed
		case map[string]any:
			if key == "properties" {
				converted[key] = lowerProperties(typed)
			} else {
				converted[key] = lowerSchema(typed)
			}
		default:
			converted[key] = value
		}
	}
	return converted
}

// post sends one JSON request to the Ollama server.
func (o *Ollama) post(ctx context.Context, path string, body any) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, status.Error(codes.Internal, "Encode the Ollama request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, o.base.JoinPath(path).String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, status.Error(codes.Internal, "Build the Ollama request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := o.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Errorf(codes.Unavailable, "The local Ollama backend at %s is unreachable", o.base.Redacted())
	}
	if response.StatusCode == http.StatusOK {
		return response, nil
	}
	defer response.Body.Close()
	var failure struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&failure)
	if response.StatusCode == http.StatusNotFound {
		return nil, status.Errorf(codes.FailedPrecondition, "The local Ollama backend has no such model: %s", failure.Error)
	}
	return nil, status.Errorf(codes.Unavailable, "The local Ollama backend failed with HTTP %d: %s", response.StatusCode, failure.Error)
}

// chatAccumulator gathers a chat answer across stream lines.
type chatAccumulator struct {
	text   strings.Builder
	calls  []Part
	result Result
	done   bool
}

// add folds one line into the answer and returns its new parts.
func (a *chatAccumulator) add(chunk ollamaChat) []Part {
	var parts []Part
	if chunk.Message.Content != "" {
		a.text.WriteString(chunk.Message.Content)
		parts = append(parts, Part{Text: chunk.Message.Content})
	}
	for _, call := range chunk.Message.ToolCalls {
		args := call.Function.Arguments
		if args == nil {
			args = map[string]any{}
		}
		part := Part{FunctionCall: &FunctionCall{Name: call.Function.Name, Args: args}}
		a.calls = append(a.calls, part)
		parts = append(parts, part)
	}
	if chunk.Done {
		a.done = true
		if chunk.DoneReason == "length" {
			a.result.FinishReason = "MAX_TOKENS"
		}
		a.result.PromptTokens, a.result.OutputTokens = chunk.PromptEvalCount, chunk.EvalCount
	}
	return parts
}

// finish assembles the complete answer, filling counts Ollama omitted.
func (a *chatAccumulator) finish(request *GenerateRequest) Result {
	result := a.result
	if a.text.Len() > 0 {
		result.Parts = append(result.Parts, Part{Text: a.text.String()})
	}
	result.Parts = append(result.Parts, a.calls...)
	if len(result.Parts) == 0 {
		result.Parts = []Part{{Text: ""}}
	}
	if result.PromptTokens == 0 {
		result.PromptTokens = countRequestTokens(request)
	}
	if result.OutputTokens == 0 {
		for _, part := range result.Parts {
			result.OutputTokens += partTokens(part)
		}
	}
	return result
}

// parseChatLine decodes one stream line; a blank line yields nothing.
func parseChatLine(raw []byte) (ollamaChat, bool, error) {
	var chunk ollamaChat
	line := bytes.TrimSpace(raw)
	if len(line) == 0 {
		return chunk, false, nil
	}
	if err := json.Unmarshal(line, &chunk); err != nil {
		return chunk, false, status.Error(codes.Unavailable, "The local Ollama backend returned malformed JSON")
	}
	if chunk.Error != "" {
		return chunk, false, status.Errorf(codes.Unavailable, "The local Ollama backend failed: %s", chunk.Error)
	}
	return chunk, true, nil
}

// readChat consumes newline-delimited chat lines until the final one.
func readChat(ctx context.Context, body io.Reader, emit func([]Part) error) (*chatAccumulator, error) {
	answer := &chatAccumulator{result: Result{FinishReason: finishStop}}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for !answer.done && scanner.Scan() {
		chunk, ok, err := parseChatLine(scanner.Bytes())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if parts := answer.add(chunk); emit != nil && len(parts) > 0 {
			if err := emit(parts); err != nil {
				return nil, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "The local Ollama response ended early")
	}
	return answer, nil
}

// Generate runs one chat completion, streaming when emit is non-nil.
func (o *Ollama) Generate(ctx context.Context, model string, request *GenerateRequest, emit func([]Part) error) (Result, error) {
	alias, err := o.alias(model)
	if err != nil {
		return Result{}, err
	}
	response, err := o.post(ctx, "/api/chat", chatBody(alias, request, emit != nil))
	if err != nil {
		return Result{}, err
	}
	defer response.Body.Close()
	answer, err := readChat(ctx, response.Body, emit)
	if err != nil {
		return Result{}, err
	}
	return answer.finish(request), nil
}

// Embed calls /api/embed and truncates to the requested size.
func (o *Ollama) Embed(ctx context.Context, model string, texts []string, dimensions int) ([][]float64, error) {
	alias, err := o.alias(model)
	if err != nil {
		return nil, err
	}
	response, err := o.post(ctx, "/api/embed", map[string]any{keyModel: alias, "input": texts})
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var decoded struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&decoded); err != nil || len(decoded.Embeddings) != len(texts) {
		return nil, status.Error(codes.Unavailable, "The local Ollama backend returned a malformed embedding response")
	}
	for i, vector := range decoded.Embeddings {
		if dimensions > 0 && dimensions < len(vector) {
			decoded.Embeddings[i] = vector[:dimensions]
		}
	}
	return decoded.Embeddings, nil
}

// String describes the backend without credentials.
func (o *Ollama) String() string { return fmt.Sprintf("ollama(%s)", o.base.Redacted()) }
