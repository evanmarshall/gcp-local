package vertexai

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"
)

// Result is one complete model answer.
type Result struct {
	Parts        []Part
	FinishReason string
	PromptTokens int
	OutputTokens int
}

// Model is a local model backend. Generate streams partial parts through emit
// when it is non-nil and always returns the complete answer.
type Model interface {
	// Version names the backend model serving a Vertex AI model ID, or
	// fails with NOT_FOUND when none is configured.
	Version(model string) (string, error)
	Generate(ctx context.Context, model string, request *GenerateRequest, emit func([]Part) error) (Result, error)
	Embed(ctx context.Context, model string, texts []string, dimensions int) ([][]float64, error)
}

// stubMarker prefixes every stub text so no caller mistakes it for Gemini.
const stubMarker = "[gcp-local stub]"

// finishStop is the normal finish reason.
const finishStop = "STOP"

// Stub is the default backend: deterministic answers derived only from the
// request, so tests are stable.
type Stub struct{}

// Version names the stub for every model ID.
func (Stub) Version(string) (string, error) { return "gcp-local-stub", nil }

// Generate answers deterministically: a function call when functions are
// declared and no function result is present yet, JSON when JSON output is
// requested, otherwise a marked echo of the last user text.
func (Stub) Generate(_ context.Context, model string, request *GenerateRequest, emit func([]Part) error) (Result, error) {
	result := Result{FinishReason: finishStop, PromptTokens: countRequestTokens(request)}
	if call, ok := stubFunctionCall(request); ok {
		result.Parts = []Part{{FunctionCall: &call}}
	} else {
		result.Parts = []Part{{Text: stubText(model, request, requestDigest(request))}}
	}
	if limit := request.config().MaxOutputTokens; limit > 0 && result.Parts[0].Text != "" {
		if text, cut := truncateTokens(result.Parts[0].Text, limit); cut {
			result.Parts[0].Text, result.FinishReason = text, "MAX_TOKENS"
		}
	}
	for _, part := range result.Parts {
		result.OutputTokens += partTokens(part)
	}
	if emit == nil {
		return result, nil
	}
	for _, chunk := range stubChunks(result.Parts) {
		if err := emit(chunk); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}

// stubChunks splits a text answer into three deterministic pieces; a function
// call is one chunk.
func stubChunks(parts []Part) [][]Part {
	if len(parts) != 1 || parts[0].Text == "" {
		return [][]Part{parts}
	}
	words := strings.SplitAfter(parts[0].Text, " ")
	size := max(1, (len(words)+2)/3)
	var chunks [][]Part
	for start := 0; start < len(words); start += size {
		end := min(start+size, len(words))
		chunks = append(chunks, []Part{{Text: strings.Join(words[start:end], "")}})
	}
	return chunks
}

// stubText renders the answer for a request without a pending function call.
func stubText(model string, request *GenerateRequest, digest string) string {
	config := request.config()
	last := lastUserText(request)
	if response, ok := lastFunctionResponse(request); ok {
		return fmt.Sprintf("%s %s received the %s result %s (digest %s).", stubMarker, model, response.Name, clip(jsonText(response.Response), 200), digest)
	}
	switch strings.ToLower(config.ResponseMimeType) {
	case "application/json":
		schema := config.ResponseJSONSchema
		if schema == nil {
			schema = config.ResponseSchema
		}
		if schema != nil {
			return jsonText(schemaValue(schema, "value", 0))
		}
		return jsonText(map[string]any{"stub": true, keyModel: model, "digest": digest, "prompt": clip(last, 200)})
	case "text/x.enum":
		if values := enumOf(config.ResponseSchema); len(values) > 0 {
			return values[0]
		}
	}
	return fmt.Sprintf("%s %s received %d prompt tokens (digest %s). Last user message: %q", stubMarker, model, countRequestTokens(request), digest, clip(last, 200))
}

// callable lists the declarations a call may target under the calling mode.
func callable(request *GenerateRequest) []FunctionDeclaration {
	declarations := request.declarations()
	mode, allowed := request.callingMode()
	if len(declarations) == 0 || mode == "NONE" {
		return nil
	}
	if _, answered := lastFunctionResponse(request); answered && mode != "ANY" {
		return nil
	}
	if len(allowed) == 0 {
		return declarations
	}
	var candidates []FunctionDeclaration
	for _, declaration := range declarations {
		if slices.Contains(allowed, declaration.Name) {
			candidates = append(candidates, declaration)
		}
	}
	return candidates
}

// stubFunctionCall picks the function a stub calls, if any: the first one
// named in the last user text, else the first callable one.
func stubFunctionCall(request *GenerateRequest) (FunctionCall, bool) {
	candidates := callable(request)
	if len(candidates) == 0 {
		return FunctionCall{}, false
	}
	chosen := candidates[0]
	text := strings.ToLower(lastUserText(request))
	for _, declaration := range candidates {
		if strings.Contains(text, strings.ToLower(declaration.Name)) {
			chosen = declaration
			break
		}
	}
	args, ok := schemaValue(chosen.schema(), chosen.Name, 0).(map[string]any)
	if !ok {
		args = map[string]any{}
	}
	return FunctionCall{Name: chosen.Name, Args: args}, true
}

// schemaKind reads a schema's type name in lower case.
func schemaKind(schema map[string]any) string {
	if kinds, ok := schema[keyType].([]any); ok && len(kinds) > 0 {
		kind, _ := kinds[0].(string)
		return strings.ToLower(kind)
	}
	kind, _ := schema[keyType].(string)
	return strings.ToLower(kind)
}

// scalarValue builds the value of a non-object schema type.
func scalarValue(schema map[string]any, kind, name string, depth int) (any, bool) {
	switch kind {
	case "string":
		return "stub-" + name, true
	case "integer":
		return 1, true
	case "number":
		return 1.5, true
	case "boolean":
		return true, true
	case "array":
		items, _ := schema["items"].(map[string]any)
		return []any{schemaValue(items, name, depth+1)}, true
	case "null":
		return nil, true
	default:
		return nil, false
	}
}

// schemaValue builds a deterministic value matching an OpenAPI or JSON schema.
func schemaValue(schema map[string]any, name string, depth int) any {
	if schema == nil || depth > 8 {
		return map[string]any{}
	}
	if values := enumOf(schema); len(values) > 0 {
		return values[0]
	}
	if value, ok := scalarValue(schema, schemaKind(schema), name, depth); ok {
		return value
	}
	properties, _ := schema["properties"].(map[string]any)
	names := make([]string, 0, len(properties))
	for key := range properties {
		names = append(names, key)
	}
	sort.Strings(names)
	value := map[string]any{}
	for _, key := range names {
		child, _ := properties[key].(map[string]any)
		value[key] = schemaValue(child, key, depth+1)
	}
	return value
}

// enumOf reads a schema's string enum.
func enumOf(schema map[string]any) []string {
	raw, _ := schema["enum"].([]any)
	values := make([]string, 0, len(raw))
	for _, item := range raw {
		if text, ok := item.(string); ok {
			values = append(values, text)
		}
	}
	return values
}

// lastUserText is the text of the last user turn.
func lastUserText(request *GenerateRequest) string {
	for _, content := range slices.Backward(request.Contents) {
		if content.Role != "" && content.Role != roleUser {
			continue
		}
		if text := joinText(content.Parts); text != "" {
			return text
		}
	}
	return ""
}

// lastFunctionResponse finds a function result in the final turn.
func lastFunctionResponse(request *GenerateRequest) (FunctionResponse, bool) {
	if len(request.Contents) == 0 {
		return FunctionResponse{}, false
	}
	for _, part := range request.Contents[len(request.Contents)-1].Parts {
		if part.FunctionResponse != nil {
			return *part.FunctionResponse, true
		}
	}
	return FunctionResponse{}, false
}

// requestDigest is a short hash of the canonical request body.
func requestDigest(request *GenerateRequest) string {
	sum := sha256.Sum256([]byte(jsonText(request)))
	return hex.EncodeToString(sum[:4])
}

// clip shortens text to at most limit runes.
func clip(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit]) + "..."
}

// Embed returns deterministic unit vectors derived from each text.
func (Stub) Embed(_ context.Context, model string, texts []string, dimensions int) ([][]float64, error) {
	if dimensions == 0 {
		dimensions = defaultDimensions(model)
	}
	vectors := make([][]float64, len(texts))
	for i, text := range texts {
		vectors[i] = stubVector(text, dimensions)
	}
	return vectors, nil
}

// defaultDimensions is the published default output size of each family.
func defaultDimensions(model string) int {
	if strings.HasPrefix(model, "gemini-embedding") {
		return 3072
	}
	return 768
}

// stubVector expands SHA-256 of the text into a unit vector.
func stubVector(text string, dimensions int) []float64 {
	vector := make([]float64, dimensions)
	var norm float64
	var block [sha256.Size]byte
	for i := range dimensions {
		// Each block of eight components comes from one hash.
		if i%8 == 0 {
			block = sha256.Sum256(fmt.Appendf(nil, "%d\x00%s", i/8, text))
		}
		value := float64(binary.BigEndian.Uint32(block[(i%8)*4:]))/float64(math.MaxUint32)*2 - 1
		vector[i] = value
		norm += value * value
	}
	norm = math.Sqrt(norm)
	for i := range vector {
		vector[i] /= norm
	}
	return vector
}
