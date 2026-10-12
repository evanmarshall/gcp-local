package vertexai

import (
	"encoding/json"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Wire shapes of the Vertex AI generative endpoints (camelCase JSON). Only the
// fields the local backends read are modeled; unsupported features that would
// change the answer are rejected explicitly in validate.

// Turn roles and the shared model key.
const (
	roleUser     = "user"
	roleModel    = "model"
	roleFunction = "function"
	keyModel     = "model"
)

// Content is one conversation turn.
type Content struct {
	Role  string `json:"role,omitempty"`
	Parts []Part `json:"parts"`
}

// Part is one piece of a turn.
type Part struct {
	Text             string            `json:"text,omitempty"`
	Thought          bool              `json:"thought,omitempty"`
	InlineData       *Blob             `json:"inlineData,omitempty"`
	FileData         json.RawMessage   `json:"fileData,omitempty"`
	FunctionCall     *FunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *FunctionResponse `json:"functionResponse,omitempty"`
	ExecutableCode   json.RawMessage   `json:"executableCode,omitempty"`
}

// Blob is inline bytes, base64 encoded on the wire.
type Blob struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

// FunctionCall is a model request to call a declared function.
type FunctionCall struct {
	ID   string         `json:"id,omitempty"`
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// FunctionResponse is the caller's result for one function call.
type FunctionResponse struct {
	ID       string         `json:"id,omitempty"`
	Name     string         `json:"name"`
	Response map[string]any `json:"response"`
}

// Tool declares callable functions; other tool kinds are not modeled.
type Tool struct {
	FunctionDeclarations []FunctionDeclaration `json:"functionDeclarations,omitempty"`
	Other                map[string]json.RawMessage
}

// UnmarshalJSON keeps unmodeled tool kinds so validate can refuse them.
func (t *Tool) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err //nolint:wrapcheck // decode errors render as INVALID_ARGUMENT by the caller
	}
	if declarations, ok := fields["functionDeclarations"]; ok {
		if err := json.Unmarshal(declarations, &t.FunctionDeclarations); err != nil {
			return err //nolint:wrapcheck // decode errors render as INVALID_ARGUMENT by the caller
		}
		delete(fields, "functionDeclarations")
	}
	t.Other = fields
	return nil
}

// FunctionDeclaration is one callable function and its parameter schema.
type FunctionDeclaration struct {
	Name                 string         `json:"name"`
	Description          string         `json:"description,omitempty"`
	Parameters           map[string]any `json:"parameters,omitempty"`
	ParametersJSONSchema map[string]any `json:"parametersJsonSchema,omitempty"`
}

// schema returns the declaration's parameter schema in either form.
func (d FunctionDeclaration) schema() map[string]any {
	if d.ParametersJSONSchema != nil {
		return d.ParametersJSONSchema
	}
	return d.Parameters
}

// ToolConfig restricts function calling.
type ToolConfig struct {
	FunctionCallingConfig *struct {
		Mode                 string   `json:"mode,omitempty"`
		AllowedFunctionNames []string `json:"allowedFunctionNames,omitempty"`
	} `json:"functionCallingConfig,omitempty"`
}

// GenerationConfig carries the sampling and output options.
type GenerationConfig struct {
	Temperature        *float64       `json:"temperature,omitempty"`
	TopP               *float64       `json:"topP,omitempty"`
	TopK               *float64       `json:"topK,omitempty"`
	CandidateCount     int            `json:"candidateCount,omitempty"`
	MaxOutputTokens    int            `json:"maxOutputTokens,omitempty"`
	StopSequences      []string       `json:"stopSequences,omitempty"`
	Seed               *int64         `json:"seed,omitempty"`
	ResponseMimeType   string         `json:"responseMimeType,omitempty"`
	ResponseSchema     map[string]any `json:"responseSchema,omitempty"`
	ResponseJSONSchema map[string]any `json:"responseJsonSchema,omitempty"`
}

// GenerateRequest is the generateContent / streamGenerateContent body.
type GenerateRequest struct {
	Contents          []Content         `json:"contents"`
	SystemInstruction *Content          `json:"systemInstruction,omitempty"`
	Tools             []Tool            `json:"tools,omitempty"`
	ToolConfig        *ToolConfig       `json:"toolConfig,omitempty"`
	GenerationConfig  *GenerationConfig `json:"generationConfig,omitempty"`
	CachedContent     string            `json:"cachedContent,omitempty"`
}

// declarations lists every declared function.
func (r *GenerateRequest) declarations() []FunctionDeclaration {
	var all []FunctionDeclaration
	for _, tool := range r.Tools {
		all = append(all, tool.FunctionDeclarations...)
	}
	return all
}

// callingMode returns the function-calling mode, AUTO by default.
func (r *GenerateRequest) callingMode() (string, []string) {
	if r.ToolConfig == nil || r.ToolConfig.FunctionCallingConfig == nil {
		return "AUTO", nil
	}
	config := r.ToolConfig.FunctionCallingConfig
	mode := strings.ToUpper(config.Mode)
	if mode == "" || mode == "MODE_UNSPECIFIED" {
		mode = "AUTO"
	}
	return mode, config.AllowedFunctionNames
}

// config returns the generation config, never nil.
func (r *GenerateRequest) config() GenerationConfig {
	if r.GenerationConfig == nil {
		return GenerationConfig{}
	}
	return *r.GenerationConfig
}

// freeForm keys hold caller data (function arguments, results, schemas,
// labels) whose keys must not be renamed.
var freeForm = map[string]bool{
	"args": true, "response": true, "parameters": true, "parametersJsonSchema": true,
	"responseSchema": true, "responseJsonSchema": true, "labels": true,
}

// camelKeys renames snake_case structural keys to lowerCamelCase. Google's
// JSON parser accepts both proto field spellings, and google-genai sends some
// fields (parameters_json_schema) in snake_case.
func camelKeys(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		renamed := make(map[string]any, len(typed))
		for key, child := range typed {
			camel := lowerCamel(key)
			if freeForm[camel] {
				renamed[camel] = child
				continue
			}
			renamed[camel] = camelKeys(child)
		}
		return renamed
	case []any:
		for i, child := range typed {
			typed[i] = camelKeys(child)
		}
		return typed
	default:
		return value
	}
}

// lowerCamel converts snake_case to lowerCamelCase.
func lowerCamel(key string) string {
	if !strings.Contains(key, "_") {
		return key
	}
	words := strings.Split(key, "_")
	for i := 1; i < len(words); i++ {
		if words[i] != "" {
			words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
		}
	}
	return strings.Join(words, "")
}

// decodeBody reads a request body, accepting either field spelling.
func decodeBody(raw []byte, target any) error {
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return provider.Invalid("Invalid JSON payload received.")
	}
	normalized, err := json.Marshal(camelKeys(generic))
	if err != nil {
		return provider.Invalid("Invalid JSON payload received.")
	}
	if err := json.Unmarshal(normalized, target); err != nil {
		return provider.Invalid("Invalid JSON payload received.")
	}
	return nil
}

// gap reports a feature the local backends do not model.
var gap = provider.CoverageGap("Vertex AI")

// validate rejects malformed requests and unmodeled features before any
// backend runs.
func (r *GenerateRequest) validate() error {
	if len(r.Contents) == 0 {
		return provider.Invalid("Unable to submit request because it has an empty contents field.")
	}
	if r.CachedContent != "" {
		return gap("cachedContent is not supported")
	}
	if config := r.config(); config.CandidateCount > 1 {
		return gap("candidateCount above 1 is not supported")
	}
	if err := r.validateTools(); err != nil {
		return err
	}
	for _, content := range r.turns() {
		if err := content.validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateTools accepts only named function declarations.
func (r *GenerateRequest) validateTools() error {
	for _, tool := range r.Tools {
		for kind := range tool.Other {
			return gap("tool " + kind + " is not supported; only functionDeclarations are modeled")
		}
		for _, declaration := range tool.FunctionDeclarations {
			if declaration.Name == "" {
				return provider.Invalid("Every function declaration needs a name.")
			}
		}
	}
	return nil
}

// turns returns the system instruction, if any, followed by the contents.
func (r *GenerateRequest) turns() []Content {
	if r.SystemInstruction == nil {
		return r.Contents
	}
	return append([]Content{*r.SystemInstruction}, r.Contents...)
}

// validRoles are the accepted turn roles; empty means user.
var validRoles = map[string]bool{"": true, roleUser: true, roleModel: true, roleFunction: true, "system": true}

// validate checks one turn's role and parts.
func (c Content) validate() error {
	if !validRoles[c.Role] {
		return provider.Invalid("Please use a valid role: user, model.")
	}
	if len(c.Parts) == 0 {
		return provider.Invalid("Unable to submit request because it must include at least one parts field.")
	}
	for _, part := range c.Parts {
		if len(part.FileData) > 0 {
			return gap("fileData parts are not supported; send inlineData")
		}
		if len(part.ExecutableCode) > 0 {
			return gap("executableCode parts are not supported")
		}
	}
	return nil
}

// Candidate is one generated answer.
type Candidate struct {
	Content      Content `json:"content"`
	FinishReason string  `json:"finishReason,omitempty"`
}

// Usage is the token accounting block.
type Usage struct {
	PromptTokenCount     int    `json:"promptTokenCount"`
	CandidatesTokenCount int    `json:"candidatesTokenCount,omitempty"`
	TotalTokenCount      int    `json:"totalTokenCount"`
	TrafficType          string `json:"trafficType,omitempty"`
}

// GenerateResponse is one generateContent response or stream chunk.
type GenerateResponse struct {
	Candidates    []Candidate `json:"candidates"`
	UsageMetadata *Usage      `json:"usageMetadata,omitempty"`
	ModelVersion  string      `json:"modelVersion"`
	CreateTime    string      `json:"createTime"`
	ResponseID    string      `json:"responseId"`
}
