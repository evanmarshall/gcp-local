package vertexai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// Wire shapes of the Vertex AI generative endpoints (camelCase JSON), from
// the aiplatform v1 GenerateContentRequest and the google-genai SDK's Vertex
// mapping (google-genai 2.29.0). Bodies are decoded strictly: a field these
// structs lack is a gap when the field lists below name it and GCP's 400 for
// an unknown name otherwise.

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

// Part is one piece of a turn. A thought signature is opaque and passed
// through unread.
type Part struct {
	Text             string            `json:"text,omitempty"`
	Thought          bool              `json:"thought,omitempty"`
	ThoughtSignature string            `json:"thoughtSignature,omitempty"`
	InlineData       *Blob             `json:"inlineData,omitempty"`
	FunctionCall     *FunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *FunctionResponse `json:"functionResponse,omitempty"`
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

// Tool declares callable functions. Any other tool kind is a gap, even
// empty: "googleSearch": {} turns search on.
type Tool struct {
	FunctionDeclarations []FunctionDeclaration `json:"functionDeclarations,omitempty"`
	Retrieval            json.RawMessage       `json:"retrieval,omitempty"`
	GoogleSearch         json.RawMessage       `json:"googleSearch,omitempty"`
	SearchRetrieval      json.RawMessage       `json:"googleSearchRetrieval,omitempty"`
	CodeExecution        json.RawMessage       `json:"codeExecution,omitempty"`
	EnterpriseWebSearch  json.RawMessage       `json:"enterpriseWebSearch,omitempty"`
	GoogleMaps           json.RawMessage       `json:"googleMaps,omitempty"`
	URLContext           json.RawMessage       `json:"urlContext,omitempty"`
	ComputerUse          json.RawMessage       `json:"computerUse,omitempty"`
	FileSearch           json.RawMessage       `json:"fileSearch,omitempty"`
	ParallelAISearch     json.RawMessage       `json:"parallelAiSearch,omitempty"`
	MCPServers           json.RawMessage       `json:"mcpServers,omitempty"`
	ExaAISearch          json.RawMessage       `json:"exaAiSearch,omitempty"`
}

// unmodeledKind names the first tool kind other than function declarations
// the tool sets, or "".
func (t Tool) unmodeledKind() string {
	for _, kind := range []struct {
		name string
		raw  json.RawMessage
	}{
		{"retrieval", t.Retrieval}, {"googleSearch", t.GoogleSearch}, {"googleSearchRetrieval", t.SearchRetrieval},
		{"codeExecution", t.CodeExecution}, {"enterpriseWebSearch", t.EnterpriseWebSearch}, {"googleMaps", t.GoogleMaps},
		{"urlContext", t.URLContext}, {"computerUse", t.ComputerUse}, {"fileSearch", t.FileSearch},
		{"parallelAiSearch", t.ParallelAISearch}, {"mcpServers", t.MCPServers}, {"exaAiSearch", t.ExaAISearch},
	} {
		if len(kind.raw) > 0 && string(bytes.TrimSpace(kind.raw)) != "null" {
			return kind.name
		}
	}
	return ""
}

// FunctionDeclaration is one callable function and its parameter schema.
// The response schemas describe what the function returns; the google-genai
// SDK sends one by default. The backends pick calls from the parameters, so
// they are accepted unread.
type FunctionDeclaration struct {
	Name                 string         `json:"name"`
	Description          string         `json:"description,omitempty"`
	Parameters           map[string]any `json:"parameters,omitempty"`
	ParametersJSONSchema map[string]any `json:"parametersJsonSchema,omitempty"`
	Response             map[string]any `json:"response,omitempty"`
	ResponseJSONSchema   any            `json:"responseJsonSchema,omitempty"`
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

// ThinkingConfig bounds thinking. The local backends don't think, so any
// budget is met; asking for thoughts or a thinking level is a gap.
type ThinkingConfig struct {
	ThinkingBudget *int64 `json:"thinkingBudget,omitempty"`
}

// GenerationConfig carries the sampling and output options. The stub's
// answers are deterministic and ignore sampling; Ollama maps it.
type GenerationConfig struct {
	Temperature        *float64        `json:"temperature,omitempty"`
	TopP               *float64        `json:"topP,omitempty"`
	TopK               *float64        `json:"topK,omitempty"`
	CandidateCount     int             `json:"candidateCount,omitempty"`
	MaxOutputTokens    int             `json:"maxOutputTokens,omitempty"`
	StopSequences      []string        `json:"stopSequences,omitempty"`
	Seed               *int64          `json:"seed,omitempty"`
	ResponseMimeType   string          `json:"responseMimeType,omitempty"`
	ResponseSchema     map[string]any  `json:"responseSchema,omitempty"`
	ResponseJSONSchema map[string]any  `json:"responseJsonSchema,omitempty"`
	ThinkingConfig     *ThinkingConfig `json:"thinkingConfig,omitempty"`
}

// SafetySetting is one harm category's blocking threshold.
type SafetySetting struct {
	Category  string `json:"category"`
	Threshold string `json:"threshold"`
	Method    string `json:"method,omitempty"`
}

// GenerateRequest is the generateContent / streamGenerateContent body.
// Labels are billing metadata with no effect on the answer.
type GenerateRequest struct {
	Contents          []Content         `json:"contents"`
	SystemInstruction *Content          `json:"systemInstruction,omitempty"`
	Tools             []Tool            `json:"tools,omitempty"`
	ToolConfig        *ToolConfig       `json:"toolConfig,omitempty"`
	GenerationConfig  *GenerationConfig `json:"generationConfig,omitempty"`
	SafetySettings    []SafetySetting   `json:"safetySettings,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}

// countRequest is the countTokens body: no safety settings, tool config,
// labels, or cached content (recorded: an unknown name).
type countRequest struct {
	Contents          []Content         `json:"contents"`
	SystemInstruction *Content          `json:"systemInstruction,omitempty"`
	Tools             []Tool            `json:"tools,omitempty"`
	GenerationConfig  *GenerationConfig `json:"generationConfig,omitempty"`
	Model             string            `json:"model,omitempty"`
}

// generate returns the count body as a generation request.
func (r countRequest) generate() GenerateRequest {
	return GenerateRequest{Contents: r.Contents, SystemInstruction: r.SystemInstruction, Tools: r.Tools, GenerationConfig: r.GenerationConfig}
}

// contentFields names the unmodeled fields of a Content at prefix.
func contentFields(prefix string) []string {
	parts := []string{
		"fileData", "executableCode", "codeExecutionResult", "videoMetadata", "mediaResolution",
		"audioTranscription", "mediaProcessing", "speechMetadata", "inlineData.displayName",
		"functionCall.partialArgs", "functionCall.willContinue",
		"functionResponse.willContinue", "functionResponse.scheduling", "functionResponse.parts",
	}
	fields := make([]string, 0, len(parts))
	for _, field := range parts {
		fields = append(fields, prefix+".parts."+field)
	}
	return fields
}

// generationFields names the GenerationConfig fields the struct lacks.
var generationFields = []string{
	"responseLogprobs", "logprobs", "presencePenalty", "frequencyPenalty", "routingConfig", "modelConfig",
	"audioTimestamp", "responseModalities", "mediaResolution", "speechConfig", "enableAffectiveDialog",
	"imageConfig", "thinkingConfig.includeThoughts", "thinkingConfig.thinkingLevel",
}

// requestFields lists the unmodeled fields shared by generation and counting.
func requestFields() []string {
	fields := append(contentFields("contents"), contentFields("systemInstruction")...)
	for _, kind := range []string{"functionDeclarations.behavior"} {
		fields = append(fields, "tools."+kind)
	}
	for _, field := range generationFields {
		fields = append(fields, "generationConfig."+field)
	}
	return fields
}

// Body field lists of the generation and counting methods.
var (
	generateFields = provider.Fields{Unmodeled: append(requestFields(),
		"cachedContent", "modelArmorConfig", "serviceTier", "toolConfig.retrievalConfig",
		"toolConfig.functionCallingConfig.streamFunctionCallArguments")}
	countFields = provider.Fields{Unmodeled: append(requestFields(), "instances")}
)

// declarations lists every declared function.
func (r *GenerateRequest) declarations() []FunctionDeclaration {
	var all []FunctionDeclaration
	for _, tool := range r.Tools {
		all = append(all, tool.FunctionDeclarations...)
	}
	return all
}

// Function-calling modes the backends model.
const (
	modeAuto = "AUTO"
	modeAny  = "ANY"
	modeNone = "NONE"
)

// callingMode returns the function-calling mode, AUTO by default.
func (r *GenerateRequest) callingMode() (string, []string) {
	if r.ToolConfig == nil || r.ToolConfig.FunctionCallingConfig == nil {
		return modeAuto, nil
	}
	config := r.ToolConfig.FunctionCallingConfig
	mode := strings.ToUpper(config.Mode)
	if mode == "" || mode == "MODE_UNSPECIFIED" {
		mode = modeAuto
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

// decodeBody strictly decodes a request body in either field spelling. Paths
// in refusals are the camelCase JSON names; GCP's are the proto names.
func decodeBody(raw []byte, target any, fields provider.Fields) error {
	var generic any
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return provider.Invalid("Invalid JSON payload received.")
	}
	normalized, err := json.Marshal(camelKeys(generic))
	if err != nil {
		return provider.Invalid("Invalid JSON payload received.")
	}
	return provider.DecodeJSON(normalized, "", target, gap, fields)
}

// gap reports a feature the local backends do not model.
var gap = provider.CoverageGap("Vertex AI")

// errEmptyContents is GCP's refusal of a request without contents (recorded).
var errEmptyContents = provider.Invalid("Unable to submit request because at least one contents field is required. Learn more: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/gemini")

// validate rejects malformed requests and unmodeled features before any
// backend runs.
func (r *GenerateRequest) validate() error {
	if len(r.Contents) == 0 {
		return errEmptyContents
	}
	if config := r.config(); config.CandidateCount > 1 {
		return gap("candidateCount above 1 is not supported")
	}
	if err := r.validateTools(); err != nil {
		return err
	}
	if err := validateSafety(r.SafetySettings); err != nil {
		return err
	}
	for _, content := range r.turns() {
		if err := content.validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateTools accepts named function declarations and the modeled
// function-calling modes.
func (r *GenerateRequest) validateTools() error {
	for _, tool := range r.Tools {
		if kind := tool.unmodeledKind(); kind != "" {
			return gap("tool " + kind + " is not supported; only functionDeclarations are modeled")
		}
		for _, declaration := range tool.FunctionDeclarations {
			if declaration.Name == "" {
				return provider.Invalid("Every function declaration needs a name.")
			}
		}
	}
	switch mode, _ := r.callingMode(); mode {
	case modeAuto, modeAny, modeNone:
		return nil
	case "VALIDATED":
		return gap("function calling mode VALIDATED is not modeled")
	default:
		return provider.FieldViolation("tool_config.function_calling_config.mode", fmt.Sprintf(
			"Invalid value at 'tool_config.function_calling_config.mode' (type.googleapis.com/google.cloud.aiplatform.v1.FunctionCallingConfig.Mode), %q", mode))
	}
}

// Harm categories, thresholds, and methods of the v1 SafetySetting enums.
var (
	harmCategories = names("HARM_CATEGORY_UNSPECIFIED", "HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
		"HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT", "HARM_CATEGORY_CIVIC_INTEGRITY",
		"HARM_CATEGORY_JAILBREAK", "HARM_CATEGORY_IMAGE_HATE", "HARM_CATEGORY_IMAGE_HARASSMENT",
		"HARM_CATEGORY_IMAGE_DANGEROUS_CONTENT", "HARM_CATEGORY_IMAGE_SEXUALLY_EXPLICIT")
	harmThresholds = names("HARM_BLOCK_THRESHOLD_UNSPECIFIED", "BLOCK_LOW_AND_ABOVE", "BLOCK_MEDIUM_AND_ABOVE",
		"BLOCK_ONLY_HIGH", "BLOCK_NONE", "OFF")
	harmMethods = names("", "HARM_BLOCK_METHOD_UNSPECIFIED", "SEVERITY", "PROBABILITY")
)

// names builds a set.
func names(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

// validateSafety refuses an unknown enum value as GCP does (recorded) and any
// threshold that would block: the local backends don't rate harm, so only
// BLOCK_NONE and OFF are modeled.
func validateSafety(settings []SafetySetting) error {
	for i, setting := range settings {
		for _, check := range []struct {
			field, value, enum string
			known              map[string]bool
		}{
			{"category", setting.Category, "HarmCategory", harmCategories},
			{"threshold", setting.Threshold, "SafetySetting.HarmBlockThreshold", harmThresholds},
			{"method", setting.Method, "SafetySetting.HarmBlockMethod", harmMethods},
		} {
			if !check.known[check.value] {
				path := fmt.Sprintf("safety_settings[%d].%s", i, check.field)
				return provider.FieldViolation(path, fmt.Sprintf("Invalid value at '%s' (type.googleapis.com/google.cloud.aiplatform.v1.%s), %q", path, check.enum, check.value))
			}
		}
		if setting.Threshold != "BLOCK_NONE" && setting.Threshold != "OFF" {
			return gap("safetySettings threshold " + setting.Threshold + " is not modeled; the local backends don't rate harm")
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
