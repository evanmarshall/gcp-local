package vertexai

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestPermissionPlans(t *testing.T) {
	h := newHarness(t, Config{})
	plans := h.engine.Plans("aiplatform.")
	if len(plans) != len(methods) {
		t.Fatalf("plans = %v", plans)
	}
	for _, method := range methods {
		if got := plans[method].Baseline; !reflect.DeepEqual(got, []string{permissionPredict}) {
			t.Fatalf("%s plan = %v", method, got)
		}
	}
}

// countingModel records every backend call.
type countingModel struct {
	mu    sync.Mutex
	calls int
}

func (m *countingModel) Version(string) (string, error) { return "counting", nil }

func (m *countingModel) Generate(context.Context, string, *GenerateRequest, func([]Part) error) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return Result{Parts: []Part{{Text: "ok"}}, FinishReason: finishStop}, nil
}

func (m *countingModel) Embed(_ context.Context, _ string, texts []string, _ int) ([][]float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return make([][]float64, len(texts)), nil
}

func TestDeniedCallsNeverReachTheBackend(t *testing.T) {
	h := newHarness(t, Config{})
	model := &countingModel{}
	h.provider.model = model
	embedBody := map[string]any{"content": turn(actorUser, map[string]any{"text": "hi"})}
	cases := map[string]any{
		testModel + ":generateContent":               prompt("hi"),
		testModel + ":streamGenerateContent?alt=sse": prompt("hi"),
		testModel + ":countTokens":                   prompt("hi"),
		testEmbedding + ":predict":                   map[string]any{"instances": []any{map[string]any{"content": "hi"}}},
		"gemini-embedding-001:embedContent":          embedBody,
	}
	for suffix, body := range cases {
		denied := h.call(modelPath+suffix, body, actorViewer)
		if denied.Code != http.StatusForbidden || !strings.Contains(denied.Body.String(), permissionPredict) {
			t.Fatalf("%s viewer: %d %s", suffix, denied.Code, denied.Body)
		}
		if anonymous := h.call(modelPath+suffix, body, ""); anonymous.Code != http.StatusUnauthorized {
			t.Fatalf("%s anonymous: %d %s", suffix, anonymous.Code, anonymous.Body)
		}
	}
	if model.calls != 0 {
		t.Fatalf("backend ran %d times for denied calls", model.calls)
	}
	if allowed := h.call(modelPath+testModel+":generateContent", prompt("hi"), actorUser); allowed.Code != http.StatusOK || model.calls != 1 {
		t.Fatalf("allowed: %d %s calls=%d", allowed.Code, allowed.Body, model.calls)
	}
}

func TestStubIsDeterministicAndMarked(t *testing.T) {
	h := newHarness(t, Config{})
	recorder := h.call(modelPath+testModel+":generateContent", prompt("Summarize the plan"), actorUser)
	if recorder.Code != http.StatusOK || recorder.Header().Get("X-Gcp-Local-Model-Backend") != BackendStub {
		t.Fatalf("generate: %d %v %s", recorder.Code, recorder.Header(), recorder.Body)
	}
	first := decode(t, recorder)
	second, other := h.generate(prompt("Summarize the plan")), h.generate(prompt("Something else"))
	if firstText(t, first) != firstText(t, second) || firstText(t, first) == firstText(t, other) {
		t.Fatalf("stub text not deterministic per input: %q %q %q", firstText(t, first), firstText(t, second), firstText(t, other))
	}
	answer := firstText(t, first)
	if !strings.HasPrefix(answer, stubMarker) || !strings.Contains(answer, "Summarize the plan") || first["modelVersion"] != "gcp-local-stub" {
		t.Fatalf("stub answer is not marked: %v", first)
	}
}

func TestUsageMatchesCountTokens(t *testing.T) {
	h := newHarness(t, Config{})
	generated := h.generate(prompt("Summarize the plan"))
	count := decode(t, h.call(modelPath+testModel+":countTokens", prompt("Summarize the plan"), actorUser))
	if at(t, generated, "usageMetadata", "promptTokenCount") != count["totalTokens"] || count["totalTokens"] != float64(5) {
		t.Fatalf("usage %v count %v", generated["usageMetadata"], count)
	}
	if reason := at(t, generated, "candidates", 0, "finishReason"); reason != finishStop {
		t.Fatalf("finish reason: %v", reason)
	}
}

func TestStubFunctionCalling(t *testing.T) {
	h := newHarness(t, Config{})
	body := prompt("Use get_weather for Paris")
	body["tools"] = []any{map[string]any{"functionDeclarations": []any{
		map[string]any{"name": "get_time", "parameters": map[string]any{keyType: typeObject, "properties": map[string]any{"zone": map[string]any{keyType: typeString}}}},
		map[string]any{"name": fnWeather, "parameters": map[string]any{keyType: typeObject, "properties": map[string]any{
			argCity: map[string]any{keyType: typeString}, "unit": map[string]any{keyType: typeString, "enum": []any{"celsius", "fahrenheit"}}, "days": map[string]any{keyType: "INTEGER"},
		}}},
	}}}
	part := firstPart(t, h.generate(body))
	want := map[string]any{"name": fnWeather, "args": map[string]any{argCity: "stub-city", "unit": "celsius", "days": float64(1)}}
	if !reflect.DeepEqual(at(t, part, "functionCall"), want) {
		t.Fatalf("function call = %v", part)
	}
	body["contents"] = append(list(t, body, "contents"),
		turn("model", part),
		turn(actorUser, map[string]any{"functionResponse": map[string]any{"name": fnWeather, "response": map[string]any{"temp": 21}}}),
	)
	if got := firstText(t, h.generate(body)); !strings.Contains(got, "get_weather result") || !strings.Contains(got, `"temp":21`) {
		t.Fatalf("answer after function result = %q", got)
	}
	body["toolConfig"] = map[string]any{"functionCallingConfig": map[string]any{"mode": "NONE"}}
	if got := firstText(t, h.generate(body)); !strings.HasPrefix(got, stubMarker) {
		t.Fatalf("mode NONE still called a function: %q", got)
	}
}

func TestSnakeCaseFieldsAreAccepted(t *testing.T) {
	h := newHarness(t, Config{})
	body := prompt("call it")
	// google-genai sends parameters_json_schema in proto spelling; property
	// names inside the schema keep their underscores.
	body["tools"] = []any{map[string]any{"function_declarations": []any{map[string]any{
		"name": "lookup", "parameters_json_schema": map[string]any{keyType: "object", "properties": map[string]any{"user_id": map[string]any{keyType: "string"}}},
	}}}}
	want := map[string]any{"name": "lookup", "args": map[string]any{"user_id": "stub-user_id"}}
	if part := firstPart(t, h.generate(body)); !reflect.DeepEqual(at(t, part, "functionCall"), want) {
		t.Fatalf("function call = %v", part)
	}
}

func TestStubJSONOutput(t *testing.T) {
	h := newHarness(t, Config{})
	body := prompt("list colors")
	body["generationConfig"] = map[string]any{"responseMimeType": "application/json", "responseSchema": map[string]any{
		keyType: typeObject, "properties": map[string]any{"colors": map[string]any{keyType: "ARRAY", "items": map[string]any{keyType: typeString}}},
	}}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(firstText(t, h.generate(body))), &parsed); err != nil {
		t.Fatalf("JSON mode returned non-JSON: %v", err)
	}
	if !reflect.DeepEqual(parsed, map[string]any{"colors": []any{"stub-colors"}}) {
		t.Fatalf("JSON mode = %v", parsed)
	}
}

func TestStreamingMatchesUnary(t *testing.T) {
	h := newHarness(t, Config{})
	unary := firstText(t, h.generate(prompt("stream me a story please")))
	sse := h.call(modelPath+testModel+":streamGenerateContent?alt=sse", prompt("stream me a story please"), actorUser)
	if sse.Code != http.StatusOK || sse.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("sse: %d %v", sse.Code, sse.Header())
	}
	events := sseEvents(t, sse.Body.String())
	if len(events) < 2 || joined(t, events) != unary {
		t.Fatalf("stream %q != unary %q", joined(t, events), unary)
	}
	for i, event := range events {
		final := i == len(events)-1
		if final != (at(t, event, "candidates", 0, "finishReason") == finishStop) || final != (event["usageMetadata"] != nil) {
			t.Fatalf("event %d finish/usage placement: %v", i, event)
		}
	}
	array := h.call(modelPath+testModel+":streamGenerateContent", prompt("stream me a story please"), actorUser)
	var chunks []map[string]any
	if err := json.Unmarshal(array.Body.Bytes(), &chunks); err != nil || len(chunks) != len(events) {
		t.Fatalf("JSON array stream: %v %s", err, array.Body)
	}
}

func TestStubEmbeddings(t *testing.T) {
	h := newHarness(t, Config{})
	body := map[string]any{"instances": []any{map[string]any{"content": "alpha"}, map[string]any{"content": "beta"}}, "parameters": map[string]any{"outputDimensionality": 16}}
	first := decode(t, h.call(modelPath+testEmbedding+":predict", body, actorUser))
	if second := decode(t, h.call(modelPath+testEmbedding+":predict", body, actorUser)); !reflect.DeepEqual(first, second) {
		t.Fatal("embeddings differ between identical calls")
	}
	alpha := list(t, first, "predictions", 0, "embeddings", "values")
	beta := list(t, first, "predictions", 1, "embeddings", "values")
	if len(alpha) != 16 || reflect.DeepEqual(alpha, beta) || math.Abs(norm(t, alpha)-1) > 1e-9 {
		t.Fatalf("alpha=%v beta=%v", alpha, beta)
	}
	fullSize := decode(t, h.call(modelPath+testEmbedding+":predict", map[string]any{"instances": []any{map[string]any{"content": "x"}}}, actorUser))
	if got := len(list(t, fullSize, "predictions", 0, "embeddings", "values")); got != 768 {
		t.Fatalf("default dimensions = %d", got)
	}
	embedded := decode(t, h.call(modelPath+"gemini-embedding-001:embedContent", map[string]any{
		"content": turn(actorUser, map[string]any{"text": "alpha"}), "embedContentConfig": map[string]any{"outputDimensionality": 16},
	}, actorUser))
	if !reflect.DeepEqual(list(t, embedded, "embedding", "values"), alpha) {
		t.Fatal("embedContent and predict disagree for the same text and size")
	}
	if notEmbedding := h.call(modelPath+testModel+":predict", body, actorUser); notEmbedding.Code != http.StatusNotImplemented {
		t.Fatalf("predict on a Gemini model: %d", notEmbedding.Code)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newHarness(t, Config{})
	search := prompt("hi")
	search["tools"] = []any{map[string]any{"googleSearch": map[string]any{}}}
	cases := []struct {
		path string
		body any
		want int
	}{
		{modelPath + testModel + ":generateContent", map[string]any{"contents": []any{}}, http.StatusBadRequest},
		{modelPath + testModel + ":generateContent", search, http.StatusNotImplemented},
		{modelPath + testModel + ":generateContent", map[string]any{"contents": []any{turn(actorUser, map[string]any{"fileData": map[string]any{"fileUri": "gs://b/o"}})}}, http.StatusNotImplemented},
		{"/v1/projects/424242/locations/global/publishers/google/models/" + testModel + ":generateContent", prompt("hi"), http.StatusOK},
	}
	for _, item := range cases {
		if got := h.call(item.path, item.body, actorUser); got.Code != item.want {
			t.Fatalf("%s: %d %s, want %d", item.path, got.Code, got.Body, item.want)
		}
	}
}

func TestConfigRejectsAmbiguousBackends(t *testing.T) {
	for _, config := range []Config{
		{Backend: "gemini"},
		{OllamaURL: "http://127.0.0.1:11434"},
		{Backend: BackendOllama, OllamaURL: "http://127.0.0.1:11434"},
		{Backend: BackendOllama, OllamaURL: "https://aiplatform.googleapis.com", Models: map[string]string{"*": "x"}},
		{Backend: BackendOllama, OllamaURL: "http://" + "user:secret" + "@127.0.0.1:11434", Models: map[string]string{"*": "x"}},
	} {
		if err := (&Provider{}).configure(config); err == nil {
			t.Fatalf("config accepted: %+v", config)
		}
	}
}
