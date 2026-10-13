package vertexai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Fake Ollama wire values.
const (
	roleAssistant = "assistant"
)

// fakeOllama is an httptest Ollama server that records each request body.
type fakeOllama struct {
	mu       sync.Mutex
	requests []map[string]any
	server   *httptest.Server
}

func newFakeOllama(t *testing.T) *fakeOllama {
	t.Helper()
	fake := &fakeOllama{}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeOllama) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.requests = append(f.requests, body)
	f.mu.Unlock()
	switch {
	case r.URL.Path == "/api/embed":
		inputs, _ := body["input"].([]any)
		vectors := make([][]float64, len(inputs))
		for i := range vectors {
			vectors[i] = []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
		}
		writeJSON(w, map[string]any{"embeddings": vectors})
	case r.URL.Path != "/api/chat":
		w.WriteHeader(http.StatusNotFound)
	case body[keyModel] == "missing":
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]any{"error": "model 'missing' not found"})
	case body["stream"] == true:
		streamChat(w)
	default:
		message := map[string]any{"role": roleAssistant, "content": "Hello from llama"}
		if _, ok := body["tools"]; ok {
			call := map[string]any{keyFunction: map[string]any{"name": fnWeather, "arguments": map[string]any{argCity: "Paris"}}}
			message = map[string]any{"role": roleAssistant, "content": "", "tool_calls": []any{call}}
		}
		writeJSON(w, map[string]any{"message": message, "done": true, "done_reason": "stop", "prompt_eval_count": 11, "eval_count": 4})
	}
}

// streamChat writes three newline-delimited chat lines.
func streamChat(w http.ResponseWriter) {
	flusher, _ := w.(http.Flusher)
	for _, line := range []map[string]any{
		{"message": map[string]any{"role": roleAssistant, "content": "Hel"}, "done": false},
		{"message": map[string]any{"role": roleAssistant, "content": "lo"}, "done": false},
		{"message": map[string]any{"role": roleAssistant, "content": ""}, "done": true, "done_reason": "stop", "prompt_eval_count": 7, "eval_count": 2},
	} {
		writeJSON(w, line)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(append(encoded, '\n'))
}

func (f *fakeOllama) last() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func ollamaHarness(t *testing.T) (*harness, *fakeOllama) {
	t.Helper()
	fake := newFakeOllama(t)
	h := newHarness(t, Config{Backend: BackendOllama, OllamaURL: fake.server.URL, Models: map[string]string{
		testModel: "llama3.2", testEmbedding: "nomic-embed-text", "gemini-2.5-pro": "missing",
	}})
	return h, fake
}

func TestOllamaTranslatesChat(t *testing.T) {
	h, fake := ollamaHarness(t)
	body := prompt("Say hello")
	body["systemInstruction"] = map[string]any{"parts": []any{map[string]any{"text": "Be brief."}}}
	body["generationConfig"] = map[string]any{"temperature": 0.2, "maxOutputTokens": 64, "seed": 7}
	recorder := h.call(modelPath+testModel+":generateContent", body, actorUser)
	if recorder.Code != http.StatusOK || recorder.Header().Get("X-Gcp-Local-Model-Backend") != BackendOllama {
		t.Fatalf("generate: %d %s", recorder.Code, recorder.Body)
	}
	value := decode(t, recorder)
	usage := []any{at(t, value, "usageMetadata", "promptTokenCount"), at(t, value, "usageMetadata", "candidatesTokenCount")}
	if firstText(t, value) != "Hello from llama" || value["modelVersion"] != "ollama/llama3.2" || !reflect.DeepEqual(usage, []any{float64(11), float64(4)}) {
		t.Fatalf("response = %v", value)
	}
	sent := fake.last()
	wantMessages := []any{
		map[string]any{"role": "system", "content": "Be brief."},
		map[string]any{"role": actorUser, "content": "Say hello"},
	}
	options := []any{at(t, sent, "options", "temperature"), at(t, sent, "options", "num_predict"), at(t, sent, "options", "seed")}
	if sent[keyModel] != "llama3.2" || sent["stream"] != false || !reflect.DeepEqual(sent["messages"], wantMessages) || !reflect.DeepEqual(options, []any{0.2, float64(64), float64(7)}) {
		t.Fatalf("sent = %v", sent)
	}
}

func TestOllamaFunctionCalling(t *testing.T) {
	h, fake := ollamaHarness(t)
	body := prompt("weather in Paris?")
	body["tools"] = weatherTool()
	part := firstPart(t, h.generate(body))
	if want := map[string]any{"name": fnWeather, "args": map[string]any{argCity: "Paris"}}; !reflect.DeepEqual(at(t, part, "functionCall"), want) {
		t.Fatalf("function call = %v", part)
	}
	function := at(t, fake.last(), "tools", 0, keyFunction)
	wantSchema := map[string]any{keyType: "object", "properties": map[string]any{argCity: map[string]any{keyType: "string"}}, "required": []any{argCity}}
	if at(t, function, "name") != fnWeather || !reflect.DeepEqual(at(t, function, "parameters"), wantSchema) {
		t.Fatalf("tool = %v", function)
	}
	body["contents"] = append(list(t, body, "contents"),
		turn("model", part),
		turn(actorUser, map[string]any{"functionResponse": map[string]any{"name": fnWeather, "response": map[string]any{"temp": 21}}}),
	)
	h.generate(body)
	messages := list(t, fake.last(), "messages")
	if at(t, messages, 1, "role") != roleAssistant || at(t, messages, 1, "tool_calls") == nil {
		t.Fatalf("assistant message = %v", messages)
	}
	if tool := at(t, messages, 2); !reflect.DeepEqual(tool, map[string]any{"role": "tool", "content": `{"temp":21}`, "tool_name": fnWeather}) {
		t.Fatalf("tool message = %v", tool)
	}
}

func TestOllamaStreaming(t *testing.T) {
	h, _ := ollamaHarness(t)
	recorder := h.call(modelPath+testModel+":streamGenerateContent?alt=sse", prompt("hi"), actorUser)
	events := sseEvents(t, recorder.Body.String())
	final := events[len(events)-1]
	if joined(t, events) != "Hello" || at(t, final, "candidates", 0, "finishReason") != finishStop || at(t, final, "usageMetadata", "totalTokenCount") != float64(9) {
		t.Fatalf("events = %v", events)
	}
}

func TestOllamaEmbeddingsAndErrors(t *testing.T) {
	h, fake := ollamaHarness(t)
	value := decode(t, h.call(modelPath+testEmbedding+":predict", map[string]any{
		"instances": []any{map[string]any{"content": "a"}}, "parameters": map[string]any{"outputDimensionality": 4},
	}, actorUser))
	if values := list(t, value, "predictions", 0, "embeddings", "values"); !reflect.DeepEqual(values, []any{0.1, 0.2, 0.3, 0.4}) || fake.last()[keyModel] != "nomic-embed-text" {
		t.Fatalf("embedding = %v sent %v", values, fake.last())
	}
	if unmapped := h.call(modelPath+"gemini-2.5-flash-lite:generateContent", prompt("hi"), actorUser); unmapped.Code != http.StatusNotFound {
		t.Fatalf("unmapped model: %d %s", unmapped.Code, unmapped.Body)
	}
	if missing := h.call(modelPath+"gemini-2.5-pro:generateContent", prompt("hi"), actorUser); missing.Code != http.StatusBadRequest || !strings.Contains(missing.Body.String(), "no such model") {
		t.Fatalf("missing Ollama model: %d %s", missing.Code, missing.Body)
	}
	fake.server.Close()
	if down := h.call(modelPath+testModel+":streamGenerateContent?alt=sse", prompt("hi"), actorUser); down.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreachable backend: %d %s", down.Code, down.Body)
	}
}
