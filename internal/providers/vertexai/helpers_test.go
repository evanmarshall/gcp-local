package vertexai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Test names and wire keys repeated across the tests.
const (
	testProject   = "demo-ai"
	testModel     = "gemini-2.5-flash"
	testEmbedding = "text-embedding-005"
	modelPath     = "/v1beta1/projects/" + testProject + "/locations/us-central1/publishers/google/models/"
	actorUser     = "user"
	actorViewer   = "viewer"
	keyContents   = "contents"
	keyParts      = "parts"
	keyContent    = "content"
	keyCandidates = "candidates"
	keyText       = "text"
	keyInstances  = "instances"
	keyArgs       = "args"
	keyName       = "name"
	keyProperties = "properties"
	keyRole       = "role"
	keyParameters = "parameters"
	keyDimensions = "outputDimensionality"
	typeObject    = "OBJECT"
	typeString    = "STRING"
	fnWeather     = "get_weather"
	argCity       = "city"
)

// harness drives the provider through the gateway.
type harness struct {
	t        *testing.T
	gate     *gateway.Gateway
	provider *Provider
	engine   *iam.Engine
}

func newHarness(t *testing.T, config Config) *harness {
	t.Helper()
	registry := gateway.NewRegistry()
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatalf("build the iam engine: %v", err)
	}
	project := "//cloudresourcemanager.googleapis.com/projects/" + testProject
	if err := index.Put("resourcemanager", state.Resource{Name: project, Service: "resourcemanager", Kind: "project", Number: "424242", Scope: state.Scope{Project: testProject, Location: state.Global}}); err != nil {
		t.Fatalf("register the project: %v", err)
	}
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/aiplatform.user", Members: []string{"serviceAccount:user@" + testProject + ".iam.gserviceaccount.com"}},
		{Role: "roles/aiplatform.viewer", Members: []string{"serviceAccount:viewer@" + testProject + ".iam.gserviceaccount.com"}},
	}}); err != nil {
		t.Fatalf("grant the project: %v", err)
	}
	provider, err := New(context.Background(), registry, index, engine, config)
	if err != nil {
		t.Fatalf("build the provider: %v", err)
	}
	g := gateway.New(registry, func(_ context.Context, token string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com", Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
	})
	return &harness{t: t, gate: g, provider: provider, engine: engine}
}

// call posts one request as actor; an empty actor is anonymous.
func (h *harness) call(path string, body any, actor string) *httptest.ResponseRecorder {
	h.t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		h.t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	if actor != "" {
		request.Header.Set("Authorization", "Bearer "+actor)
	}
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// generate posts generateContent for the test model as the granted user.
func (h *harness) generate(body any) map[string]any {
	h.t.Helper()
	return decode(h.t, h.call(modelPath+testModel+":generateContent", body, actorUser))
}

func decode(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return value
}

// at walks decoded JSON by map keys and slice indexes.
func at(t *testing.T, value any, path ...any) any {
	t.Helper()
	for _, step := range path {
		switch key := step.(type) {
		case string:
			object, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("expected an object at %q in %v", key, value)
			}
			value = object[key]
		case int:
			list, ok := value.([]any)
			if !ok || key >= len(list) {
				t.Fatalf("expected an array with index %d in %v", key, value)
			}
			value = list[key]
		}
	}
	return value
}

// list reads an array at a path.
func list(t *testing.T, value any, path ...any) []any {
	t.Helper()
	items, ok := at(t, value, path...).([]any)
	if !ok {
		t.Fatalf("expected an array at %v", path)
	}
	return items
}

// prompt builds a one-turn user request.
func prompt(text string) map[string]any {
	return map[string]any{keyContents: []any{turn(actorUser, map[string]any{keyText: text})}}
}

// turn builds one content turn.
func turn(role string, parts ...any) map[string]any {
	return map[string]any{keyRole: role, keyParts: parts}
}

// firstPart reads the first candidate's first part.
func firstPart(t *testing.T, value any) any {
	t.Helper()
	return at(t, value, keyCandidates, 0, keyContent, keyParts, 0)
}

// firstText joins the first candidate's text parts.
func firstText(t *testing.T, value any) string {
	t.Helper()
	var builder strings.Builder
	for _, part := range list(t, value, keyCandidates, 0, keyContent, keyParts) {
		if text, ok := at(t, part, keyText).(string); ok {
			builder.WriteString(text)
		}
	}
	return builder.String()
}

// weatherTool declares get_weather(city).
func weatherTool() []any {
	return []any{map[string]any{"functionDeclarations": []any{map[string]any{
		keyName: fnWeather, "description": "Weather by city",
		keyParameters: map[string]any{keyType: typeObject, keyProperties: map[string]any{argCity: map[string]any{keyType: typeString}}, "required": []any{argCity}},
	}}}}
}

// sseEvents reads server-sent event payloads.
func sseEvents(t *testing.T, body string) []map[string]any {
	t.Helper()
	var events []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		if payload, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), "data: "); ok {
			var event map[string]any
			if err := json.Unmarshal([]byte(payload), &event); err != nil {
				t.Fatalf("event %q: %v", payload, err)
			}
			events = append(events, event)
		}
	}
	return events
}

// joined concatenates the text of every event.
func joined(t *testing.T, events []map[string]any) string {
	t.Helper()
	var builder strings.Builder
	for _, event := range events {
		builder.WriteString(firstText(t, event))
	}
	return builder.String()
}

// norm is a vector's Euclidean length.
func norm(t *testing.T, values []any) float64 {
	t.Helper()
	var sum float64
	for _, value := range values {
		number, ok := value.(float64)
		if !ok {
			t.Fatalf("non-numeric component %v", value)
		}
		sum += number * number
	}
	return math.Sqrt(sum)
}
