# Vertex AI

Vertex AI generative endpoints for Google publisher models (`gemini-*`, `text-embedding-*`, `gemini-embedding-*`) over REST. **Answers come from a local stub or a local Ollama model, never from Gemini.** Nothing calls Google. Response shapes are recorded against GCP; answer text is not.

## Use

The google-genai SDK in Vertex AI mode works with local ADC and `base_url`:

```python
import google.auth
from google import genai
from google.genai import types

credentials, _ = google.auth.load_credentials_from_file(
    ".gcp-local/environment/identity/adc.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = genai.Client(
    vertexai=True,
    project="gcp-local-test",
    location="us-central1",
    credentials=credentials,
    http_options=types.HttpOptions(base_url="http://127.0.0.1:4566"),
)
print(client.models.generate_content(model="gemini-2.5-flash", contents="Hello").text)
```

Pass `project` and `location` explicitly. Without them the SDK drops the project path when `base_url` is set.

## Methods

All methods are REST only, under `/v1` and `/v1beta1` at `projects/{project}/locations/{location}/publishers/google/models/{model}`. Every method checks `aiplatform.endpoints.predict` on the project before any backend runs; `roles/aiplatform.user` grants it.

| Method | Behavior |
| --- | --- |
| `:generateContent` | One answer with `candidates`, `usageMetadata`, `modelVersion`, `createTime`, `responseId` |
| `:streamGenerateContent` | Server-sent events with `?alt=sse`, a JSON array otherwise. The last chunk carries `finishReason` and `usageMetadata` |
| `:countTokens` | `totalTokens`, `totalBillableCharacters`, `promptTokensDetails` |
| `:predict` | Text embedding models only (`instances[].content`, `parameters.outputDimensionality`) |
| `:embedContent` | Text parts only; `outputDimensionality` top-level or in `embedContentConfig` |

Responses carry `X-Gcp-Local-Model-Backend: stub` or `ollama`.

## Backends

Select the backend with explicit server flags. Environment variables are never read.

| Flag | Meaning |
| --- | --- |
| `-vertex-backend=stub` | Default. Deterministic answers derived from the request |
| `-vertex-backend=ollama` | Send inference to an Ollama server |
| `-vertex-ollama-url=http://127.0.0.1:11434` | Ollama server. Google hosts, credentials in the URL, and proxies are refused |
| `-vertex-model=gemini-2.5-flash=llama3.2` | Map a Vertex AI model ID to an Ollama model. Repeatable. `*=MODEL` is a fallback |

```sh
bin/gcp-local -services vertexai -identity-config ... -iam-config ... \
  -vertex-backend ollama -vertex-ollama-url http://127.0.0.1:11434 \
  -vertex-model gemini-2.5-flash=llama3.2 -vertex-model text-embedding-005=nomic-embed-text
```

The stub:

- Text answers start with `[gcp-local stub]`, name the model, and echo the last user message. `modelVersion` is `gcp-local-stub`. The same request always returns the same text.
- With function declarations and no function result yet, it calls the function named in the last user message, or the first one, with arguments built from the parameter schema (`"stub-<name>"` strings, `1` integers, the first enum value). With a function result, it answers in text that quotes the result. `toolConfig` modes `NONE`, `ANY`, and `allowedFunctionNames` apply.
- `responseMimeType: application/json` returns JSON matching `responseSchema` or `responseJsonSchema`.
- Streams split the answer into three chunks.
- Embeddings are unit vectors derived from SHA-256 of the text: 768 dimensions by default, 3072 for `gemini-embedding-*`.

Ollama:

- Calls `/api/chat` and `/api/embed`. Contents, system instruction, function declarations, function calls and results, inline images, temperature, topP, topK, maxOutputTokens, stop sequences, seed, and JSON output map to Ollama fields. Streaming relays Ollama's chunks.
- `modelVersion` is `ollama/<model>`. An unmapped model ID returns 404; a model Ollama lacks returns 400; an unreachable server returns 503.
- `ANY` function-calling mode cannot force a call in Ollama.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `vertexai-inference-authorization` | 2026-10-12 | Response shapes, streaming, token counts, embeddings, 403 for an ungranted caller |

```sh
sh scripts/check_rest_recordings.sh   # the recording
sh scripts/check_vertexai.sh          # google-genai SDK against the stub
```

## Gaps

- `countTokens` and the stub's `usageMetadata` approximate tokens as one per started four characters of each word. This is not Gemini's tokenizer. Ollama answers report Ollama's own counts.
- Not implemented: `fileData` parts, `cachedContent`, `candidateCount` above 1, tools other than function declarations (Google Search, retrieval, code execution), safety ratings, `computeTokens`, `rawPredict`, tuned models, endpoints, and gRPC `PredictionService`.
- The packaged environment runs the stub. Its backend network has no route to an Ollama server.
- Error messages have not been compared with Google Cloud.
