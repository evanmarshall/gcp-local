# Vertex AI

Vertex AI generative endpoints for Google publisher models over REST. **Answers come from a local stub or a local Ollama model, never from Gemini.** Nothing calls Google. Response shapes are recorded against GCP; answer text is not.

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

All methods are REST only, under `/v1` and `/v1beta1` at `projects/{project}/locations/{location}/publishers/google/models/{model}`. Every method checks `aiplatform.endpoints.predict` on the project before any backend runs; `roles/aiplatform.user` grants it. A 403 names the model and carries ErrorInfo `IAM_PERMISSION_DENIED`, domain `aiplatform.googleapis.com`, as GCP's does (without GCP's troubleshooter link). IAM Conditions see no resource attributes, as in GCP (recorded in `logging-conditional-access`).

| Method | Behavior |
| --- | --- |
| `:generateContent` | One answer with `candidates`, `usageMetadata`, `modelVersion`, `createTime`, `responseId` |
| `:streamGenerateContent` | Server-sent events with `?alt=sse`, a JSON array otherwise. The last chunk carries `finishReason` and `usageMetadata` |
| `:countTokens` | `totalTokens`, `totalBillableCharacters`, `promptTokensDetails`. Takes `contents`, `systemInstruction`, `tools`, `generationConfig`, `model`; any other field is GCP's 400 |
| `:predict` | Text embedding models only (`instances[].content`, `parameters.outputDimensionality`). Other instance and parameter keys are accepted and unread, as GCP accepts them |
| `:embedContent` | Embedding models only; text parts only; `outputDimensionality` top-level or in `embedContentConfig` |

Models are the IDs GCP served in `us-central1` when recorded, per method: generation and `countTokens` on `gemini-2.5-pro`, `gemini-2.5-flash`, `gemini-2.5-flash-lite`, `gemini-2.5-flash-image`; `:predict` on `text-embedding-005`, `text-embedding-004`, `text-multilingual-embedding-002`, `gemini-embedding-001`; `:embedContent` on `gemini-embedding-001` and `gemini-embedding-2-preview`. Any other ID (the retired `gemini-2.0-flash`, `gemini-embedding-2`) is GCP's 404 `Publisher model ... was not found`, in every location. An embedding model through the other embedding method is 400, as in GCP (the message is local). `countTokens` on an embedding model is GCP's 400 `Should provide instances for text model prediction.`

Request bodies are decoded strictly, in either field spelling: an unknown field is GCP's 400 (`Unknown name "x"`); paths in nested refusals use JSON names (`generationConfig.temperature`), not GCP's proto names. `labels` are accepted. `safetySettings` with `BLOCK_NONE` or `OFF` are accepted; an unknown category, threshold, or method is GCP's 400. `thinkingConfig.thinkingBudget` is accepted; the backends don't think.

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

- Text answers start with `[gcp-local stub]`, name the model, and echo the last user message. `modelVersion` is `gcp-local-stub`. The same request always returns the same text, whatever `temperature`, `topP`, `topK`, or `seed` say. Text stops before the first of `stopSequences`.
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
| `vertexai-inference-authorization` | 2026-10-13 | Response shapes, streaming, token counts, embeddings, denials with ErrorInfo, the model catalog by `countTokens`, `:predict`, and `:embedContent`, unknown models, `countTokens` on an embedding model, `BLOCK_NONE` safety settings, an unknown category, unknown and mistyped fields, empty contents |

```sh
sh scripts/check/recordings.sh   # the recording
sh scripts/check/sdk/vertexai.sh          # google-genai SDK against the stub
```

## Gaps

These return 501 naming the field:

- `safetySettings` thresholds that block (`BLOCK_LOW_AND_ABOVE`, `BLOCK_MEDIUM_AND_ABOVE`, `BLOCK_ONLY_HIGH`): the backends don't rate harm. Responses carry no `safetyRatings`.
- `cachedContent`, `modelArmorConfig`, `serviceTier`, `toolConfig.retrievalConfig`, and function calling mode `VALIDATED`.
- `generationConfig` `responseLogprobs`, `logprobs`, `presencePenalty`, `frequencyPenalty`, `routingConfig`, `modelConfig`, `audioTimestamp`, `responseModalities`, `mediaResolution`, `speechConfig`, `enableAffectiveDialog`, `imageConfig`, `thinkingConfig.includeThoughts`, `thinkingConfig.thinkingLevel`; `candidateCount` above 1.
- Parts other than text, inline data, function calls, and function results (`fileData`, `executableCode`, `videoMetadata`, ...); tools other than function declarations (Google Search, retrieval, code execution, ...); function `behavior`. Function `response` and `responseJsonSchema` are accepted and unread.
- `generateContent` and `streamGenerateContent` on an embedding model, `embedContent` on a Gemini model, `predict` on a Gemini model, `countTokens` with `instances`.

Also:

- `countTokens` and the stub's `usageMetadata` approximate tokens as one per started four characters of each word. This is not Gemini's tokenizer. Ollama answers report Ollama's own counts.
- Not implemented: `computeTokens`, `rawPredict`, tuned models, endpoints, and gRPC `PredictionService`.
- The model catalog doesn't vary by location; GCP's does.
- The packaged environment runs the stub. Its backend network has no route to an Ollama server.
