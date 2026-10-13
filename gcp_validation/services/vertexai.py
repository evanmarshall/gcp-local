"""Bounded Vertex AI publisher-model inference recording.

Records response shape and authorization only, never generated text: real answers come
from Gemini and vary, local answers come from the deterministic stub. One run makes at
most two short generations (16 output tokens), one token count, and one 8-dimension
embedding, and creates no resources.
"""

import json
from pathlib import Path

from gcp_validation.errors import detailed
from gcp_validation.evidence import SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.denials import iam_denial

SERVICE = 'aiplatform'
LIMITS = {'max_generate_calls': 3, 'max_output_tokens': 16, 'max_run_seconds': 300}
LIVE_ENDPOINT = 'https://us-central1-aiplatform.googleapis.com'
REGION = 'us-central1'
MODEL = 'gemini-2.5-flash'
EMBEDDING = 'text-embedding-005'
PROMPT = {'contents': [{'role': 'user', 'parts': [{'text': 'Reply with one word: ready'}]}]}
GENERATE = {**PROMPT,
            'generationConfig': {'maxOutputTokens': LIMITS['max_output_tokens'], 'temperature': 0,
                                 # Without this, 2.5 models spend the token cap on thinking.
                                 'thinkingConfig': {'thinkingBudget': 0}}}
USAGE_COUNTS = frozenset({'promptTokenCount', 'totalTokenCount'})
# countTokens answers 200 for a served generative model, 400 for an embedding model, and
# 404 for an unknown or retired one (gemini-2.0-flash in us-central1 by 2026-10).
CATALOG_PROBES = ('gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.5-flash-lite',
                  'gemini-2.5-flash-image', 'gemini-2.0-flash', 'gemini-nonexistent-model',
                  'text-embedding-005', 'text-embedding-004', 'text-multilingual-embedding-002',
                  'gemini-embedding-001')
SAFETY = [{'category': 'HARM_CATEGORY_DANGEROUS_CONTENT', 'threshold': 'BLOCK_NONE'}]


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the owner-approved Vertex AI live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class InferenceRun(OwnedRun):
    """A run that owns no resources; the manifest and lock bound concurrent runs."""

    def __init__(self, client: RestClient, state_dir: Path, *,
                 manifest: str | None = None) -> None:
        """Bind the run manifest."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'], manifest=manifest)
        self.base = f'/v1/projects/{self.project}/locations/{REGION}/publishers/google/models'

    def call(self, model: str, method: str, body: dict, *, actor: str = 'fixture',
             query: dict | None = None) -> Response:
        """Invoke one model method as a scenario identity."""
        return self.client.request(actor, 'POST', f'{self.base}/{model}:{method}', body=body,
                                   query=query, timeout=self.remaining())

    def setup(self) -> None:
        """Confirm the project and record the attempt."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()

    def cleanup(self) -> None:
        """Nothing is created, so cleanup only closes the manifest."""
        self.manifest.update(cleanup='passed', finished_at=now())
        self.save()
        self.release()


def generation(response: Response) -> dict:
    """Select a generateContent response's public shape, not its text."""
    value = response.json()
    candidates = value.get('candidates') or [{}]
    content = candidates[0].get('content', {})
    usage = value.get('usageMetadata', {})
    return {'http_status': response.status, 'fields': sorted(value),
            'candidates': len(value.get('candidates') or []), 'role': content.get('role'),
            'has_text': any('text' in part for part in content.get('parts', [])),
            'finish_reason_present': bool(candidates[0].get('finishReason')),
            'usage_counts': sorted(key for key in usage if key in USAGE_COUNTS)}


def stream(response: Response) -> dict:
    """Select the server-sent event stream's shape."""
    events = [json.loads(line[len(b'data: '):]) for line in response.body.splitlines()
              if line.startswith(b'data: ')]
    final = events[-1] if events else {}
    finish = (final.get('candidates') or [{}])[0].get('finishReason')
    return {'http_status': response.status, 'events_present': bool(events),
            'final_finish_reason_present': bool(finish),
            'final_has_usage': 'usageMetadata' in final}


def embedding(response: Response) -> dict:
    """Select an embedding prediction's shape and size."""
    prediction = (response.json().get('predictions') or [{}])[0]
    values = prediction.get('embeddings', {}).get('values', [])
    return {'http_status': response.status, 'fields': sorted(response.json()),
            'prediction_fields': sorted(prediction), 'dimensions': len(values)}


def refusals(run: InferenceRun, snapshot: SnapshotCase, observe: Observations) -> None:
    """Record the model catalog, safety settings, and refused request bodies."""
    snapshot.match('count-tokens-models', {model: run.call(model, 'countTokens', PROMPT).status
                                           for model in CATALOG_PROBES})
    snapshot.match('unknown-model', detailed(observe, run.call(
        'gemini-nonexistent-model', 'generateContent', GENERATE)))
    snapshot.match('count-tokens-embedding-model', detailed(observe, run.call(
        EMBEDDING, 'countTokens', PROMPT)))
    snapshot.match('safety-block-none', generation(run.call(
        MODEL, 'generateContent', {**GENERATE, 'safetySettings': SAFETY})))
    unknown_category = [{'category': 'HARM_CATEGORY_NOT_A_CATEGORY', 'threshold': 'BLOCK_NONE'}]
    snapshot.match('safety-unknown-category', observe.error(run.call(
        MODEL, 'generateContent', {**PROMPT, 'safetySettings': unknown_category})))
    snapshot.match('count-tokens-safety-settings', detailed(observe, run.call(
        MODEL, 'countTokens', {**PROMPT, 'safetySettings': SAFETY})))
    snapshot.match('unknown-field', detailed(observe, run.call(
        MODEL, 'generateContent', {**PROMPT, 'notAField': 1})))
    snapshot.match('unknown-config-field', observe.error(run.call(
        MODEL, 'generateContent', {**PROMPT, 'generationConfig': {'notAField': 1}})))
    snapshot.match('mistyped-field', observe.error(run.call(
        MODEL, 'generateContent', {**PROMPT, 'generationConfig': {'temperature': 'hot'}})))
    snapshot.match('empty-contents', detailed(observe, run.call(
        MODEL, 'generateContent', {'contents': []})))


def run_scenario(run: InferenceRun, snapshot: SnapshotCase) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    client = run.client
    observe = Observations({
        run.project: '<project>', client.number: '<project-number>',
        **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
    counted = run.call(MODEL, 'countTokens', PROMPT)
    snapshot.match('count-tokens', {'http_status': counted.status, 'fields': sorted(counted.json()),
                                    'total_positive': counted.json().get('totalTokens', 0) > 0})
    snapshot.match('generate-content', generation(run.call(MODEL, 'generateContent', GENERATE)))
    streamed = run.call(MODEL, 'streamGenerateContent', GENERATE, query={'alt': 'sse'})
    snapshot.match('stream-generate-content', stream(streamed))
    snapshot.match('denied-generate', iam_denial(observe, run.call(
        MODEL, 'generateContent', GENERATE, actor='denied')))
    snapshot.match('denied-count-tokens', iam_denial(observe, run.call(
        MODEL, 'countTokens', PROMPT, actor='denied')))
    refusals(run, snapshot, observe)
    instances = {'instances': [{'content': 'ready'}]}
    snapshot.match('embedding', embedding(run.call(
        EMBEDDING, 'predict', {**instances, 'parameters': {'outputDimensionality': 8}})))
    snapshot.match('denied-embedding', iam_denial(observe, run.call(
        EMBEDDING, 'predict', instances, actor='denied')))
    snapshot.match('unknown-embedding-model', detailed(observe, run.call(
        'text-embedding-999', 'predict', instances)))
    snapshot.match('predict-unknown-field', detailed(observe, run.call(
        EMBEDDING, 'predict', {**instances, 'notAField': 1})))
