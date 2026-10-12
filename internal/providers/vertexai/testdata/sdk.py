"""Drive the google-genai SDK in Vertex AI mode against the local stub with local ADC."""

import os
import sys

import google.auth
from google import genai
from google.genai import errors, types

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
MARKER = "[gcp-local stub]"
MODEL = "gemini-2.5-flash"


def client(endpoint: str, credentials=None) -> genai.Client:
    """Build a Vertex AI client pointed only at the local endpoint."""
    return genai.Client(
        vertexai=True,
        project="project-a",
        location="us-central1",
        credentials=credentials,
        http_options=types.HttpOptions(base_url=endpoint),
    )


def get_weather(city: str) -> dict:
    """Return the weather for a city."""
    return {"city": city, "temperature_c": 21}


def main() -> None:
    """Exercise generate, stream, count, embed, function calling, and denial."""
    endpoint = sys.argv[1]
    allowed = client(endpoint)

    first = allowed.models.generate_content(model=MODEL, contents="Hello from the SDK")
    second = allowed.models.generate_content(model=MODEL, contents="Hello from the SDK")
    assert first.text.startswith(MARKER), first.text
    assert first.text == second.text, (first.text, second.text)
    assert "Hello from the SDK" in first.text
    assert first.usage_metadata.prompt_token_count > 0

    stream = allowed.models.generate_content_stream(model=MODEL, contents="Hello from the SDK")
    chunks = list(stream)
    assert len(chunks) > 1, chunks
    assert "".join(chunk.text or "" for chunk in chunks) == first.text

    counted = allowed.models.count_tokens(model=MODEL, contents="Hello from the SDK")
    assert counted.total_tokens == first.usage_metadata.prompt_token_count, counted

    embedded = allowed.models.embed_content(
        model="text-embedding-005",
        contents=["alpha", "beta"],
        config=types.EmbedContentConfig(output_dimensionality=16),
    )
    assert [len(item.values) for item in embedded.embeddings] == [16, 16]
    assert embedded.embeddings[0].values != embedded.embeddings[1].values

    gemini_embedding = allowed.models.embed_content(model="gemini-embedding-2", contents="alpha")
    assert len(gemini_embedding.embeddings[0].values) == 3072

    # Automatic function calling: the stub calls get_weather, the SDK runs it,
    # and the stub's next answer carries the result.
    answer = allowed.models.generate_content(
        model=MODEL,
        contents="What is the weather? Use get_weather.",
        config=types.GenerateContentConfig(tools=[get_weather]),
    )
    assert answer.text.startswith(MARKER) and "temperature_c" in answer.text, answer.text

    manual = allowed.models.generate_content(
        model=MODEL,
        contents="weather please",
        config=types.GenerateContentConfig(
            tools=[get_weather],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        ),
    )
    assert manual.function_calls[0].name == "get_weather", manual
    assert manual.function_calls[0].args == {"city": "stub-city"}, manual.function_calls[0].args

    denied_credentials, _ = google.auth.load_credentials_from_file(
        os.environ["GCP_LOCAL_DENIED_ADC"], scopes=SCOPES
    )
    denied = client(endpoint, denied_credentials)  # a temporary client closes on collection
    try:
        denied.models.generate_content(model=MODEL, contents="hi")
    except errors.ClientError as error:
        assert error.code == 403, error
        assert "aiplatform.endpoints.predict" in str(error), error
    else:
        raise AssertionError("An ungranted caller generated content")
    print("google-genai Vertex AI SDK checks passed")


if __name__ == "__main__":
    main()
