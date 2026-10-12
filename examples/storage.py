"""Run after scripts/dev/local_env.py up; creates and removes one private local bucket."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import storage


def main() -> None:
    """Run the local Storage example against a packaged environment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path.cwd() / ".gcp-local/environment")
    args = parser.parse_args()
    directory = args.directory.expanduser().resolve()
    settings = json.loads((directory / "environment.json").read_text())
    endpoint = f"http://127.0.0.1:{settings['port']}"
    adc = directory / "identity/adc.json"
    profile = json.loads(adc.read_text())
    if (
        settings.get("owner") != "gcp-local"
        or settings.get("directory") != str(directory)
        or profile.get("type") != "external_account"
        or profile.get("audience") != "//gcp-local/identity"
        or profile.get("token_url") != endpoint + "/v1/token"
        or profile.get("credential_source") != {"file": str(directory / "identity/subject.txt")}
        or profile.get("service_account_impersonation_url")
    ):
        raise ValueError("Select a generated local environment, not a real GCP credential profile")

    # The runtime has no real-cloud credentials. This example also confines the
    # client's requests, including STS refresh, to its explicit local endpoint.
    class LocalSession(requests.Session):
        def __init__(self) -> None:
            super().__init__()
            self.trust_env = False

        def request(self, method: str, url: str, **kwargs: object) -> requests.Response:
            if urlsplit(url).scheme != "http" or urlsplit(url).netloc != urlsplit(endpoint).netloc:
                raise ValueError("Request left the local environment")
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    transport = LocalSession()
    request = Request(session=transport)
    credentials, _ = google.auth.load_credentials_from_file(
        str(adc), scopes=["https://www.googleapis.com/auth/cloud-platform"], request=request
    )

    class API(AuthorizedSession):
        def request(self, method: str, url: str, **kwargs: object) -> requests.Response:
            if urlsplit(url).scheme != "http" or urlsplit(url).netloc != urlsplit(endpoint).netloc:
                raise ValueError("Request left the local environment")
            kwargs.update(allow_redirects=False, timeout=5)
            return super().request(method, url, **kwargs)

    session = API(credentials, auth_request=request)
    session.trust_env = False
    client = storage.Client(
        project=settings["project"],
        credentials=credentials,
        _http=session,
        client_options={"api_endpoint": endpoint},
        use_auth_w_custom_endpoint=True,
    )
    bucket = client.bucket("gcp-local-example-" + uuid4().hex[:16])
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"
    bucket.soft_delete_policy.retention_duration_seconds = 0
    created = False
    blob = bucket.blob("hello.txt")
    try:
        client.create_bucket(bucket, location="us-central1", retry=None, timeout=5)
        created = True
        blob.upload_from_string(
            b"Hello from local GCP.", if_generation_match=0, retry=None, timeout=5
        )
        print(blob.download_as_bytes(retry=None, timeout=5).decode())
    finally:
        if created:
            if blob.exists(retry=None, timeout=5):
                blob.delete(retry=None, timeout=5)
            bucket.delete(retry=None, timeout=5)
        client.close()
        session.close()
        transport.close()


if __name__ == "__main__":
    main()
