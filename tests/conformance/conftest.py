from pathlib import Path

import pytest

from gcp_validation.sandbox import inspect_credentials
from gcp_validation.storage import StorageClient, StorageFixture


@pytest.fixture
def storage(gcp_snapshot, gcp_target, pytestconfig):
    profile = pytestconfig.gcp_sandbox
    if gcp_target.mode != "local":
        if profile is None:
            raise ValueError("Storage live runs require --gcp-profile with separate fixture/allowed/denied identities")
        for identity in profile.identities.values():
            inspect_credentials(identity, profile.project)
        state = Path.home() / ".local/state/gcp-local" / profile.project
        gcp_snapshot.context["scope"] = profile.scope()
        region = profile.region
    else:
        state = pytestconfig.gcp_artifacts / "resources"
        region = gcp_snapshot.manifest.get("context", {}).get("scope", {}).get("region", "us-central1")
    fixture = StorageFixture(StorageClient(gcp_target, sandbox=profile), state, region=region)
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"bucket_absent": True, "owned_objects_removed": True})
