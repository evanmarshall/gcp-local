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


@pytest.fixture
def peer_storage(gcp_snapshot, gcp_target, pytestconfig, request):
    from gcp_validation.evidence import Unverified
    if gcp_target.mode != "local" and (pytestconfig.gcp_sandbox is None
                                      or "peer" not in pytestconfig.gcp_sandbox.identities):
        raise Unverified("Cross-project recording requires an explicitly authorized peer project and identity")
    return request.getfixturevalue("storage")


@pytest.fixture
def sdk_storage(gcp_snapshot, gcp_target, request):
    from gcp_validation.evidence import Unverified
    from gcp_validation.sdk_storage import require_local_isolation, upstream_slice
    try:
        from importlib.metadata import version, PackageNotFoundError
        if version("google-cloud-storage") != "3.10.1":
            raise Unverified("Install the pinned requirements-sdk.txt before SDK conformance")
    except (ModuleNotFoundError, PackageNotFoundError):
        raise Unverified("Install requirements-sdk.txt before SDK conformance") from None
    if gcp_target.mode == "local":
        require_local_isolation()
    upstream_slice()
    return request.getfixturevalue("storage")
