"""These scenarios remain unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.security.secretmanager.policies import run_events, run_policies

CONTRACT = (
    "gcp_validation/services/security/secretmanager/policies.py",
    "gcp_validation/services/security/refusals.py",
    "gcp_validation/live_rest.py",
    "gcp_validation/errors.py",
    "gcp_validation/sandbox.py",
    "tests/conformance/security/secretmanager/policies/conftest.py",
)
VERSIONS = {
    "sdk": "urllib;google-auth/2.56.2",
    "provider": "not-applicable",
    "backend": "not-applicable",
}


@pytest.mark.gcp_scenario(
    id="secretmanager-secret-policies",
    service="secretmanager",
    transport="rest-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_secret_policies(secretmanager_policies, gcp_snapshot):
    run_policies(secretmanager_policies, gcp_snapshot)


@pytest.mark.gcp_scenario(
    id="secretmanager-rotation-events",
    service="secretmanager",
    transport="rest-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_rotation_events(secretmanager_policies, gcp_snapshot):
    run_events(secretmanager_policies, gcp_snapshot)
