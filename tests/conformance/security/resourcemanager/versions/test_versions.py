"""Project IAM policy versions, conditional bindings, audit configs, and update masks."""

import pytest

from gcp_validation.services.security.resourcemanager.projectiam_versions import run_scenario


@pytest.mark.gcp_scenario(
    id="project-iam-policy-versions",
    service="resourcemanager",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/security/resourcemanager/projectiam_versions.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/security/resourcemanager/versions/conftest.py",
    ),
)
def test_policy_versions(projectiamversions, gcp_snapshot):
    fixture, original = projectiamversions
    run_scenario(fixture, gcp_snapshot, original)
