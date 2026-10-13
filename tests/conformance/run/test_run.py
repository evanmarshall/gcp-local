"""Cloud Run v2 service lifecycle, paging, rollouts, invocation, and conditions over REST."""

import pytest

from gcp_validation.services.run.service import run_scenario


@pytest.mark.gcp_scenario(
    id="run-service-lifecycle",
    service="run",
    transport="rest-v2",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/errors.py",
        "gcp_validation/services/run/service.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/live_paths.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/run/conftest.py",
    ),
)
def test_run(run_services, gcp_snapshot):
    fixture, created = run_services
    run_scenario(fixture, gcp_snapshot, created)
