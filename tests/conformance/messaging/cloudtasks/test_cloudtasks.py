"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.messaging.cloudtasks import run_scenario


@pytest.mark.gcp_scenario(
    id='cloudtasks-dispatch-retry', service='cloudtasks', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/messaging/cloudtasks.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/live_paths.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/messaging/cloudtasks/conftest.py',
    ),
)
def test_cloudtasks(cloudtasks, gcp_snapshot):
    fixture, created = cloudtasks
    run_scenario(fixture, gcp_snapshot, created)
