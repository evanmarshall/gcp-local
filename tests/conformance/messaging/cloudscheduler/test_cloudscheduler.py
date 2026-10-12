"""Cloud Scheduler job lifecycle, a Pub/Sub target run, and refusals over REST."""

import pytest

from gcp_validation.services.messaging.cloudscheduler import run_scenario


@pytest.mark.gcp_scenario(
    id='cloudscheduler-job-lifecycle', service='cloudscheduler', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/messaging/cloudscheduler.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/messaging/cloudscheduler/conftest.py',
    ),
)
def test_cloudscheduler(cloudscheduler, gcp_snapshot):
    fixture, created = cloudscheduler
    run_scenario(fixture, gcp_snapshot, created)
