"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.cloudlogging_precedence import run_scenario


@pytest.mark.gcp_scenario(
    id='logging-filter-precedence', service='logging', transport='rest-v2',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=('gcp_validation/cloudlogging_precedence.py', 'gcp_validation/cloudlogging.py',
                    'gcp_validation/live_rest.py', 'gcp_validation/sandbox.py',
                    'tests/conformance/cloudlogging/conftest.py'),
)
def test_cloudlogging_precedence(cloudlogging, gcp_snapshot):
    run_scenario(cloudlogging, gcp_snapshot)
