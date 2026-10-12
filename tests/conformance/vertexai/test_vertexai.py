"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.vertexai import run_scenario


@pytest.mark.gcp_scenario(
    id='vertexai-inference-authorization', service='aiplatform', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable',
              'backend': 'not-applicable'},
    contract_files=('gcp_validation/vertexai.py', 'gcp_validation/live_rest.py',
                    'gcp_validation/sandbox.py', 'tests/conformance/vertexai/conftest.py'),
)
def test_vertexai(vertexai, gcp_snapshot):
    run_scenario(vertexai, gcp_snapshot)
