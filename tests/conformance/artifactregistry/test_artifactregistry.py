"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.artifactregistry import run_scenario


@pytest.mark.gcp_scenario(
    id='artifactregistry-repository-lifecycle', service='artifactregistry', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/artifactregistry.py',
        'gcp_validation/services/denials.py',
        'gcp_validation/errors.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/artifactregistry/conftest.py',
    ),
)
def test_artifactregistry(artifactregistry, gcp_snapshot):
    fixture, created = artifactregistry
    run_scenario(fixture, gcp_snapshot, created)
