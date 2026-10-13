"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.databases.firestore import run_scenario


@pytest.mark.gcp_scenario(
    id='firestore-document-lifecycle', service='firestore', transport='rest-v1',
    versions={'sdk': 'urllib;google-auth/2.56.2', 'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/databases/firestore.py',
        'gcp_validation/errors.py',
        'gcp_validation/live_rest.py',
        'gcp_validation/sandbox.py',
        'tests/conformance/databases/firestore/documents/conftest.py',
    ),
)
def test_firestore(firestore, gcp_snapshot):
    fixture, created = firestore
    run_scenario(fixture, gcp_snapshot, created)
