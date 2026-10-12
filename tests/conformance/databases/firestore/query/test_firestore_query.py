"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.databases.firestore_query import run_scenario


@pytest.mark.gcp_scenario(
    id="firestore-query-transaction",
    service="firestore",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/databases/firestore_query.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/databases/firestore/query/conftest.py",
    ),
)
def test_firestore_query(firestore_query, gcp_snapshot):
    fixture, created = firestore_query
    run_scenario(fixture, gcp_snapshot, created)
