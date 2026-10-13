import pytest

from gcp_validation.services.storage.configured import owned_bucket


@pytest.fixture
def bucket(gcp_snapshot, gcp_target, pytestconfig):
    yield from owned_bucket(gcp_snapshot, gcp_target, pytestconfig)


@pytest.fixture
def retention_bucket(gcp_snapshot, gcp_target, pytestconfig):
    """A bucket created with object retention enabled, which only bucket insert can enable."""
    yield from owned_bucket(
        gcp_snapshot, gcp_target, pytestconfig, query={"enableObjectRetention": "true"}
    )


@pytest.fixture
def soft_delete_bucket(gcp_snapshot, gcp_target, pytestconfig):
    """A bucket with a 7-day soft-delete policy; GCP keeps it soft-deleted 7 days after cleanup."""
    yield from owned_bucket(
        gcp_snapshot,
        gcp_target,
        pytestconfig,
        body={"softDeletePolicy": {"retentionDurationSeconds": "604800"}},
    )
