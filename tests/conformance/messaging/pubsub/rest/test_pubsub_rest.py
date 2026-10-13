import pytest

from gcp_validation.services.messaging.pubsub_rest import run_scenario


@pytest.mark.gcp_scenario(
    id="pubsub-rest-data-plane",
    service="pubsub",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/messaging/pubsub_rest.py",
        "gcp_validation/errors.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/messaging/pubsub/rest/conftest.py",
    ),
)
def test_pubsub_rest(pubsubrest, gcp_snapshot):
    fixture, created = pubsubrest
    run_scenario(fixture, gcp_snapshot, created)
