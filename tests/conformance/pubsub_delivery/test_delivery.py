"""Unverified until an authorized GCP recording exists; never run against GCP without approval."""

import pytest

from gcp_validation.pubsub_delivery import run_delivery_scenario


@pytest.mark.gcp_scenario(
    id="pubsub-delivery-features",
    service="pubsub",
    transport="grpc-v1",
    versions={
        "sdk": "google-cloud-pubsub/2.42.0;grpcio/1.84.0;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/pubsub_delivery.py",
        "gcp_validation/pubsub.py",
        "gcp_validation/pubsub_scope.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/pubsub_delivery/conftest.py",
    ),
)
def test_delivery_features(pubsub_delivery, gcp_snapshot):
    run_delivery_scenario(pubsub_delivery, gcp_snapshot)
