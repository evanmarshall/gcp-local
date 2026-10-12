"""This scenario remains unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.messaging.pubsub import run_scenario


@pytest.mark.gcp_scenario(
    id='pubsub-pull-authorization', service='pubsub', transport='grpc-v1',
    versions={'sdk': 'google-cloud-pubsub/2.42.0;grpcio/1.84.0;google-auth/2.56.2',
              'provider': 'not-applicable', 'backend': 'not-applicable'},
    contract_files=(
        'gcp_validation/services/messaging/pubsub.py',
        'gcp_validation/services/messaging/pubsub_scope.py',
        'gcp_validation/sandbox.py',
        'gcp_validation/federation.py',
        'tests/conformance/messaging/pubsub/pull/conftest.py',
    ),
)
def test_pull_authorization(pubsub, gcp_snapshot):
    run_scenario(pubsub, gcp_snapshot)
