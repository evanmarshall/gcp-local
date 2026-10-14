"""Secret Manager over REST and gRPC; unverified until an authorized GCP recording exists."""

import pytest

from gcp_validation.services.security.secretmanager.transports import run


@pytest.mark.gcp_scenario(
    id="secretmanager-transport-parity",
    service="secretmanager",
    transport="rest-v1+grpc-v1",
    versions={
        "sdk": "urllib;grpcio/1.84.0;google-cloud-secret-manager/2.31.0 (messages only);"
        "google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/security/secretmanager/transports.py",
        "gcp_validation/runner/rpc.py",
        "gcp_validation/runner/parity.py",
        "gcp_validation/services/security/refusals.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/security/secretmanager/transports/conftest.py",
    ),
)
def test_transport_parity(secretmanager_transports, gcp_snapshot):
    run(secretmanager_transports, gcp_snapshot)
