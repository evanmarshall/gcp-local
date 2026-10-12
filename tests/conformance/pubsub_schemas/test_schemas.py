import pytest

from gcp_validation.pubsub_schemas import run_schema_scenario


@pytest.mark.gcp_scenario(
    id="pubsub-schemas",
    service="pubsub",
    transport="grpc-v1",
    versions={
        "sdk": "google-cloud-pubsub/2.42.0;grpcio/1.84.0;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/pubsub_schemas.py",
        "gcp_validation/pubsub.py",
        "gcp_validation/pubsub_scope.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/pubsub_schemas/conftest.py",
    ),
)
def test_schemas(pubsub_schemas, gcp_snapshot):
    run_schema_scenario(pubsub_schemas, gcp_snapshot)
