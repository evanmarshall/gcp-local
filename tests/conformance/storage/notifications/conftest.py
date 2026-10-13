from pathlib import Path

import pytest

from gcp_validation.live_rest import RestClient
from gcp_validation.services.storage.notifications import (
    LIVE_ENDPOINT,
    SERVICE,
    NotificationTopic,
    load_scope,
)


@pytest.fixture
def topic(gcp_snapshot, gcp_target, pytestconfig):
    """The run's Pub/Sub topic and subscription, under the gcp_live_scope_file scope."""
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "notification-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["pubsub_scope"] = scope.evidence()
    owned = NotificationTopic(
        RestClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state
    )
    try:
        owned.setup()
        yield owned
    finally:
        owned.cleanup()
        if owned.manifest["cleanup"] == "passed":
            gcp_snapshot.match("topic-cleanup", {"grant_removed_and_topic_absent": True})
