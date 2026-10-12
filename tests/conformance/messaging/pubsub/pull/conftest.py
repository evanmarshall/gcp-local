from pathlib import Path

import pytest

from gcp_validation.services.messaging.pubsub import LiveScope, PubSubClient, PubSubFixture


@pytest.fixture
def pubsub(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == 'local':
        state = pytestconfig.gcp_artifacts / 'pubsub-resources'
    else:
        scope = LiveScope.load(pytestconfig.getoption('gcp_pubsub_scope'), sandbox, gcp_target.mode)
        state = Path.home() / '.local/state/gcp-local' / sandbox.project / 'pubsub'
        gcp_snapshot.context['scope'] = scope.evidence()
    client = PubSubClient(gcp_target, sandbox=sandbox, scope=scope,
                         identities=pytestconfig.getoption('gcp_pubsub_identities'))
    try:
        fixture = PubSubFixture(client, state)
        try:
            fixture.setup()
            gcp_snapshot.match('created', {'topic': '<topic:1>', 'subscription': '<subscription:1>',
                                           'subscription_topic': '<topic:1>', 'ack_deadline_seconds': 10})
            yield fixture
        finally:
            fixture.cleanup()
            if fixture.manifest['cleanup'] == 'passed':
                gcp_snapshot.match('cleanup', {'topic_absent': True, 'subscription_absent': True})
    finally:
        client.close()
