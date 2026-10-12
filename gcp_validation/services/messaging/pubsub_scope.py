"""Offline Pub/Sub scope contract and private JSON writer; no SDK imports."""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

ACTORS = ('fixture', 'allowed', 'denied')
LIMITS = {'max_topics': 1, 'max_subscriptions': 1, 'max_messages': 10,
          'max_message_bytes': 1024, 'max_run_seconds': 600}


@dataclass(frozen=True)
class LiveScope:
    project: str
    project_number: str

    @classmethod
    def load(cls, filename, sandbox):
        if not filename or sandbox is None:
            raise ValueError('Pub/Sub requires a separate explicitly approved --gcp-pubsub-scope')
        value = json.loads(Path(filename).read_text())
        expected = {'schema': 1, 'approved': True, 'services': ['pubsub'],
                    'project': sandbox.project, 'project_number': sandbox.project_number,
                    'location': 'global', 'limits': LIMITS, 'cleanup_required': True,
                    'scheduled_runs': False}
        if (value != expected or value.get('approved') is not True
                or value.get('cleanup_required') is not True or value.get('scheduled_runs') is not False
                or type(value.get('schema')) is not int
                or any(type(item) is not int for item in value.get('limits', {}).values())):
            raise ValueError('Pub/Sub scope must match the approved project, fixed limits, and cleanup contract')
        return cls(sandbox.project, sandbox.project_number)

    def evidence(self):
        return {'services': ['pubsub'], 'location': 'global', 'limits': dict(LIMITS),
                'cleanup_required': True, 'identities': list(ACTORS)}


def private_json(path, value):
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix=path.stem + '-', suffix='.tmp', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(value, output, indent=2)
            output.write('\n')
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


