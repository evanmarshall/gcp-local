"""Offline Pub/Sub scope contract and private JSON writer; no SDK imports."""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from gcp_validation.scope import load_scope_file

ACTORS = ('fixture', 'allowed', 'denied')
LIMITS = {'max_topics': 1, 'max_subscriptions': 1, 'max_messages': 10,
          'max_message_bytes': 1024, 'max_run_seconds': 600}


@dataclass(frozen=True)
class LiveScope:
    project: str
    project_number: str

    @classmethod
    def load(cls, filename, sandbox, mode: str):
        load_scope_file(filename, sandbox, service='pubsub', limits=LIMITS, mode=mode,
                        label='Pub/Sub', option='--gcp-pubsub-scope')
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


