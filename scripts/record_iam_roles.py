#!/usr/bin/env python3
"""Record a candidate of allowlisted predefined service roles (read-only)."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from datetime import datetime, timezone

ROLES = {'storage': ('storage.admin', 'storage.objectUser', 'storage.objectViewer', 'storage.objectCreator'),
         'pubsub': ('pubsub.admin', 'pubsub.publisher', 'pubsub.subscriber', 'pubsub.viewer'),
         'identity': ('iam.serviceAccountUser', 'iam.serviceAccountTokenCreator')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account', required=True)
    parser.add_argument('--quota-project', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--service', choices=ROLES, default='storage')
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    files = []
    for role in ROLES[args.service]:
        # Predefined roles are global: gcloud rejects --project for this command.
        raw = subprocess.check_output(['gcloud', 'iam', 'roles', 'describe', 'roles/' + role,
                                       '--account=' + args.account, '--billing-project=' + args.quota_project,
                                       '--format=json'])
        value = json.loads(raw)
        if value.get('name') != 'roles/' + role or not value.get('includedPermissions'):
            raise SystemExit('Unexpected role response; candidate is incomplete')
        name = role + '.json'
        (args.output / name).write_bytes(raw)
        files.append({'file': name, 'name': value['name'], 'sha256': hashlib.sha256(raw).hexdigest()})
    manifest = {'source': 'https://iam.googleapis.com/v1/roles/{role}',
                'recorded_at': datetime.now(timezone.utc).isoformat(),
                'scope': 'Read-only predefined ' + args.service + ' role definitions; no resource mutation or grant',
                'roles': files}
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Candidate recorded. Inspect role changes before replacing committed snapshots.')


if __name__ == '__main__':
    main()
