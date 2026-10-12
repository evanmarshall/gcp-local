import json
from types import SimpleNamespace as ns  # noqa: N813 -- short alias keeps fakes readable

import pytest

from gcp_validation.config import Target
from gcp_validation.services.security.secretmanager import (
    LIMITS,
    LiveScope,
    SecretManagerClient,
    SecretManagerFixture,
    crc32c,
)


def scope_file(tmp_path, **changes):
    value = {'schema': 1, 'approved': True, 'services': ['secretmanager'],
             'project': 'sandbox-project', 'project_number': '123', 'location': 'global',
             'limits': dict(LIMITS), 'cleanup_required': True, 'scheduled_runs': False}
    value.update(changes)
    path = tmp_path / 'scope.json'
    path.write_text(json.dumps(value))
    return path


def test_scope_requires_exact_approved_contract(tmp_path):
    sandbox = ns(project='sandbox-project', project_number='123')
    assert LiveScope.load(scope_file(tmp_path), sandbox).project == 'sandbox-project'
    for changes in ({'approved': False}, {'services': ['storage']}, {'project_number': '456'},
                    {'limits': dict(LIMITS, max_secrets=2)}, {'scheduled_runs': True}):
        with pytest.raises(ValueError):
            LiveScope.load(scope_file(tmp_path, **changes), sandbox)
    with pytest.raises(ValueError):
        LiveScope.load(None, sandbox)


def test_cleanup_manifest_must_name_an_owned_secret(tmp_path):
    target = Target.select('local', project='project-a', endpoint='http://127.0.0.1:4566')
    client = SecretManagerClient(target)
    fixture = SecretManagerFixture(client, tmp_path)
    fixture.save()
    assert SecretManagerFixture(client, tmp_path, manifest=fixture.path).secret == fixture.secret
    tampered = dict(fixture.manifest, secret='projects/project-a/secrets/someone-else')
    fixture.path.write_text(json.dumps(tampered))
    with pytest.raises(ValueError):
        SecretManagerFixture(client, tmp_path, manifest=fixture.path)


def test_local_client_refuses_real_credentials():
    target = Target.select('local', project='project-a', endpoint='http://127.0.0.1:4566')
    with pytest.raises(ValueError):
        SecretManagerClient(target, sandbox=ns(project='project-a'))


def test_crc32c_check_value():
    assert crc32c(b'123456789') == 0xE3069283
