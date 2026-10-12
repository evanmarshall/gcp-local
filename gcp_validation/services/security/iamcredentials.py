"""Bounded IAM Credentials recording: two owned service accounts, grants only on them.

Every grant lives on a service account the run creates, so deleting the accounts
removes it. Tokens and signatures stay in memory; observations keep only field
names, statuses, and derived booleans.
"""

import argparse
import base64
import calendar
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = 'iamcredentials'
LIMITS = {'max_service_accounts': 2, 'max_keys': 0, 'max_run_seconds': 1500}
IAM_ENDPOINT = 'https://iam.googleapis.com'
CREDENTIALS_ENDPOINT = 'https://iamcredentials.googleapis.com'
CLOUD_PLATFORM = 'https://www.googleapis.com/auth/cloud-platform'
AUDIENCE = 'https://gcp-local.invalid/audience'
CREATOR_ROLE = 'roles/iam.serviceAccountTokenCreator'
OPENID_ROLE = 'roles/iam.serviceAccountOpenIdTokenCreator'
ACCESS = 'generateAccessToken'
SCOPE = {'scope': [CLOUD_PLATFORM]}

Snapshot = Any  # the gcp_snapshot fixture


def load_scope(filename: str | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def decode_segment(segment: str) -> dict:
    """Decode one base64url JWT segment."""
    return json.loads(base64.urlsafe_b64decode(segment + '=' * (-len(segment) % 4)))


def owned_accounts(manifest: dict) -> bool:
    """Report whether a manifest names the two accounts its run would create."""
    run = manifest['run_id'][:12]
    return manifest.get('accounts') == ['gl-d-' + run, 'gl-t-' + run]


class CredentialsFixture(OwnedRun):
    """A delegate account and a target account, both owned by this run."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Name the run's accounts; nothing is created until setup."""
        super().__init__(client, state_dir, SERVICE, LIMITS['max_run_seconds'],
                         manifest=manifest, check=owned_accounts)
        run = self.manifest['run_id'][:12]
        self.manifest.setdefault('accounts', ['gl-d-' + run, 'gl-t-' + run])
        self.delegate, self.target = (f'{account}@{self.project}.iam.gserviceaccount.com'
                                      for account in self.manifest['accounts'])
        self.missing = f'gl-x-{run}@{self.project}.iam.gserviceaccount.com'
        self.prefix = f'/v1/projects/{self.project}/serviceAccounts'
        self.description = 'gcp-local conformance run ' + self.manifest['run_id']
        live = client.target.mode != 'local'
        self.credentials_endpoint = CREDENTIALS_ENDPOINT if live else client.endpoint

    def account(self, method: str, email: str, suffix: str = '', *, body: dict | None = None,
                ) -> Response:
        """Call an IAM v1 account method as the fixture identity."""
        return self.client.request('fixture', method, f'{self.prefix}/{email}{suffix}', body=body,
                                   timeout=self.remaining())

    def send(self, url: str, token: str, *, method: str = 'POST', body: dict | None = None,
             quota: bool = True) -> Response:
        """Send one JSON request with an explicit bearer that is never stored or logged."""
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        if quota and self.client.target.mode != 'local':
            headers['x-goog-user-project'] = self.project
        data = json.dumps(body).encode() if body is not None else None
        # The URL is the fixed https endpoint or the selected loopback endpoint.
        request = Request(url, data=data, headers=headers, method=method)  # noqa: S310
        try:
            response = self.client.opener.open(request, timeout=self.remaining())
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))

    def credentials(self, verb: str, email: str, body: dict, *, actor: str = 'allowed',
                    ) -> Response:
        """Call one IAM Credentials method on an account as a scenario identity."""
        if not email.endswith('@' + self.project + '.iam.gserviceaccount.com'):
            raise ValueError('Select an account in the selected project')
        url = f'{self.credentials_endpoint}/v1/projects/-/serviceAccounts/{email}:{verb}'
        return self.send(url, self.client.token(actor), body=body)

    def as_token(self, token: str, email: str) -> Response:
        """Read an account with a minted token, whose quota project is its account's."""
        url = f'{self.client.endpoint}{self.prefix}/{email}'
        return self.send(url, token, method='GET', quota=False)

    def create(self, account_id: str) -> Response:
        """Create one owned account as the fixture identity."""
        return self.client.request('fixture', 'POST', self.prefix, timeout=self.remaining(), body={
            'accountId': account_id,
            'serviceAccount': {'displayName': 'gcp-local conformance',
                               'description': self.description}})

    def grant(self, email: str, bindings: list[tuple[str, list[str]]]) -> Response:
        """Replace an owned account's policy with the given role bindings."""
        policy = self.wait_until(lambda: self.account('POST', email, ':getIamPolicy', body={}),
                                 {404}, 60)
        body = {'policy': {'etag': policy.json().get('etag', ''), 'bindings': [
            {'role': role, 'members': members} for role, members in bindings]}}
        # A member account created moments ago is refused until it propagates.
        response = self.wait_until(lambda: self.account('POST', email, ':setIamPolicy', body=body),
                                   {400}, 120)
        if response.status != 200:
            raise ValueError(f'Owned account policy write returned HTTP {response.status}')
        return response

    def setup(self) -> list[Response]:
        """Create both accounts after recording the attempt."""
        self.acquire()
        self.client.verify_project()
        self.manifest['attempted'] = True
        self.save()  # Creation may succeed even if the response is lost.
        return [self.create(account) for account in self.manifest['accounts']]

    def owned(self, response: Response, email: str) -> None:
        """Refuse to touch an account this run did not create."""
        value = response.json()
        if value.get('email') != email or value.get('description') != self.description:
            raise ValueError('Refusing service-account cleanup: ownership does not match')

    def remove(self, email: str) -> None:
        """Delete one owned account and wait until reads miss it."""
        path = f'{self.prefix}/{email}'
        current = self.client.request('fixture', 'GET', path)
        # A just-created account can still read as missing; give it time to appear.
        appear = time.monotonic() + (30 if self.client.sandbox else 0)
        while current.status == 404 and time.monotonic() < appear:
            time.sleep(5)
            current = self.client.request('fixture', 'GET', path)
        if current.status == 200:
            self.owned(current, email)
            deleted = self.client.request('fixture', 'DELETE', path)
            if deleted.status not in {200, 404}:
                raise ValueError(f'Owned service-account delete returned HTTP {deleted.status}')
        elif current.status != 404:
            raise ValueError(f'Owned service-account read returned HTTP {current.status}')
        # Deletion propagates; a read can still find the account briefly.
        gone = self.wait_until(lambda: self.client.request('fixture', 'GET', path), {200}, 60)
        if gone.status != 404:
            raise ValueError('Owned service account remains after deletion')

    def cleanup(self) -> None:
        """Delete both owned accounts, which removes every grant the run made."""
        if not self.manifest['attempted']:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for email in (self.target, self.delegate):
                self.remove(email)
            self.manifest.update(cleanup='passed', finished_at=now())
            self.save()
        finally:
            self.release()


def expires_in(response: Response, seconds: int) -> bool:
    """Report whether an expireTime is within a minute of now plus seconds."""
    value = response.json().get('expireTime', '')
    expires = calendar.timegm(time.strptime(value[:19], '%Y-%m-%dT%H:%M:%S'))
    return abs(expires - time.time() - seconds) < 60


class Scenario:
    """The selected observations, the same in local, recording, and revalidation runs."""

    def __init__(self, fixture: CredentialsFixture, snapshot: Snapshot) -> None:
        """Alias every identifier the observations could carry."""
        self.fixture, self.snapshot = fixture, snapshot
        client = fixture.client
        self.observe = Observations({
            fixture.target: '<target>', fixture.delegate: '<delegate>',
            fixture.missing: '<missing>', fixture.manifest['run_id']: '<run>',
            fixture.project: '<project>', client.number: '<project-number>',
            **{email: '<' + actor + '>' for actor, email in client.emails.items()}})
        self.allowed = client.emails['allowed']
        self.chain = ['projects/-/serviceAccounts/' + fixture.delegate]

    def outcome(self, response: Response, fields: tuple[str, ...] = ()) -> dict:
        """Select a success's field names, or an error's status and code."""
        if response.status == 200:
            return self.observe.select(response, fields)
        return self.observe.error(response)

    def match(self, name: str, response: Response, **extra: object) -> None:
        """Record one response's outcome with derived values."""
        observation = {**self.outcome(response), **extra}
        check_observation(observation)
        self.snapshot.match(name, observation)

    def call(self, verb: str, body: dict, *, email: str | None = None,
             actor: str = 'allowed') -> Response:
        """Call one IAM Credentials method on the target unless another account is named."""
        return self.fixture.credentials(verb, email or self.fixture.target, body, actor=actor)

    def retry(self, operation: Callable[[], Response], statuses: set[int]) -> Response:
        """Retry while a grant propagates."""
        return self.fixture.wait_until(operation, statuses, 240)

    def delegation(self) -> Response:
        """Record direct, hop-probe, delegated, and denied access tokens."""
        fixture, delegate, chain = self.fixture, self.fixture.delegate, self.chain
        direct = self.retry(lambda: self.call(ACCESS, SCOPE), {403, 404})
        self.match('direct-access-token', direct, expires_in_one_hour=expires_in(direct, 3600))
        ready = self.retry(lambda: self.call('generateIdToken', {'audience': AUDIENCE},
                                             email=delegate), {403, 404})
        self.match('openid-creator-on-delegate', ready)
        # The hop from the caller to the delegate holds only getOpenIdToken.
        self.match('delegated-id-token-openid-hop', self.call(
            'generateIdToken', {'audience': AUDIENCE, 'delegates': chain}))
        self.match('delegated-access-token-openid-hop', self.call(
            ACCESS, {**SCOPE, 'delegates': chain}))
        fixture.grant(delegate, [(CREATOR_ROLE, ['serviceAccount:' + self.allowed])])
        self.retry(lambda: self.call(ACCESS, SCOPE, email=delegate), {403})
        delegated = self.retry(lambda: self.call(ACCESS, {**SCOPE, 'delegates': chain}), {403})
        self.match('delegated-access-token', delegated,
                   expires_in_one_hour=expires_in(delegated, 3600))
        self.match('delegate-bare-email', self.call(ACCESS, {**SCOPE, 'delegates': [delegate]}))
        self.match('denied-direct', self.call(ACCESS, SCOPE, actor='denied'))
        self.match('denied-delegated', self.call(ACCESS, {**SCOPE, 'delegates': chain},
                                                 actor='denied'))
        self.match('missing-target', self.call(ACCESS, SCOPE, email=fixture.missing))
        return direct

    def lifetimes(self, direct: Response) -> None:
        """Record lifetime limits, the scope requirement, and the minted token's identity."""
        short = self.call(ACCESS, {**SCOPE, 'lifetime': '600s'})
        self.match('lifetime-600s', short, expires_in_ten_minutes=expires_in(short, 600))
        for lifetime in ('3600s', '3601s', '43200s', '0s'):
            self.match('lifetime-' + lifetime, self.call(ACCESS, {**SCOPE, 'lifetime': lifetime}))
        self.match('scope-missing', self.call(ACCESS, {}))
        token = direct.json()['accessToken']
        read = self.fixture.as_token(token, self.fixture.target)
        self.snapshot.match('minted-reads-target', self.outcome(read, ('email',)))
        self.match('minted-reads-delegate', self.fixture.as_token(token, self.fixture.delegate))

    def tokens(self) -> None:
        """Record ID-token claims, JWT signing, and delegated blob signing."""
        for include in (True, False):
            minted = self.call('generateIdToken', {'audience': AUDIENCE, 'includeEmail': include})
            claims = decode_segment(minted.json()['token'].split('.')[1])
            self.match(f'id-token-include-email-{str(include).lower()}', minted,
                       claims=sorted(claims), audience=claims.get('aud') == AUDIENCE,
                       email=self.observe.value(claims.get('email')),
                       lifetime=claims['exp'] - claims['iat'])
        self.match('id-token-missing-audience', self.call('generateIdToken', {}))
        issued = int(time.time())
        claims = {'sub': self.fixture.target, 'aud': AUDIENCE, 'iat': issued, 'exp': issued + 600}
        signed = self.call('signJwt', {'payload': json.dumps(claims)})
        header, payload = (decode_segment(part)
                           for part in signed.json()['signedJwt'].split('.')[:2])
        self.match('sign-jwt', signed, header=sorted(header), alg=header.get('alg'),
                   typ=header.get('typ'), kid_is_key_id=header.get('kid') == signed.json()['keyId'],
                   payload_unchanged=payload == claims)
        late = json.dumps({**claims, 'exp': issued + 13 * 3600})
        self.match('sign-jwt-exp-over-12h', self.call('signJwt', {'payload': late}))
        blob = base64.b64encode(b'gcp-local').decode()
        self.match('sign-blob-delegated', self.call(
            'signBlob', {'payload': blob, 'delegates': self.chain}))


def run_scenario(fixture: CredentialsFixture, snapshot: Snapshot, created: list[Response]) -> None:
    """Grant on the owned accounts, then record every observation."""
    for response, email in zip(created, (fixture.delegate, fixture.target), strict=True):
        if response.status != 200:
            raise AssertionError(f'Service-account create returned HTTP {response.status}')
        fixture.owned(response, email)
    scenario = Scenario(fixture, snapshot)
    allowed, target = 'serviceAccount:' + scenario.allowed, fixture.target
    fixture.grant(target, [(CREATOR_ROLE, [allowed, 'serviceAccount:' + fixture.delegate]),
                           ('roles/iam.serviceAccountViewer', ['serviceAccount:' + target])])
    fixture.grant(fixture.delegate, [(OPENID_ROLE, [allowed])])
    direct = scenario.delegation()
    scenario.lifetimes(direct)
    scenario.tokens()


def main(argv: list[str] | None = None) -> None:
    """Clean only the owned service accounts from an interrupted run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument('command', choices=['cleanup'])
    parser.add_argument('--profile', required=True)
    parser.add_argument('--scope', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--allow-live', action='store_true', required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select('record', project=sandbox.project,
                           credentials_file=sandbox.identities['allowed'].credentials_file,
                           live_scope=sandbox.live_scope, allow_live=args.allow_live)
    client = RestClient(target, IAM_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / '.local/state/gcp-local' / sandbox.project / SERVICE
    CredentialsFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write('Owned service-account cleanup passed.\n')


if __name__ == '__main__':
    main()
