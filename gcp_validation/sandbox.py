"""Explicit sandbox profiles and offline/live credential diagnostics."""

import argparse
import json
import re
import shlex
from dataclasses import dataclass
from pathlib import Path


class SandboxError(ValueError):
    """A safe, actionable configuration diagnostic."""


@dataclass(frozen=True)
class Identity:
    email: str
    credentials_file: Path
    wif_provider: str | None = None


@dataclass(frozen=True)
class Sandbox:
    project: str
    project_number: str
    region: str
    live_scope: str
    identities: dict[str, Identity]
    max_buckets: int = 1
    max_objects: int = 10
    max_object_bytes: int = 1048576
    peer_project: str | None = None

    @classmethod
    def load(cls, filename):
        if not filename:
            raise SandboxError("Select an explicit sandbox .env with --gcp-profile/--profile")
        profile = Path(filename).expanduser().resolve()
        values = {}
        for line in profile.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            key, separator, value = line.partition("=")
            if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
                raise SandboxError("Profile must contain NAME=value assignments only")
            if key in values:
                raise SandboxError("Profile contains duplicate fields")
            tokens = shlex.split(value, comments=True)
            if len(tokens) > 1:
                raise SandboxError("Quote profile values containing spaces")
            values[key] = tokens[0] if tokens else ""

        def required(key):
            value = values.get(key, "")
            if not value:
                raise SandboxError("Missing sandbox field: " + key)
            return value

        project = required("GCP_PROJECT_ID")
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
            raise SandboxError("Invalid sandbox project ID")
        number = required("GCP_PROJECT_NUMBER")
        if not re.fullmatch(r"[0-9]{6,20}", number):
            raise SandboxError("Invalid sandbox project number")
        region = required("GCP_REGION")
        if not re.fullmatch(r"[a-z]+-[a-z]+[0-9]+", region):
            raise SandboxError("Select an explicit GCP region")
        wif_provider = values.get("GCP_TEST_WIF_PROVIDER") or None
        if wif_provider and not re.fullmatch(
                r"projects/" + re.escape(number) + r"/locations/global/workloadIdentityPools/[a-z][a-z0-9-]{3,31}/providers/[a-z][a-z0-9-]{3,31}",
                wif_provider):
            raise SandboxError("Select an explicit WIF provider in the sandbox project")
        identities = {}
        for alias, prefix in (("allowed", "GCP_TEST"), ("fixture", "GCP_TEST_FIXTURE"),
                              ("denied", "GCP_TEST_DENIED")):
            email = required(prefix + "_SERVICE_ACCOUNT")
            if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]@" + re.escape(project)
                                + r"\.iam\.gserviceaccount\.com", email):
                raise SandboxError("Test identities must belong to the selected sandbox")
            credential_path = Path(required(prefix + "_CREDENTIALS_FILE")).expanduser()
            if not credential_path.is_absolute():
                raise SandboxError("Credential paths must be absolute (or start with ~/)")
            identities[alias] = Identity(email, credential_path.resolve(), wif_provider)
        if len({identity.email for identity in identities.values()}) != 3:
            raise SandboxError("Fixture, allowed, and denied identities must be distinct")
        peer_project = values.get("GCP_TEST_PEER_PROJECT_ID") or None
        peer_fields = ("GCP_TEST_PEER_PROJECT_ID", "GCP_TEST_PEER_SERVICE_ACCOUNT", "GCP_TEST_PEER_CREDENTIALS_FILE")
        if any(values.get(key) for key in peer_fields):
            peer_project = required(peer_fields[0])
            if (not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", peer_project)
                    or peer_project == project):
                raise SandboxError("Cross-project tests require a distinct explicit peer project")
            email = required(peer_fields[1])
            if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]@" + re.escape(peer_project)
                                + r"\.iam\.gserviceaccount\.com", email):
                raise SandboxError("Peer identity must belong to the selected peer project")
            path = Path(required(peer_fields[2])).expanduser()
            if not path.is_absolute():
                raise SandboxError("Peer credentials require an absolute path")
            identities["peer"] = Identity(email, path.resolve(), wif_provider)
        limits = {}
        for name, maximum in (("max_buckets", 1), ("max_objects", 10),
                              ("max_object_bytes", 1048576)):
            text = values.get("GCP_TEST_" + name.upper()) or str(maximum)
            if not re.fullmatch(r"[0-9]{1,7}", text):
                raise SandboxError("Invalid integer resource limit: " + name)
            value = int(text)
            if not 1 <= value <= maximum:
                raise SandboxError("Initial Storage runner resource limit exceeded: " + name)
            limits[name] = value
        return cls(project, number, region, required("GCP_TEST_LIVE_SCOPE"), identities, peer_project=peer_project, **limits)

    def scope(self):
        return {"services": ["storage"], "region": self.region, "max_buckets": self.max_buckets,
                "max_objects": self.max_objects, "max_object_bytes": self.max_object_bytes,
                "cleanup_required": True, "identities": sorted(self.identities)}


def inspect_credentials(identity, project):
    """Validate the selected file without network access or printing its contents."""
    path = identity.credentials_file
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise SandboxError("Credential file is missing or not owner-readable-only")
    try:
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            raise SandboxError
        if data.get("type") == "external_account":
            from gcp_validation.federation import validate_github_credentials
            return validate_github_credentials(data, identity, project)
        expected = ("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
                    + identity.email + ":generateAccessToken")
        source = data["source_credentials"]
        if (data["type"] != "impersonated_service_account"
                or data["service_account_impersonation_url"] != expected
                or data.get("quota_project_id") != project
                or source["type"] != "authorized_user"
                or source.get("token_uri", "https://oauth2.googleapis.com/token")
                != "https://oauth2.googleapis.com/token"
                or data.get("delegates")
                or any(item.get("universe_domain", "googleapis.com") != "googleapis.com"
                       for item in (data, source))
                or source.get("quota_project_id") not in {None, project}):
            raise SandboxError
        if not all(isinstance(source.get(key), str) and source[key]
                   for key in ("client_id", "client_secret", "refresh_token")):
            raise SandboxError
    except (KeyError, TypeError, ValueError):
        raise SandboxError("Expected explicit, keyless ADC for the selected sandbox identity") from None
    return data


def load_credentials(identity, project):
    data = inspect_credentials(identity, project)
    if data["type"] == "external_account":
        from google.auth.identity_pool import Credentials
        return Credentials.from_info(data, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    from google.auth.impersonated_credentials import Credentials
    return Credentials.from_impersonated_service_account_info(
        data, scopes=["https://www.googleapis.com/auth/cloud-platform"])


def refresh_credentials(credentials):
    import requests
    from google.auth.transport.requests import Request
    session = requests.Session()
    session.trust_env = False
    try:
        credentials.refresh(Request(session=session))
    except Exception:
        raise SandboxError("Credential refresh failed; check sign-in, API enablement, and impersonation grants") from None
    finally:
        session.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["doctor"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--allow-live", action="store_true",
                        help="Also refresh the three explicitly selected identities")
    args = parser.parse_args(argv)
    try:
        sandbox = Sandbox.load(args.profile)
        for identity in sandbox.identities.values():
            inspect_credentials(identity, sandbox.project)
            if args.allow_live:
                refresh_credentials(load_credentials(identity, sandbox.project))
    except SandboxError as error:
        parser.exit(1, str(error) + "\n")
    except (OSError, ValueError):
        # Do not include arbitrary file/parser contents in diagnostics.
        parser.exit(1, "Sandbox check failed: validate required fields, private ADC paths, and impersonation grants.\n")
    print(json.dumps({"status": "passed", "mode": "live-auth" if args.allow_live else "offline",
                      "project": sandbox.project, "scope": sandbox.scope(),
                      "emulator": "not-implemented"}, indent=2))


if __name__ == "__main__":
    main()
