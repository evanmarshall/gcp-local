"""Bounded Cloud KMS asymmetric-sign and MAC scenario, not yet recorded against GCP.

Keys cannot be deleted in GCP, so each run creates one labeled EC_SIGN_P256_SHA256 key
and one HMAC_SHA256 key in the shared persistent ring and schedules every version for
destruction.
"""

import argparse
import base64
import hashlib
import sys
from pathlib import Path

import google_crc32c
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from gcp_validation.config import Target
from gcp_validation.evidence import SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.security.cloudkms import LIVE_ENDPOINT, LOCATION, RING, b64, version_of

SERVICE = "cloudkms"
LIMITS = {"max_key_rings": 1, "max_crypto_keys": 2, "max_key_versions": 2, "max_run_seconds": 600}
MESSAGE = b"gcp-local synthetic message"
ROLE = "roles/cloudkms.signerVerifier"
KEYS = {"sign": ("ASYMMETRIC_SIGN", "EC_SIGN_P256_SHA256"), "mac": ("MAC", "HMAC_SHA256")}
USABLE = {"ENABLED", "DISABLED"}


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the separately approved live scope for this scenario."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


def crc32c(data: bytes) -> str:
    """Render one CRC32C in the int64-as-string JSON form."""
    return str(google_crc32c.value(data))


def names(run_id: str) -> dict[str, str]:
    """Name the run's signing and MAC keys."""
    return {"sign": "gs-" + run_id[:12], "mac": "gm-" + run_id[:12]}


class PurposeKeysFixture(OwnedRun):
    """One labeled signing key and one MAC key per run; cleanup destroys their versions."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run manifest, key names, and ownership labels."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("keys") == names(v["run_id"]),
        )
        self.manifest.setdefault("keys", names(self.manifest["run_id"]))
        self.keys = self.manifest["keys"]
        self.ring_path = f"/v1/projects/{self.project}/locations/{LOCATION}/keyRings/{RING}"
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]}

    def call(
        self,
        kind: str,
        method: str,
        suffix: str = "",
        *,
        actor: str = "fixture",
        body: dict | None = None,
        query: dict | None = None,
    ) -> Response:
        """Send one request to the run's key of the given kind."""
        path = f"{self.ring_path}/cryptoKeys/{self.keys[kind]}{suffix}"
        return self.client.request(
            actor, method, path, body=body, query=query, timeout=self.remaining()
        )

    def use(self, kind: str, verb: str, body: dict, actor: str = "allowed") -> Response:
        """Call one key-use verb on version 1."""
        return self.call(kind, "POST", f"/cryptoKeyVersions/1:{verb}", actor=actor, body=body)

    def setup(self) -> dict[str, Response]:
        """Ensure the shared ring exists, then create the run's two keys."""
        self.acquire()
        self.client.verify_project()
        ring = self.client.request("fixture", "GET", self.ring_path)
        if ring.status == 404:
            created = self.client.request(
                "fixture",
                "POST",
                self.ring_path.rsplit("/", 1)[0],
                query={"keyRingId": RING},
                body={},
            )
            if created.status not in {200, 409}:
                raise ValueError(f"Shared key ring create returned HTTP {created.status}")
        elif ring.status != 200:
            raise ValueError(f"Shared key ring read returned HTTP {ring.status}")
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        return {
            kind: self.client.request(
                "fixture",
                "POST",
                f"{self.ring_path}/cryptoKeys",
                timeout=self.remaining(),
                query={"cryptoKeyId": self.keys[kind]},
                body={
                    "purpose": purpose,
                    "labels": self.labels,
                    "versionTemplate": {"algorithm": algorithm},
                },
            )
            for kind, (purpose, algorithm) in KEYS.items()
        }

    def cleanup(self) -> None:
        """Keys persist in GCP; destroy every owned version that is not already destroyed."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for kind in KEYS:
                self.destroy_versions(kind)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def destroy_versions(self, kind: str) -> None:
        """Destroy the usable versions of one owned key after checking its labels."""
        key = self.call(kind, "GET")
        if key.status == 404:
            return
        if key.status != 200:
            raise ValueError(f"Owned key read returned HTTP {key.status}")
        if key.json().get("labels") != self.labels:
            raise ValueError("Refusing KMS cleanup: key ownership does not match the manifest")
        for version in self.versions(kind):
            if version["state"] in USABLE:
                number = version_of(version["name"])
                destroyed = self.call(kind, "POST", f"/cryptoKeyVersions/{number}:destroy", body={})
                if destroyed.status != 200:
                    raise ValueError(f"Version destroy returned HTTP {destroyed.status}")
        if any(version["state"] in USABLE for version in self.versions(kind)):
            raise ValueError("Owned key versions remain usable after cleanup")

    def versions(self, kind: str) -> list[dict]:
        """List one owned key's versions."""
        return self.call(kind, "GET", "/cryptoKeyVersions").json().get("cryptoKeyVersions", [])


def verifies(pem: str, signature: bytes) -> bool:
    """Check one ECDSA P-256 signature over MESSAGE with the returned public key."""
    try:
        key = serialization.load_pem_public_key(pem.encode())
        key.verify(signature, MESSAGE, ec.ECDSA(hashes.SHA256()))
    except (InvalidSignature, ValueError, TypeError, AttributeError):
        return False
    return True


def run_scenario(fixture: PurposeKeysFixture, snapshot: SnapshotCase, created: dict) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    aliases = {name: "<" + kind + "-key>" for kind, name in fixture.keys.items()}
    aliases.update({email: "<" + actor + ">" for actor, email in client.emails.items()})
    aliases.update(
        {
            fixture.manifest["run_id"]: "<run>",
            RING: "<ring>",
            fixture.project: "<project>",
            client.number: "<project-number>",
        }
    )
    observe = Observations(aliases)
    for kind, response in created.items():
        snapshot.match(
            f"{kind}-key-created",
            observe.select(response, ("purpose", "versionTemplate", "labels")),
        )
    digest = hashlib.sha256(MESSAGE).digest()
    by_digest = {"digest": {"sha256": b64(digest)}, "digestCrc32c": crc32c(digest)}
    denied = fixture.use("sign", "asymmetricSign", by_digest, actor="denied")
    snapshot.match("denied-sign", observe.error(denied))
    admin = fixture.use("sign", "asymmetricSign", by_digest, actor="fixture")
    snapshot.match("admin-cannot-sign", observe.error(admin))
    for kind in KEYS:
        policy = fixture.call(kind, "GET", ":getIamPolicy")
        member = "serviceAccount:" + client.emails["allowed"]
        granted = fixture.call(
            kind,
            "POST",
            ":setIamPolicy",
            body={
                "policy": {
                    "etag": policy.json().get("etag", ""),
                    "bindings": [{"role": ROLE, "members": [member]}],
                }
            },
        )
        snapshot.match(f"{kind}-granted", observe.select(granted, ("bindings",)))
    pem = observe_public_key(fixture, snapshot, observe)
    observe_signatures(fixture, snapshot, observe, pem, by_digest)
    observe_mac(fixture, snapshot, observe)
    disabled = fixture.call(
        "sign",
        "PATCH",
        "/cryptoKeyVersions/1",
        query={"updateMask": "state"},
        body={"state": "DISABLED"},
    )
    snapshot.match("version-disabled", observe.select(disabled, ("state", "algorithm")))
    disabled_sign = fixture.use("sign", "asymmetricSign", by_digest)
    snapshot.match("disabled-sign", observe.error(disabled_sign))
    destroyed = fixture.call("sign", "POST", "/cryptoKeyVersions/1:destroy", body={})
    snapshot.match("version-destroyed", observe.select(destroyed, ("state",)))
    restored = fixture.call("sign", "POST", "/cryptoKeyVersions/1:restore", body={})
    snapshot.match("version-restored", observe.select(restored, ("state",)))


def observe_public_key(
    fixture: PurposeKeysFixture, snapshot: SnapshotCase, observe: Observations
) -> str:
    """Read the signing key's PEM once the grant has propagated, and return it."""
    public = fixture.wait_until(
        lambda: fixture.call("sign", "GET", "/cryptoKeyVersions/1/publicKey", actor="allowed"),
        {403},
        180,
    )
    pem = public.json().get("pem", "")
    snapshot.match(
        "public-key",
        {
            **observe.select(public, ("algorithm", "protectionLevel")),
            "pem_header": pem.splitlines()[0] if pem else None,
            "pem_crc32c_matches": public.json().get("pemCrc32c") == crc32c(pem.encode()),
        },
    )
    return pem


def observe_signatures(
    fixture: PurposeKeysFixture,
    snapshot: SnapshotCase,
    observe: Observations,
    pem: str,
    by_digest: dict,
) -> None:
    """Sign by digest and by data, then probe checksum, digest, and purpose refusals."""
    by_data = {"data": b64(MESSAGE), "dataCrc32c": crc32c(MESSAGE)}
    for label, body in (("signed-digest", by_digest), ("signed-data", by_data)):
        signed = fixture.use("sign", "asymmetricSign", body)
        value = signed.json()
        signature = base64.b64decode(value.get("signature", ""))
        fields = ("verifiedDigestCrc32c", "verifiedDataCrc32c", "protectionLevel")
        snapshot.match(
            label,
            {
                **observe.select(signed, fields),
                "version": version_of(value.get("name")),
                "signature_crc32c_matches": value.get("signatureCrc32c") == crc32c(signature),
                "verifies": verifies(pem, signature),
            },
        )
    mismatch = fixture.use("sign", "asymmetricSign", {**by_digest, "digestCrc32c": "1"})
    snapshot.match("digest-crc-mismatch", observe.error(mismatch))
    wrong = fixture.use("sign", "asymmetricSign", {"digest": {"sha384": b64(bytes(48))}})
    snapshot.match("wrong-digest", observe.error(wrong))
    snapshot.match(
        "sign-key-mac", observe.error(fixture.use("sign", "macSign", {"data": b64(MESSAGE)}))
    )
    mac_public = fixture.call("mac", "GET", "/cryptoKeyVersions/1/publicKey", actor="allowed")
    snapshot.match("mac-key-public-key", observe.error(mac_public))


def observe_mac(fixture: PurposeKeysFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Sign one HMAC, then verify it against the same and different data."""
    tagged = fixture.wait_until(
        lambda: fixture.use(
            "mac", "macSign", {"data": b64(MESSAGE), "dataCrc32c": crc32c(MESSAGE)}
        ),
        {403},
        180,
    )
    tag = base64.b64decode(tagged.json().get("mac", ""))
    snapshot.match(
        "mac-signed",
        {
            **observe.select(tagged, ("verifiedDataCrc32c", "protectionLevel")),
            "mac_length": len(tag),
            "mac_crc32c_matches": tagged.json().get("macCrc32c") == crc32c(tag),
        },
    )
    for label, data in (("mac-verified", MESSAGE), ("mac-mismatch", b"other")):
        body = {"data": b64(data), "mac": b64(tag), "macCrc32c": crc32c(tag)}
        verified = fixture.use("mac", "macVerify", body)
        fields = ("success", "verifiedSuccessIntegrity", "verifiedMacCrc32c")
        snapshot.match(label, observe.select(verified, fields))


def main(argv: list[str] | None = None) -> None:
    """Destroy the owned key versions after an interrupted live run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, 'cleanup')
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    PurposeKeysFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned KMS cleanup passed.\n")


if __name__ == "__main__":
    main()
