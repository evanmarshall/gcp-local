"""A sandbox bucket created with extra configuration, and a cleanup that undoes it.

Versioning, holds, retention, and folders each keep something the base
fixture's cleanup can't remove: noncurrent generations, held or retained
objects, and folders. This fixture creates the base fixture's private bucket
plus the scenario's extra fields, and its cleanup releases holds, removes
unlocked retention, waits out locked retention (scenarios lock only
seconds-long periods), deletes every generation of each owned object name and
every folder of the owned bucket, then deletes the bucket.

After an interrupted run, clean up with this module's ``cleanup`` command, not
``gcp_validation.services.storage.client``, whose cleanup leaves noncurrent
generations and held objects behind.
"""

import argparse
import json
import re
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.sandbox import Sandbox, inspect_credentials
from gcp_validation.services.storage.client import Response, StorageClient, StorageFixture

if TYPE_CHECKING:
    import pytest

    from gcp_validation.evidence import SnapshotCase

# Object names a scenario may own: the base fixture's names, optionally under
# up to three folder levels (hierarchical-namespace scenarios).
OBJECT_NAME = re.compile(r"([a-z0-9.-]{1,40}/){0,3}[a-z0-9.-]{1,80}")
# The longest retention cleanup waits out. Scenarios retain for seconds.
MAX_RETENTION_WAIT = 120
MAX_FOLDERS = 20


def parse_time(value: str) -> datetime:
    """Parse an RFC 3339 timestamp as Storage writes it."""
    return datetime.fromisoformat(value)


class ConfiguredBucket(StorageFixture):
    """The base fixture's one private bucket, created with extra fields."""

    def __init__(
        self,
        client: StorageClient,
        state_dir: Path,
        *,
        region: str,
        query: dict | None = None,
        body: dict | None = None,
        manifest: str | None = None,
    ) -> None:
        """Hold the extra insert query options and body fields, or load a manifest to clean up."""
        super().__init__(client, state_dir, region=region)
        self.query, self.extra = dict(query or {}), dict(body or {})
        if manifest is not None:
            self.load(Path(manifest).resolve())

    def load(self, path: Path) -> None:
        """Adopt an interrupted run's manifest after checking it names only owned resources."""
        data = json.loads(path.read_text())
        if (
            data.get("version") != 1
            or data.get("project") != self.project
            or data.get("project_number") != self.project_number
            or data.get("creation_attempted") is not True
            or not re.fullmatch(r"[0-9a-f]{32}", data.get("run_id", ""))
            or data.get("bucket") != "gcp-local-" + self.project_number + "-" + data["run_id"]
            or not isinstance(data.get("objects"), list)
            or len(data["objects"]) > self.max_objects
            or any(
                not isinstance(name, str) or not OBJECT_NAME.fullmatch(name)
                for name in data["objects"]
            )
        ):
            raise ValueError("Cleanup manifest is outside the selected sandbox ownership scope")
        self.manifest, self.manifest_path, self.creation_attempted = data, path, True
        self.bucket = data["bucket"]
        self.path = "/storage/v1/b/" + self.bucket

    def setup(self) -> None:
        """Create the owned bucket with the extra fields and grant the allowed caller."""
        self.acquire()
        listing = self.wait_allowed(
            lambda: self.client.request(
                "fixture", "GET", "/storage/v1/b", query={"project": self.project, "maxResults": 2}
            )
        )
        listing = listing.require(200)
        if listing.get("items") or listing.get("nextPageToken"):
            raise Inconclusive(
                "Sandbox already has a bucket; clean owned resources before continuing"
            )
        self.creation_attempted = True
        self.manifest["creation_attempted"] = True
        self.save()  # Persist ownership intent before the first resource mutation.
        body = {
            "name": self.bucket,
            "location": self.region.upper(),
            "storageClass": "STANDARD",
            "labels": {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]},
            "iamConfiguration": {
                "uniformBucketLevelAccess": {"enabled": True},
                "publicAccessPrevention": "enforced",
            },
            "softDeletePolicy": {"retentionDurationSeconds": "0"},
            **self.extra,
        }
        self.creation_response = self.client.request(
            "fixture",
            "POST",
            "/storage/v1/b",
            query={"project": self.project, **self.query},
            body=body,
        )
        self.assert_owned(self.creation_response.require(200))
        policy = self.client.request("fixture", "GET", self.path + "/iam").require(200)
        member = (
            "serviceAccount:" + self.sandbox.identities["allowed"].email
            if self.sandbox
            else "serviceAccount:allowed@" + self.project + ".iam.gserviceaccount.com"
        )
        policy.setdefault("bindings", []).append(
            {"role": "roles/storage.objectUser", "members": [member]}
        )
        self.client.request("fixture", "PUT", self.path + "/iam", body=policy).require(200)
        self.wait_allowed(self.list_objects).require(200)

    def object_path(self, name: str) -> str:
        """Return the JSON API path of one object."""
        return self.path + "/o/" + quote(name, safe="")

    def own(self, name: str) -> None:
        """Record an object name before anything creates it."""
        if not OBJECT_NAME.fullmatch(name):
            raise ValueError("Object name exceeds the bounded scenario scope")
        if name not in self.manifest["objects"]:
            if len(self.manifest["objects"]) >= self.max_objects:
                raise ValueError("Object count exceeds the bounded scenario scope")
            self.manifest["objects"].append(name)
            self.save()

    def put(
        self, name: str, payload: bytes, *, actor: str = "allowed", query: dict | None = None
    ) -> Response:
        """Upload one owned object with the given query options and no implicit precondition."""
        limit = self.sandbox.max_object_bytes if self.sandbox else 1048576
        if len(payload) > limit:
            raise ValueError("Payload exceeds the bounded scenario scope")
        self.own(name)
        return self.client.request(
            actor,
            "POST",
            "/upload/storage/v1/b/" + self.bucket + "/o",
            query={"uploadType": "media", "name": name, **(query or {})},
            body=payload,
            media=True,
        )

    def patch_bucket(
        self, body: dict, *, actor: str = "fixture", query: dict | None = None
    ) -> Response:
        """Patch the owned bucket."""
        return self.client.request(actor, "PATCH", self.path, query=query, body=body)

    def versions(self, name: str) -> list[dict]:
        """Every generation of one owned object name, live and noncurrent."""
        listing = self.client.request(
            "fixture",
            "GET",
            self.path + "/o",
            query={"versions": "true", "prefix": name, "maxResults": 50},
        )
        return [item for item in listing.require(200).get("items", []) if item["name"] == name]

    def cleanup(self) -> None:
        """Undo the extra configuration, then delete owned objects, folders, and the bucket."""
        try:
            if not self.creation_attempted:
                return
            response = self.client.request("fixture", "GET", self.path)
            if response.status != 404:
                bucket = response.require(200)
                self.assert_owned(bucket)
                self.release_bucket(bucket)
                for name in self.manifest["objects"]:
                    for item in self.versions(name):
                        self.remove_generation(item)
                self.remove_folders(bucket)
                self.client.request("fixture", "DELETE", self.path).require(204)
                self.client.request("fixture", "GET", self.path).require(404)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            if self.lock:
                self.lock.close()
                self.lock = None

    def release_bucket(self, bucket: dict) -> None:
        """Remove an unlocked retention policy and the default event-based hold.

        Deleting the bucket deletes its notification configurations.
        """
        changes = {}
        if bucket.get("retentionPolicy") and not bucket["retentionPolicy"].get("isLocked"):
            changes["retentionPolicy"] = None
        if bucket.get("defaultEventBasedHold"):
            changes["defaultEventBasedHold"] = False
        if changes:
            self.patch_bucket(changes).require(200)

    def remove_generation(self, item: dict) -> None:
        """Release one generation's holds and retention, then delete it."""
        path, generation = self.object_path(item["name"]), item["generation"]
        if item.get("temporaryHold") or item.get("eventBasedHold"):
            self.client.request(
                "fixture",
                "PATCH",
                path,
                query={"generation": generation},
                body={"temporaryHold": False, "eventBasedHold": False},
            ).require(200)
        retention = item.get("retention") or {}
        if retention.get("mode") == "Unlocked":
            self.client.request(
                "fixture",
                "PATCH",
                path,
                query={"generation": generation, "overrideUnlockedRetention": "true"},
                body={"retention": None},
            ).require(200)
        for until in (
            retention.get("retainUntilTime") if retention.get("mode") == "Locked" else None,
            item.get("retentionExpirationTime"),
        ):
            if until:
                wait_until(parse_time(until))
        deleted = self.client.request("fixture", "DELETE", path, query={"generation": generation})
        if deleted.status != 404:
            deleted.require(204)

    def remove_folders(self, bucket: dict) -> None:
        """Delete every folder of a hierarchical-namespace bucket, deepest first."""
        if not bucket.get("hierarchicalNamespace", {}).get("enabled"):
            return
        listing = self.client.request(
            "fixture", "GET", self.path + "/folders", query={"pageSize": MAX_FOLDERS + 1}
        ).require(200)
        folders = [item["name"] for item in listing.get("items", [])]
        if len(folders) > MAX_FOLDERS or listing.get("nextPageToken"):
            raise ValueError("Folder count exceeds the bounded scenario scope")
        for name in sorted(folders, key=lambda folder: (-folder.count("/"), folder)):
            deleted = self.client.request(
                "fixture", "DELETE", self.path + "/folders/" + quote(name, safe="")
            )
            if deleted.status != 404:
                deleted.require(204)


def wait_until(moment: datetime) -> None:
    """Sleep until a retention time passes; refuse one a scenario could not have set."""
    remaining = (moment - datetime.now(UTC)).total_seconds()
    if remaining > MAX_RETENTION_WAIT:
        raise ValueError(
            "Retention outlasts the bounded cleanup wait; delete the bucket after it expires"
        )
    if remaining > 0:
        time.sleep(remaining + 2)


def owned_bucket(
    gcp_snapshot: "SnapshotCase",
    gcp_target: Target,
    pytestconfig: "pytest.Config",
    *,
    query: dict | None = None,
    body: dict | None = None,
) -> Iterator[ConfiguredBucket]:
    """Yield one configured bucket for a scenario and clean it up afterwards."""
    profile = pytestconfig.gcp_sandbox
    if gcp_target.mode != "local":
        if profile is None:
            raise ValueError("Storage live runs need --gcp-profile with all scenario identities")
        for identity in profile.identities.values():
            inspect_credentials(identity, profile.project)
        state = Path.home() / ".local/state/gcp-local" / profile.project
        gcp_snapshot.context["scope"] = profile.scope()
        region = profile.region
    else:
        state = pytestconfig.gcp_artifacts / "resources"
        region = (
            gcp_snapshot.manifest.get("context", {}).get("scope", {}).get("region", "us-central1")
        )
    fixture = ConfiguredBucket(
        StorageClient(gcp_target, sandbox=profile), state, region=region, query=query, body=body
    )
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"bucket_absent": True})


def main(argv: list[str] | None = None) -> None:
    """Clean only the resources an interrupted configured-bucket manifest names."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    profile = Sandbox.load(args.profile)
    target = Target.select(
        "record",
        project=profile.project,
        credentials_file=profile.identities["allowed"].credentials_file,
        live_scope=profile.live_scope,
        allow_live=args.allow_live,
    )
    state_dir = Path.home() / ".local/state/gcp-local" / profile.project
    fixture = ConfiguredBucket(
        StorageClient(target, sandbox=profile),
        state_dir,
        region=profile.region,
        manifest=args.manifest,
    )
    fixture.acquire()
    fixture.cleanup()
    sys.stdout.write("Owned Storage resource cleanup passed.\n")


if __name__ == "__main__":
    main()
