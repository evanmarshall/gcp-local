"""A bucket created with the request shape a real client sends, leaving GCP's defaults.

rclone and gcloud create buckets without publicAccessPrevention or a
softDeletePolicy, so GCP applies its defaults: public access prevention
"inherited" and a seven-day soft-delete policy. The fixture owns that one
bucket.

GCP soft-deletes a deleted bucket created under a seven-day policy and keeps
it for seven days, even after the policy is set to 0 (observed 2026-10-12). No
API removes a soft-deleted bucket or object sooner, and restoring either leaves
the soft-deleted copy in place. Every run therefore leaves its bucket
soft-deleted for a week. Cleanup disables soft delete before deleting objects,
so the retained bucket holds only what the scenario itself soft-deleted.
"""

import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from .evidence import Inconclusive, now
from .storage import Response, StorageClient, StorageFixture

# A policy change applies within 30 seconds; allow for a slow propagation.
POLICY_DEADLINE = 120


class ClientBucketFixture(StorageFixture):
    """One sandbox bucket created with a client's exact bucket insert."""

    def __init__(
        self,
        client: StorageClient,
        state_dir: Path,
        *,
        region: str,
        create: tuple[dict, dict],
    ) -> None:
        """Hold the client's insert query options and iamConfiguration."""
        super().__init__(client, state_dir, region=region)
        self.query, self.iam_configuration = create

    def setup(self) -> None:
        """Create the one owned bucket with the selected client's request."""
        self.acquire()
        listing = self.wait_allowed(
            lambda: self.client.request(
                "fixture", "GET", "/storage/v1/b", query={"project": self.project, "maxResults": 2}
            )
        ).require(200)
        if listing.get("items") or listing.get("nextPageToken"):
            raise Inconclusive("Sandbox already has a bucket; clean owned resources first")
        self.creation_attempted = True
        self.manifest["creation_attempted"] = True
        self.save()  # Persist ownership intent before the first resource mutation.
        # Labels mark ownership for cleanup; neither client sends them, and they
        # change no default.
        self.creation_response = self.client.request(
            "fixture",
            "POST",
            "/storage/v1/b",
            query={"project": self.project, **self.query},
            body={
                "name": self.bucket,
                "location": self.region.upper(),
                "labels": {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]},
                "iamConfiguration": self.iam_configuration,
            },
        )
        self.assert_owned(self.creation_response.require(200))

    def grant(self, bindings: list[dict]) -> Response:
        """Replace the bucket policy's bindings, keeping its etag."""
        policy = self.client.request("fixture", "GET", self.path + "/iam").require(200)
        policy["bindings"] = bindings
        return self.client.request("fixture", "PUT", self.path + "/iam", body=policy)

    def restore(self, name: str, generation: str, *, actor: str = "allowed") -> Response:
        """Restore one soft-deleted object generation."""
        return self.client.request(
            actor,
            "POST",
            self.path + "/o/" + quote(name, safe="") + "/restore",
            query={"generation": generation},
        )

    def soft_deleted(self, *, actor: str = "allowed") -> Response:
        """List the bucket's soft-deleted objects."""
        return self.client.request(
            actor, "GET", self.path + "/o", query={"softDeleted": "true", "maxResults": 11}
        )

    def disable_soft_delete(self) -> tuple[Response, Response]:
        """Disable soft delete and wait until GCP applies the change."""
        response = self.client.request(
            "fixture",
            "PATCH",
            self.path,
            body={"softDeletePolicy": {"retentionDurationSeconds": "0"}},
        )
        response.require(200)
        deadline = time.monotonic() + (POLICY_DEADLINE if self.sandbox else 0)
        while True:
            listing = self.soft_deleted(actor="fixture")
            if listing.status == 400:
                return response, listing
            listing.require(200)
            if time.monotonic() >= deadline:
                raise Inconclusive("The disabled soft-delete policy did not apply in time")
            time.sleep(5)

    def cleanup(self) -> None:
        """Disable soft delete, then delete the owned objects and bucket."""
        try:
            if not self.creation_attempted:
                return
            response = self.client.request("fixture", "GET", self.path)
            if response.status != 404:
                bucket = response.require(200)
                self.assert_owned(bucket)
                if bucket.get("softDeletePolicy", {}).get("retentionDurationSeconds", "0") != "0":
                    self.disable_soft_delete()
                for name in self.manifest["objects"]:
                    metadata = self.read(name, actor="fixture")
                    if metadata.status != 404:
                        generation = metadata.require(200)["generation"]
                        self.delete(name, generation, actor="fixture").require(204)
                self.client.request("fixture", "DELETE", self.path).require(204)
                self.client.request("fixture", "GET", self.path).require(404)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            if self.lock:
                self.lock.close()
                self.lock = None


def soft_delete_policy(bucket: dict) -> dict:
    """Report a bucket's soft-delete policy shape without its timestamps."""
    policy = bucket.get("softDeletePolicy", {})
    effective = policy.get("effectiveTime")
    return {
        "fields": sorted(policy),
        "retention_seconds": policy.get("retentionDurationSeconds"),
        "effective_time_is_creation": effective is not None
        and parse(effective) == parse(bucket["timeCreated"]),
    }


def soft_deleted_shape(value: dict) -> dict:
    """Report a soft-deleted object's lifecycle fields without their timestamps."""
    soft, hard = value.get("softDeleteTime"), value.get("hardDeleteTime")
    return {
        "has_soft_delete_time": soft is not None,
        "has_hard_delete_time": hard is not None,
        "retained_seconds": (parse(hard) - parse(soft)).total_seconds() if soft and hard else None,
        "has_time_deleted": "timeDeleted" in value,
        "time_deleted_is_soft_delete_time": value.get("timeDeleted") == soft if soft else None,
    }


def parse(timestamp: str) -> datetime:
    """Parse an RFC 3339 timestamp as Storage writes it."""
    return datetime.fromisoformat(timestamp)
