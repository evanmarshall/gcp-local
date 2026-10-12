"""Bucket create with the legacy iamConfiguration.bucketPolicyOnly, as rclone 1.75.2 sends it.

rclone's mkdir with bucket_policy_only = true sets only bucketPolicyOnly, the
former name of uniform bucket-level access. This scenario adds the explicit
public access prevention and soft-delete settings the sandbox scope requires,
and observes how GCP reports both names on create and read.
"""

from pathlib import Path

import pytest

from gcp_validation.sandbox import inspect_credentials
from gcp_validation.storage import StorageClient, StorageFixture


class AliasBucketFixture(StorageFixture):
    """The one sandbox bucket, created through the bucketPolicyOnly alias."""

    def setup(self):
        self.acquire()
        listing = self.wait_allowed(
            lambda: self.client.request(
                "fixture", "GET", "/storage/v1/b", query={"project": self.project, "maxResults": 2}
            )
        ).require(200)
        if listing.get("items") or listing.get("nextPageToken"):
            from gcp_validation.evidence import Inconclusive  # noqa: PLC0415 -- the setup refusal only

            raise Inconclusive(
                "Sandbox already has a bucket; clean owned resources before continuing"
            )
        self.creation_attempted = True
        self.manifest["creation_attempted"] = True
        self.save()  # Persist ownership intent before the first resource mutation.
        self.creation_response = self.client.request(
            "fixture",
            "POST",
            "/storage/v1/b",
            query={"project": self.project},
            body={
                "name": self.bucket,
                "location": self.region.upper(),
                "storageClass": "STANDARD",
                "labels": {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]},
                "iamConfiguration": {
                    "bucketPolicyOnly": {"enabled": True},
                    "publicAccessPrevention": "enforced",
                },
                "softDeletePolicy": {"retentionDurationSeconds": "0"},
            },
        )
        self.assert_owned(self.creation_response.require(200))


def access_shape(response):
    """Report the iamConfiguration fields of one bucket document."""
    value = response.json().get("iamConfiguration", {})
    uniform, legacy = value.get("uniformBucketLevelAccess", {}), value.get("bucketPolicyOnly", {})
    return {
        "status": response.status,
        "fields": sorted(value),
        "uniform_enabled": uniform.get("enabled"),
        "uniform_locked_time": "lockedTime" in uniform,
        "legacy_enabled": legacy.get("enabled"),
        "legacy_locked_time": "lockedTime" in legacy,
        "locked_times_equal": uniform.get("lockedTime") == legacy.get("lockedTime"),
        "public_access_prevention": value.get("publicAccessPrevention"),
    }


@pytest.fixture
def alias_storage(gcp_snapshot, gcp_target, pytestconfig):
    profile = pytestconfig.gcp_sandbox
    if gcp_target.mode != "local":
        if profile is None:
            raise ValueError(
                "Storage live runs need --gcp-profile with fixture, allowed, and denied identities"
            )
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
    fixture = AliasBucketFixture(StorageClient(gcp_target, sandbox=profile), state, region=region)
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"bucket_absent": True})


@pytest.mark.gcp_scenario(
    id="storage-bucket-policy-only",
    service="storage",
    transport="rest-json-v1",
    versions={
        "sdk": "rclone/1.75.2 request shape",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/storage.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
    ),
)
def test_storage_bucket_policy_only(alias_storage, gcp_snapshot):
    gcp_snapshot.match("bucket-created", access_shape(alias_storage.creation_response))
    read = alias_storage.client.request("fixture", "GET", alias_storage.path)
    gcp_snapshot.match("bucket-read", access_shape(read))
