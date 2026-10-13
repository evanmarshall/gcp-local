"""Bucket Lock: locking, what a locked policy refuses, and deleting a locked bucket.

Locking is irreversible, so the policy retains for five seconds: the object
written after the lock is deletable seconds later, and cleanup then deletes
the locked, empty bucket.
"""

import platform
import time

import pytest

from gcp_validation.services.storage.normalization import StorageObservations

CONTRACT = (
    "gcp_validation/services/storage/client.py",
    "gcp_validation/services/storage/configured.py",
    "gcp_validation/services/storage/normalization.py",
    "gcp_validation/sandbox.py",
    "gcp_validation/federation.py",
    "tests/conformance/storage/protection/conftest.py",
)
VERSIONS = {
    "sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
    "provider": "not-applicable",
    "backend": "not-applicable",
}
PERIOD = 5


@pytest.mark.gcp_scenario(
    id="storage-bucket-lock",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_bucket_lock(bucket, gcp_snapshot):
    observed = StorageObservations(bucket)

    def policy(response):
        if response.status >= 400:
            return observed.error(response)
        retention = response.json().get("retentionPolicy")
        return {
            "status": response.status,
            "fields": sorted(retention) if retention else None,
            "retentionPeriod": retention.get("retentionPeriod") if retention else None,
            "isLocked": retention.get("isLocked") if retention else None,
        }

    def lock(actor="fixture", **query):
        return bucket.client.request(
            actor, "POST", bucket.path + "/lockRetentionPolicy", query=query or None
        )

    configured = bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": str(PERIOD)}})
    gcp_snapshot.match("policy-set", policy(configured))
    metageneration = configured.json()["metageneration"]
    gcp_snapshot.match("lock-without-precondition", policy(lock()))
    gcp_snapshot.match(
        "lock-stale-precondition", policy(lock(ifMetagenerationMatch=int(metageneration) + 5))
    )
    gcp_snapshot.match("lock-denied", policy(lock("denied", ifMetagenerationMatch=metageneration)))
    locked = lock(ifMetagenerationMatch=metageneration)
    gcp_snapshot.match("locked", policy(locked))
    gcp_snapshot.match("locked-read", policy(bucket.client.request("fixture", "GET", bucket.path)))
    current = locked.json().get("metageneration", metageneration)
    gcp_snapshot.match("lock-again", policy(lock(ifMetagenerationMatch=current)))
    gcp_snapshot.match("locked-remove", policy(bucket.patch_bucket({"retentionPolicy": None})))
    gcp_snapshot.match(
        "locked-reduce", policy(bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": "1"}}))
    )
    gcp_snapshot.match(
        "locked-same",
        policy(bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": str(PERIOD)}})),
    )

    stored = bucket.put("locked-in", b"locked", query={"ifGenerationMatch": 0}).require(200)
    early = bucket.client.request("allowed", "DELETE", bucket.object_path("locked-in"))
    gcp_snapshot.match("retained-delete", {"status": early.status, "retained": early.status == 403})
    time.sleep(PERIOD + 2)
    expired = bucket.client.request(
        "allowed",
        "DELETE",
        bucket.object_path("locked-in"),
        query={"ifGenerationMatch": stored["generation"]},
    )
    gcp_snapshot.match("expired-delete", {"status": expired.status})
