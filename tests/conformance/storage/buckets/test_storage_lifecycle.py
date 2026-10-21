"""Bucket lifecycle rules: what GCP accepts, how it renders them, and what it refuses.

GCP applies lifecycle rules asynchronously, typically within 24 hours, so a
recording can't observe an action at a deterministic time. This scenario
records the configuration round trip, the validation errors, and the object
customTime updates the customTime conditions read.
"""

import platform
import time

import pytest

from gcp_validation.services.storage.configured import RATE_LIMIT_SECONDS
from gcp_validation.services.storage.normalization import StorageObservations

RULES = [
    {
        "action": {"type": "Delete"},
        "condition": {"age": 30, "matchesPrefix": ["logs/"], "matchesSuffix": [".tmp"]},
    },
    {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 60, "matchesStorageClass": ["STANDARD"]},
    },
    {
        "action": {"type": "Delete"},
        "condition": {"isLive": False, "numNewerVersions": 2, "daysSinceNoncurrentTime": 7},
    },
    {
        "action": {"type": "Delete"},
        "condition": {
            "createdBefore": "2020-01-01",
            "customTimeBefore": "2021-01-01",
            "noncurrentTimeBefore": "2022-01-01",
            "daysSinceCustomTime": 3,
        },
    },
    {"action": {"type": "AbortIncompleteMultipartUpload"}, "condition": {"age": 1}},
    {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
        "condition": {"age": 90, "matchesStorageClass": ["NEARLINE"]},
    },
    {"action": {"type": "Delete"}, "condition": {"age": 0}},
]
INVALID = {
    "unknown-action": [{"action": {"type": "Explode"}, "condition": {"age": 1}}],
    "missing-storage-class": [{"action": {"type": "SetStorageClass"}, "condition": {"age": 1}}],
    "unknown-storage-class": [
        {"action": {"type": "SetStorageClass", "storageClass": "GLACIER"}, "condition": {"age": 1}}
    ],
    "multi-regional-in-region": [
        {
            "action": {"type": "SetStorageClass", "storageClass": "MULTI_REGIONAL"},
            "condition": {"age": 1},
        }
    ],
    "empty-condition": [{"action": {"type": "Delete"}, "condition": {}}],
    "missing-condition": [{"action": {"type": "Delete"}}],
    "missing-action": [{"condition": {"age": 1}}],
    "abort-with-is-live": [
        {"action": {"type": "AbortIncompleteMultipartUpload"}, "condition": {"isLive": True}}
    ],
    "abort-with-prefix": [
        {
            "action": {"type": "AbortIncompleteMultipartUpload"},
            "condition": {"age": 1, "matchesPrefix": ["uploads/"]},
        }
    ],
    "negative-age": [{"action": {"type": "Delete"}, "condition": {"age": -1}}],
    "created-before-timestamp": [
        {"action": {"type": "Delete"}, "condition": {"createdBefore": "2020-01-01T00:00:00Z"}}
    ],
    "unknown-matched-class": [
        {"action": {"type": "Delete"}, "condition": {"matchesStorageClass": ["GLACIER"]}}
    ],
    "negative-newer-versions": [
        {"action": {"type": "Delete"}, "condition": {"numNewerVersions": -1}}
    ],
    "unknown-condition": [{"action": {"type": "Delete"}, "condition": {"ageInDays": 1}}],
}


@pytest.mark.gcp_scenario(
    id="storage-lifecycle-rules",
    service="storage",
    transport="rest-json-v1",
    versions={
        "sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_lifecycle_rules(storage, gcp_snapshot):
    observed = StorageObservations(storage)

    def patch(body, path=storage.path, actor="fixture", query=None):
        # GCP allows about one update per second to a bucket or object, and
        # these patches run back to back: wait out a 429 on live runs.
        deadline = time.monotonic() + (RATE_LIMIT_SECONDS if storage.sandbox else 0)
        while True:
            response = storage.client.request(actor, "PATCH", path, body=body, query=query)
            if response.status != 429 or time.monotonic() >= deadline:
                return response
            time.sleep(2)

    def lifecycle(response):
        if response.status >= 400:
            return observed.error(response)
        value = response.json()
        return {
            "status": response.status,
            "has_lifecycle": "lifecycle" in value,
            "lifecycle": value.get("lifecycle"),
        }

    gcp_snapshot.match("rules-set", lifecycle(patch({"lifecycle": {"rule": RULES}})))
    gcp_snapshot.match(
        "rules-read", lifecycle(storage.client.request("fixture", "GET", storage.path))
    )
    for label, rules in INVALID.items():
        gcp_snapshot.match(label, lifecycle(patch({"lifecycle": {"rule": rules}})))
    gcp_snapshot.match(
        "rules-after-refusals", lifecycle(storage.client.request("fixture", "GET", storage.path))
    )
    gcp_snapshot.match("rules-emptied", lifecycle(patch({"lifecycle": {"rule": []}})))
    gcp_snapshot.match("rules-reset", lifecycle(patch({"lifecycle": {"rule": RULES[:1]}})))
    gcp_snapshot.match("rules-cleared", lifecycle(patch({"lifecycle": None})))
    gcp_snapshot.match(
        "denied-update", lifecycle(patch({"lifecycle": {"rule": RULES[:1]}}, actor="denied"))
    )

    def custom_time(response):
        if response.status >= 400:
            return observed.error(response)
        return {"status": response.status, "customTime": response.json().get("customTime")}

    storage.upload("aged", b"aged").require(200)
    aged = storage.path + "/o/aged"
    gcp_snapshot.match(
        "custom-time-set",
        custom_time(patch({"customTime": "2026-01-01T00:00:00Z"}, aged, "allowed")),
    )
    gcp_snapshot.match(
        "custom-time-earlier",
        custom_time(patch({"customTime": "2025-01-01T00:00:00Z"}, aged, "allowed")),
    )
    gcp_snapshot.match(
        "custom-time-removed", custom_time(patch({"customTime": None}, aged, "allowed"))
    )
    gcp_snapshot.match(
        "custom-time-later",
        custom_time(patch({"customTime": "2026-02-01T12:30:00.5Z"}, aged, "allowed")),
    )
    gcp_snapshot.match("custom-time-read", custom_time(storage.read("aged")))
