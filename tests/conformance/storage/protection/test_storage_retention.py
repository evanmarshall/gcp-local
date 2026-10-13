"""Bucket retention policies and object holds: what they refuse and how they render.

An unlocked retention policy refuses deleting or replacing an object until its
retentionExpirationTime and can be removed. A temporary or event-based hold
refuses the same until released. defaultEventBasedHold puts an event-based
hold on new objects; with a retention policy, an event-based hold's release
starts the object's retention period.
"""

import platform
import re
from datetime import timedelta

import pytest

from gcp_validation.services.storage.configured import parse_time
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
PERIOD = 3600
TIMESTAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)")


def errors(observed, response):
    """A refusal with its timestamps replaced, since retention errors name the expiry."""
    selected = observed.error(response)
    selected["message"] = TIMESTAMP.sub("<time>", selected["message"])
    return selected


@pytest.mark.gcp_scenario(
    id="storage-retention-holds",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_retention_holds(bucket, gcp_snapshot):
    observed = StorageObservations(bucket)

    def protection(item):
        expiry = item.get("retentionExpirationTime")
        return {
            "temporaryHold": item.get("temporaryHold"),
            "eventBasedHold": item.get("eventBasedHold"),
            "has_retention_expiration": expiry is not None,
            "retention_from_creation": expiry is not None
            and parse_time(expiry) == parse_time(item["timeCreated"]) + timedelta(seconds=PERIOD),
        }

    def outcome(response, shape=protection):
        if response.status >= 400:
            return errors(observed, response)
        if response.status == 204:
            return {"status": 204}
        return {"status": response.status, **shape(response.json())}

    def policy(value):
        retention = value.get("retentionPolicy")
        return {
            "has_policy": retention is not None,
            "fields": sorted(retention) if retention else None,
            "retentionPeriod": retention.get("retentionPeriod") if retention else None,
            "defaultEventBasedHold": value.get("defaultEventBasedHold"),
        }

    def patch_object(name, body, query=None):
        return bucket.client.request(
            "allowed", "PATCH", bucket.object_path(name), body=body, query=query
        )

    def delete(name):
        return bucket.client.request("allowed", "DELETE", bucket.object_path(name))

    gcp_snapshot.match(
        "policy-set",
        outcome(bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": str(PERIOD)}}), policy),
    )
    for label, period in {
        "policy-zero": "0",
        "policy-negative": "-1",
        "policy-text": "soon",
    }.items():
        gcp_snapshot.match(
            label,
            outcome(bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": period}}), policy),
        )
    gcp_snapshot.match(
        "policy-read", outcome(bucket.client.request("fixture", "GET", bucket.path), policy)
    )
    gcp_snapshot.match(
        "retained-upload", outcome(bucket.put("kept", b"kept", query={"ifGenerationMatch": 0}))
    )
    gcp_snapshot.match("retained-delete", outcome(delete("kept")))
    gcp_snapshot.match("retained-overwrite", outcome(bucket.put("kept", b"again")))
    gcp_snapshot.match(
        "retained-patch", outcome(patch_object("kept", {"metadata": {"note": "kept"}}))
    )
    gcp_snapshot.match(
        "object-retention-unavailable",
        outcome(
            patch_object(
                "kept",
                {"retention": {"mode": "Unlocked", "retainUntilTime": "2099-01-01T00:00:00Z"}},
            )
        ),
    )
    gcp_snapshot.match(
        "policy-removed", outcome(bucket.patch_bucket({"retentionPolicy": None}), policy)
    )
    gcp_snapshot.match("released-read", outcome(bucket.read("kept")))
    gcp_snapshot.match("released-delete", outcome(bucket.released_delete("kept")))

    bucket.put("held", b"held", query={"ifGenerationMatch": 0}).require(200)
    gcp_snapshot.match("temporary-hold-set", outcome(patch_object("held", {"temporaryHold": True})))
    gcp_snapshot.match("temporary-hold-delete", outcome(delete("held")))
    gcp_snapshot.match("temporary-hold-overwrite", outcome(bucket.put("held", b"again")))
    gcp_snapshot.match(
        "temporary-hold-released", outcome(patch_object("held", {"temporaryHold": False}))
    )
    gcp_snapshot.match("event-hold-set", outcome(patch_object("held", {"eventBasedHold": True})))
    gcp_snapshot.match("event-hold-delete", outcome(delete("held")))
    gcp_snapshot.match(
        "event-hold-released", outcome(patch_object("held", {"eventBasedHold": False}))
    )
    gcp_snapshot.match(
        "hold-denied",
        outcome(
            bucket.client.request(
                "denied", "PATCH", bucket.object_path("held"), body={"temporaryHold": True}
            )
        ),
    )
    gcp_snapshot.match("released-hold-delete", outcome(bucket.released_delete("held")))

    gcp_snapshot.match(
        "default-hold-set", outcome(bucket.patch_bucket({"defaultEventBasedHold": True}), policy)
    )
    gcp_snapshot.match(
        "default-hold-upload",
        outcome(bucket.put("defaulted", b"defaulted", query={"ifGenerationMatch": 0})),
    )
    gcp_snapshot.match("default-hold-delete", outcome(delete("defaulted")))
    gcp_snapshot.match(
        "default-hold-cleared",
        outcome(bucket.patch_bucket({"defaultEventBasedHold": False}), policy),
    )
    gcp_snapshot.match(
        "after-default-upload",
        outcome(bucket.put("later", b"later", query={"ifGenerationMatch": 0})),
    )

    bucket.patch_bucket({"retentionPolicy": {"retentionPeriod": str(PERIOD)}}).require(200)
    event = bucket.put("event", b"event", query={"ifGenerationMatch": 0}).require(200)
    gcp_snapshot.match(
        "event-hold-with-policy", outcome(patch_object("event", {"eventBasedHold": True}))
    )
    released = patch_object("event", {"eventBasedHold": False})
    gcp_snapshot.match("event-hold-release-with-policy", outcome(released))
    expiry = released.json().get("retentionExpirationTime")
    gcp_snapshot.match(
        "release-starts-retention",
        {
            "after_creation_period": expiry is not None
            and parse_time(expiry) > parse_time(event["timeCreated"]) + timedelta(seconds=PERIOD),
            "from_update": expiry is not None
            and parse_time(expiry)
            == parse_time(released.json()["updated"]) + timedelta(seconds=PERIOD),
        },
    )
