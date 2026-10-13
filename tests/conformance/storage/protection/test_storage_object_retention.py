"""Object retention in a bucket created with enableObjectRetention=true.

An object's retention configuration refuses deleting or replacing it until
retainUntilTime. Unlocked retention can be shortened or removed only with
overrideUnlockedRetention=true; locked retention can't be removed. The locked
configuration here retains for seconds, so cleanup waits it out.
"""

import platform
import re
from datetime import UTC, datetime, timedelta

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
TIMESTAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)")
LOCKED_SECONDS = 20


def later(seconds):
    return (datetime.now(UTC) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ] + "Z"


@pytest.mark.gcp_scenario(
    id="storage-object-retention",
    service="storage",
    transport="rest-json-v1",
    versions=VERSIONS,
    contract_files=CONTRACT,
)
def test_storage_object_retention(retention_bucket, gcp_snapshot):
    bucket = retention_bucket
    observed = StorageObservations(bucket)

    def errors(response):
        selected = observed.error(response)
        selected["message"] = TIMESTAMP.sub("<time>", selected["message"])
        return selected

    def retention(item):
        value = item.get("retention")
        return {
            "has_retention": value is not None,
            "mode": value.get("mode") if value else None,
            "has_retain_until": bool(value and value.get("retainUntilTime")),
        }

    def outcome(response, shape=retention):
        if response.status >= 400:
            return errors(response)
        if response.status == 204:
            return {"status": 204}
        return {"status": response.status, **shape(response.json())}

    def patch(name, body, actor="fixture", **query):
        return bucket.client.request(
            actor, "PATCH", bucket.object_path(name), body=body, query=query or None
        )

    def delete(name):
        return bucket.client.request("allowed", "DELETE", bucket.object_path(name))

    created = bucket.creation_response.json()
    gcp_snapshot.match(
        "bucket-object-retention", {"objectRetention": created.get("objectRetention")}
    )
    gcp_snapshot.match(
        "bucket-read",
        {
            "objectRetention": bucket.client.request("fixture", "GET", bucket.path)
            .require(200)
            .get("objectRetention")
        },
    )

    bucket.put("unlocked", b"unlocked", query={"ifGenerationMatch": 0}).require(200)
    unlocked = {"mode": "Unlocked", "retainUntilTime": later(3600)}
    gcp_snapshot.match(
        "unlocked-set-denied", outcome(patch("unlocked", {"retention": unlocked}, actor="allowed"))
    )
    gcp_snapshot.match("unlocked-set", outcome(patch("unlocked", {"retention": unlocked})))
    gcp_snapshot.match("unlocked-delete", outcome(delete("unlocked")))
    gcp_snapshot.match("unlocked-overwrite", outcome(bucket.put("unlocked", b"again")))
    gcp_snapshot.match(
        "unlocked-metadata",
        outcome(patch("unlocked", {"metadata": {"note": "kept"}}, actor="allowed")),
    )
    gcp_snapshot.match(
        "unlocked-shortened",
        outcome(
            patch("unlocked", {"retention": {"mode": "Unlocked", "retainUntilTime": later(1800)}})
        ),
    )
    gcp_snapshot.match("unlocked-removed", outcome(patch("unlocked", {"retention": None})))
    gcp_snapshot.match(
        "unlocked-extended",
        outcome(
            patch("unlocked", {"retention": {"mode": "Unlocked", "retainUntilTime": later(7200)}})
        ),
    )
    gcp_snapshot.match(
        "unlocked-overridden-denied",
        outcome(
            patch(
                "unlocked", {"retention": None}, actor="allowed", overrideUnlockedRetention="true"
            )
        ),
    )
    gcp_snapshot.match(
        "unlocked-overridden",
        outcome(patch("unlocked", {"retention": None}, overrideUnlockedRetention="true")),
    )
    gcp_snapshot.match("unlocked-released-delete", outcome(bucket.released_delete("unlocked")))

    bucket.put("invalid", b"invalid", query={"ifGenerationMatch": 0}).require(200)
    for label, value in {
        "mode-unknown": {"mode": "Forever", "retainUntilTime": later(3600)},
        "until-past": {"mode": "Unlocked", "retainUntilTime": "2020-01-01T00:00:00Z"},
        "until-missing": {"mode": "Unlocked"},
        "mode-missing": {"retainUntilTime": later(3600)},
    }.items():
        gcp_snapshot.match(label, outcome(patch("invalid", {"retention": value}, actor="allowed")))
    gcp_snapshot.match(
        "no-retention-removed", outcome(patch("invalid", {"retention": None}, actor="allowed"))
    )

    bucket.put("locked", b"locked", query={"ifGenerationMatch": 0}).require(200)
    gcp_snapshot.match(
        "locked-set",
        outcome(
            patch(
                "locked",
                {"retention": {"mode": "Locked", "retainUntilTime": later(LOCKED_SECONDS)}},
            )
        ),
    )
    gcp_snapshot.match(
        "locked-removed",
        outcome(patch("locked", {"retention": None}, overrideUnlockedRetention="true")),
    )
    gcp_snapshot.match(
        "locked-unlocked",
        outcome(
            patch(
                "locked",
                {"retention": {"mode": "Unlocked", "retainUntilTime": later(LOCKED_SECONDS)}},
                overrideUnlockedRetention="true",
            )
        ),
    )
    gcp_snapshot.match(
        "locked-extended",
        outcome(
            patch(
                "locked",
                {"retention": {"mode": "Locked", "retainUntilTime": later(LOCKED_SECONDS + 5)}},
            )
        ),
    )
    gcp_snapshot.match("locked-delete", outcome(delete("locked")))
