"""Bucket defaults, label merge/removal, and conditional updates needed by Terraform."""

import datetime
import platform

import pytest

from gcp_validation.services.storage.normalization import StorageObservations


@pytest.mark.gcp_scenario(
    id="storage-bucket-updates", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_bucket_updates(storage, gcp_snapshot):
    observed = StorageObservations(storage)

    def get():
        return storage.client.request("fixture", "GET", storage.path).require(200)

    def patch(labels, generation, actor="fixture"):
        return storage.client.request(actor, "PATCH", storage.path,
            query={"ifMetagenerationMatch": generation}, body={"labels": labels})

    def metadata(value):
        assert datetime.datetime.fromisoformat(value["updated"].replace("Z", "+00:00")) >= datetime.datetime.fromisoformat(value["timeCreated"].replace("Z", "+00:00"))
        return {"project_number": observed.text(value["projectNumber"]), "rpo": value.get("rpo"),
                "metageneration": observed.reference("bucket-metageneration", value["metageneration"]),
                "etag": observed.reference("bucket-etag", value["etag"]),
                "labels": {k: v for k, v in value.get("labels", {}).items() if k.startswith("tf-test-")},
                "timestamps_present_and_ordered": True}

    original = get()
    gcp_snapshot.match("initial-metadata", metadata(original))
    updated = patch({"tf-test-one": "first", "tf-test-two": "second"}, original["metageneration"]).require(200)
    assert int(updated["metageneration"]) > int(original["metageneration"])
    assert updated["etag"] != original["etag"]
    assert all(updated["labels"][key] == value for key, value in original["labels"].items())
    gcp_snapshot.match("labels-added", metadata(updated))
    reduced = patch({"tf-test-one": "changed", "tf-test-two": None}, updated["metageneration"]).require(200)
    assert "tf-test-two" not in reduced["labels"]
    gcp_snapshot.match("label-removed", metadata(reduced))
    stale = patch({"tf-test-one": "stale"}, updated["metageneration"])
    stale.require(412)
    gcp_snapshot.match("stale-update", observed.error(stale))
    denied = patch({"tf-test-one": "denied"}, reduced["metageneration"], actor="denied")
    denied.require(403)
    gcp_snapshot.match("denied-update", observed.error(denied))
    after = get()
    assert after["labels"] == reduced["labels"] and after["metageneration"] == reduced["metageneration"] and after["etag"] == reduced["etag"]
    gcp_snapshot.match("unchanged-after-failures", metadata(after))
    restored = patch({"tf-test-one": None}, after["metageneration"]).require(200)
    assert restored["labels"] == original["labels"]
    gcp_snapshot.match("labels-restored", metadata(restored))
