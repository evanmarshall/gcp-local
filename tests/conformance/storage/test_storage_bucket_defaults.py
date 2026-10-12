"""Buckets created as rclone 1.75.2 and gcloud 588.0.0 create them, with GCP's defaults.

Neither client sends publicAccessPrevention or a softDeletePolicy unless asked,
so GCP applies "inherited" public access prevention and a seven-day soft-delete
policy. gcloud also sends enableObjectRetention=False and projection=noAcl on
every bucket insert (apitools encodes the unset boolean as "False").

- storage-bucket-defaults: rclone's insert body; soft-deleted objects are
  hidden from reads, listed with softDeleted=true, restorable, and do not
  block a bucket delete.
- storage-gcloud-bucket-defaults: gcloud's insert with enableObjectRetention=False;
  a retention below the minimum is refused; disabling soft delete, after which
  a soft-deleted listing is refused.
- storage-public-access-enforced: gcloud's insert with
  --public-access-prevention; public bindings are refused.

Public bindings under inherited prevention are not recorded: the sandbox
organization's domain-restricted-sharing constraint refuses allUsers with 412
"One or more users named in the policy do not belong to a permitted customer."
before public access prevention applies.

Each scenario leaves its bucket soft-deleted in the sandbox for seven days; GCP
offers no way to remove it sooner (see gcp_validation/services/storage/defaults.py).
"""

from pathlib import Path

import pytest

from gcp_validation.sandbox import inspect_credentials
from gcp_validation.services.storage.client import StorageClient
from gcp_validation.services.storage.defaults import (
    ClientBucketFixture,
    soft_delete_policy,
    soft_deleted_shape,
)
from gcp_validation.services.storage.normalization import StorageObservations

GCLOUD_QUERY = {"enableObjectRetention": "False", "projection": "noAcl"}
UNIFORM = {"uniformBucketLevelAccess": {"enabled": True}}
CONTRACT = (
    "gcp_validation/services/storage/client.py",
    "gcp_validation/services/storage/defaults.py",
    "gcp_validation/services/storage/normalization.py",
    "gcp_validation/sandbox.py",
    "gcp_validation/federation.py",
)
NOT_APPLICABLE = {"provider": "not-applicable", "backend": "not-applicable"}
RCLONE = {"sdk": "rclone/1.75.2 request shape", **NOT_APPLICABLE}
GCLOUD = {"sdk": "gcloud/588.0.0 request shape", **NOT_APPLICABLE}


def owned(gcp_snapshot, gcp_target, pytestconfig, create):
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
        scope = gcp_snapshot.manifest.get("context", {}).get("scope", {})
        region = scope.get("region", "us-central1")
    client = StorageClient(gcp_target, sandbox=profile)
    fixture = ClientBucketFixture(client, state, region=region, create=create)
    try:
        fixture.setup()
        yield fixture
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"bucket_absent": True})


@pytest.fixture
def rclone_bucket(gcp_snapshot, gcp_target, pytestconfig):
    create = ({}, {"bucketPolicyOnly": {"enabled": True}})
    yield from owned(gcp_snapshot, gcp_target, pytestconfig, create)


@pytest.fixture
def gcloud_bucket(gcp_snapshot, gcp_target, pytestconfig):
    yield from owned(gcp_snapshot, gcp_target, pytestconfig, (GCLOUD_QUERY, UNIFORM))


@pytest.fixture
def gcloud_enforced_bucket(gcp_snapshot, gcp_target, pytestconfig):
    create = (GCLOUD_QUERY, {**UNIFORM, "publicAccessPrevention": "enforced"})
    yield from owned(gcp_snapshot, gcp_target, pytestconfig, create)


def bucket_shape(response):
    value = response.json()
    iam = value.get("iamConfiguration", {})
    return {
        "status": response.status,
        "public_access_prevention": iam.get("publicAccessPrevention"),
        "uniform_enabled": iam.get("uniformBucketLevelAccess", {}).get("enabled"),
        "soft_delete": soft_delete_policy(value),
        "has_object_retention": "objectRetention" in value,
    }


def read_bucket(fixture):
    return fixture.client.request("fixture", "GET", fixture.path)


def member(fixture, actor):
    if fixture.sandbox:
        return "serviceAccount:" + fixture.sandbox.identities[actor].email
    return "serviceAccount:" + actor + "@" + fixture.project + ".iam.gserviceaccount.com"


def policy_shape(observed, response):
    if response.status != 200:
        return observed.error(response)
    bindings = response.json().get("bindings", [])
    shapes = [[binding["role"], sorted(observed.text(value) for value in binding["members"])]
              for binding in bindings]
    return {"status": 200, "bindings": sorted(shapes)}


def object_user(fixture):
    return {"role": "roles/storage.objectUser", "members": [member(fixture, "allowed")]}


def viewer(principal):
    return {"role": "roles/storage.objectViewer", "members": [principal]}


def granted(fixture, bindings):
    response = fixture.grant(bindings)
    response.require(200)
    return response


def soft_listing(observed, response):
    items = response.require(200).get("items", [])
    items = sorted(items, key=lambda item: (item["name"], int(item["generation"])))
    return {"status": response.status, "items": [
        {"name": item["name"], "generation": observed.reference("generation", item["generation"]),
         **soft_deleted_shape(item)} for item in items]}


@pytest.mark.gcp_scenario(id="storage-bucket-defaults", service="storage",
                          transport="rest-json-v1", versions=RCLONE, contract_files=CONTRACT)
def test_storage_bucket_defaults(rclone_bucket, gcp_snapshot):
    fixture, observed = rclone_bucket, StorageObservations(rclone_bucket)
    gcp_snapshot.match("bucket-created", bucket_shape(fixture.creation_response))
    gcp_snapshot.match("bucket-read", bucket_shape(read_bucket(fixture)))
    granted(fixture, [object_user(fixture)])
    fixture.wait_allowed(fixture.list_objects).require(200)

    first = fixture.upload("deleted.txt", b"soft-deleted evidence\n").require(200)
    generation = first["generation"]
    gcp_snapshot.match("delete", {"status": fixture.delete("deleted.txt", generation).status})
    gcp_snapshot.match("read-after-delete", observed.error(fixture.read("deleted.txt")))
    live = fixture.list_objects().require(200)
    names = [item["name"] for item in live.get("items", [])]
    gcp_snapshot.match("live-list-after-delete", {"names": names})
    gcp_snapshot.match("soft-deleted-list", soft_listing(observed, fixture.soft_deleted()))
    got = fixture.client.request("allowed", "GET", fixture.path + "/o/deleted.txt",
                                 query={"softDeleted": "true", "generation": generation})
    soft = got.require(200)
    gcp_snapshot.match("soft-deleted-get", {"status": got.status, **observed.object(soft),
                                            **soft_deleted_shape(soft)})
    denied = fixture.restore("deleted.txt", generation, actor="denied")
    gcp_snapshot.match("restore-denied", observed.error(denied))
    restored = fixture.restore("deleted.txt", generation)
    value = restored.require(200)
    assert value["generation"] != generation
    gcp_snapshot.match("restore", {"status": restored.status, **observed.object(value),
                                   **soft_deleted_shape(value)})
    content = fixture.read("deleted.txt", media=True)
    gcp_snapshot.match("restored-content", observed.content(content))
    after_restore = soft_listing(observed, fixture.soft_deleted())
    gcp_snapshot.match("soft-deleted-after-restore", after_restore)

    original = fixture.upload("replaced.txt", b"first version\n").require(200)
    replaced = fixture.upload("replaced.txt", b"second version\n",
                              generation=original["generation"]).require(200)
    after_overwrite = soft_listing(observed, fixture.soft_deleted())
    gcp_snapshot.match("soft-deleted-after-overwrite", after_overwrite)

    # rclone's rmdir: delete the live objects, then the bucket. Only
    # soft-deleted objects remain, which do not block the delete.
    fixture.delete("deleted.txt", value["generation"]).require(204)
    fixture.delete("replaced.txt", replaced["generation"]).require(204)
    removed = fixture.client.request("fixture", "DELETE", fixture.path)
    gcp_snapshot.match("bucket-delete-with-soft-deleted-objects", {"status": removed.status})
    gcp_snapshot.match("bucket-read-after-delete", observed.error(read_bucket(fixture)))


@pytest.mark.gcp_scenario(id="storage-gcloud-bucket-defaults", service="storage",
                          transport="rest-json-v1", versions=GCLOUD, contract_files=CONTRACT)
def test_storage_gcloud_bucket_defaults(gcloud_bucket, gcp_snapshot):
    fixture, observed = gcloud_bucket, StorageObservations(gcloud_bucket)
    gcp_snapshot.match("bucket-created", bucket_shape(fixture.creation_response))
    gcp_snapshot.match("bucket-read", bucket_shape(read_bucket(fixture)))
    short = fixture.client.request("fixture", "PATCH", fixture.path,
                                   body={"softDeletePolicy": {"retentionDurationSeconds": "3600"}})
    gcp_snapshot.match("soft-delete-below-minimum", observed.error(short))
    patched, listing = fixture.disable_soft_delete()
    gcp_snapshot.match("soft-delete-disabled", {
        "status": patched.status, "soft_delete": soft_delete_policy(patched.json()),
        "metageneration": patched.json()["metageneration"],
    })
    gcp_snapshot.match("soft-deleted-list-without-policy", observed.error(listing))


@pytest.mark.gcp_scenario(id="storage-public-access-enforced", service="storage",
                          transport="rest-json-v1", versions=GCLOUD, contract_files=CONTRACT)
def test_storage_public_access_enforced(gcloud_enforced_bucket, gcp_snapshot):
    fixture, observed = gcloud_enforced_bucket, StorageObservations(gcloud_enforced_bucket)
    gcp_snapshot.match("bucket-created", bucket_shape(fixture.creation_response))
    private = granted(fixture, [object_user(fixture)])
    gcp_snapshot.match("private-binding", policy_shape(observed, private))
    for principal, label in (("allUsers", "all-users"),
                             ("allAuthenticatedUsers", "all-authenticated-users")):
        refused = fixture.grant([object_user(fixture), viewer(principal)])
        gcp_snapshot.match("binding-" + label, policy_shape(observed, refused))
    current = fixture.client.request("fixture", "GET", fixture.path + "/iam")
    gcp_snapshot.match("policy-after-refusals", policy_shape(observed, current))
