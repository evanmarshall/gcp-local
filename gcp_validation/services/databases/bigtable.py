"""Bounded Bigtable admin recording.

Clusters bill while they exist, so the scenario uses two instances of one HDD node
each (the second only for list order) and deletes both at the end.
"""

import argparse
import re
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from gcp_validation.config import Target
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_paths import PrefixClient
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response
from gcp_validation.sandbox import Sandbox

SERVICE = "bigtable"
LIMITS = {
    "max_instances": 2,
    "max_clusters": 2,
    "max_nodes": 1,
    "max_tables": 3,
    "max_run_seconds": 1500,
}
LIVE_ENDPOINT = "https://bigtableadmin.googleapis.com"
ZONE = "us-central1-b"


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class AdminClient(PrefixClient):
    """Admin paths and this project's operations, plus an anonymous caller.

    Operations live under /v2/operations/projects/P/; the anonymous caller sends no
    credentials.
    """

    prefix = "/v2"

    def request(self, actor: str, method: str, path: str, **options: object) -> Response:
        """Send one request; operation reads use the /v2/operations prefix."""
        if actor == "anonymous":
            return self.anonymous(method, path, **options)
        if path.startswith(f"/v2/operations/projects/{self.target.project}/") and method == "GET":
            self.prefix = "/v2/operations"
            try:
                return super().request(actor, method, path, **options)
            finally:
                self.prefix = "/v2"
        return super().request(actor, method, path, **options)

    def anonymous(
        self,
        method: str,
        path: str,
        *,
        query: dict | None = None,
        body: object = None,
        timeout: float = 30,
    ) -> Response:
        """Send one bodiless request without credentials."""
        if body is not None or not path.startswith(f"/v2/projects/{self.target.project}/"):
            raise ValueError("Anonymous calls are bodiless reads in the selected project")
        url = self.endpoint + path + ("?" + urlencode(query) if query else "")
        try:
            response = self.opener.open(Request(url, method=method), timeout=timeout)  # noqa: S310 -- fixed endpoint
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive("Transport failed; no cloud fallback is available") from None
        with response:
            return Response(response.code, response.read(1 << 20))


class InstanceFixture(OwnedRun):
    """Two labeled instances, each with a single one-node HDD cluster."""

    def __init__(
        self, client: AdminClient, state_dir: Path, *, manifest: str | None = None
    ) -> None:
        """Name the run's instances from its manifest."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("instance") == "gl-" + v["run_id"][:12],
        )
        self.manifest.setdefault("instance", "gl-" + self.manifest["run_id"][:12])
        self.instance = self.manifest["instance"]
        self.second = self.instance + "-b"
        self.cluster = self.instance + "-c1"
        self.base = f"/v2/projects/{self.project}/instances"
        self.labels = {"managed-by": "gcp-local", "gcp-local-run": self.manifest["run_id"]}

    def call(
        self,
        method: str,
        suffix: str = "",
        *,
        actor: str = "fixture",
        body: object = None,
        query: dict | None = None,
        instance: str | None = None,
    ) -> Response:
        """Call one path under an owned instance."""
        return self.client.request(
            actor,
            method,
            f"{self.base}/{instance or self.instance}{suffix}",
            body=body,
            query=query,
            timeout=self.remaining(),
        )

    def create_instance(self, instance: str | None = None) -> Response:
        """Create one owned instance with its one-node cluster."""
        instance = instance or self.instance
        return self.client.request(
            "fixture",
            "POST",
            self.base,
            timeout=self.remaining(),
            body={
                "parent": f"projects/{self.project}",
                "instanceId": instance,
                "instance": {
                    "displayName": "gcp-local conformance",
                    "type": "PRODUCTION",
                    "labels": self.labels,
                },
                "clusters": {
                    instance + "-c1": {
                        "location": f"projects/{self.project}/locations/{ZONE}",
                        "serveNodes": LIMITS["max_nodes"],
                        "defaultStorageType": "HDD",
                    }
                },
            },
        )

    def finish(self, response: Response) -> Response:
        """Poll a long-running operation to completion; return the final operation."""
        operation = response.json()
        while response.status == 200 and not operation.get("done"):
            time.sleep(3 if self.client.sandbox else 0.2)
            response = self.client.request(
                "fixture", "GET", "/v2/" + operation["name"], timeout=self.remaining()
            )
            operation = response.json()
        return response

    def setup(self) -> Response:
        """Create the first instance after checking the sandbox holds none."""
        self.acquire()
        self.client.verify_project()
        listing = self.client.request("fixture", "GET", self.base)
        if listing.status != 200:
            raise Inconclusive(f"Instance listing returned HTTP {listing.status}")
        if listing.json().get("instances"):
            raise Inconclusive(
                "Bigtable sandbox already holds an instance; refusing to add billable capacity"
            )
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create_instance()

    def cleanup(self) -> None:
        """Delete both owned instances."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for instance in (self.instance, self.second):
                self.remove(f"{self.base}/{instance}")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def remove(self, path: str) -> None:
        """Delete one owned instance if it exists, and confirm it is gone."""
        current = self.client.request("fixture", "GET", path)
        if current.status == 200:
            if current.json().get("labels", {}).get("gcp-local-run") != self.manifest["run_id"]:
                raise ValueError(
                    "Refusing Bigtable cleanup: instance ownership does not match the manifest"
                )
            deleted = self.client.request("fixture", "DELETE", path)
            if deleted.status not in {200, 404}:
                raise ValueError(f"Owned instance delete returned HTTP {deleted.status}")
        elif current.status != 404:
            raise ValueError(f"Owned instance read returned HTTP {current.status}")
        if self.client.request("fixture", "GET", path).status != 404:
            raise ValueError("Owned instance remains after deletion")


def operation(response: Response) -> dict[str, Any]:
    """Select a finished operation's shape."""
    value = response.json()
    result = value.get("response", {})
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "done": value.get("done"),
        "metadata_type": value.get("metadata", {}).get("@type"),
        "response_type": result.get("@type"),
        "response_fields": sorted(result),
        "error": value.get("error", {}).get("code"),
    }


def outcome(observe: Observations, response: Response) -> dict[str, Any]:
    """Select an error with its message and details, or a success's status and fields."""
    if response.status >= 400:
        return detailed(observe, response)
    return {"http_status": response.status, "fields": sorted(response.json())}


def started(observe: Observations, response: Response) -> dict[str, Any]:
    """Select an operation as first returned: its shape and whether it is done."""
    value = response.json()
    name = re.sub(r"/operations/[^/]+$", "/operations/<id>", observe.text(value.get("name", "")))
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "done": value.get("done", False),
        "name": name,
        "metadata_type": value.get("metadata", {}).get("@type"),
    }


def observe_instances(
    fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Observe a second instance for list order, list page refusals, and partial updates."""
    second = fixture.create_instance(fixture.second)
    snapshot.match("second-instance-started", started(observe, second))
    snapshot.match("second-instance-operation", operation(fixture.finish(second)))
    listed = fixture.client.request("fixture", "GET", fixture.base, timeout=fixture.remaining())
    instances = listed.json().get("instances", [])
    owned = [
        observe.text(item["name"])
        for item in instances
        if item["name"].rsplit("/", 1)[-1] in (fixture.instance, fixture.second)
    ]
    snapshot.match(
        "instances-list",
        {
            "http_status": listed.status,
            "fields": sorted(listed.json()),
            "owned_order": owned,
            "item_fields": sorted(instances[0]) if instances else [],
        },
    )
    bad = fixture.client.request(
        "fixture",
        "GET",
        fixture.base,
        query={"pageToken": "not-a-page-token"},
        timeout=fixture.remaining(),
    )
    snapshot.match("instances-bad-token", outcome(observe, bad))
    renamed = fixture.call(
        "PATCH", query={"updateMask": "displayName"}, body={"displayName": "gcp-local renamed"}
    )
    snapshot.match("rename-started", started(observe, renamed))
    snapshot.match("rename-operation", operation(fixture.finish(renamed)))
    snapshot.match("renamed-read", observe.select(fixture.call("GET"), ("displayName", "labels")))
    relabeled = fixture.finish(
        fixture.call(
            "PATCH",
            query={"updateMask": "labels"},
            body={"labels": {**fixture.labels, "stage": "patched"}},
        )
    )
    snapshot.match("relabel-operation", operation(relabeled))
    snapshot.match("relabeled-read", observe.select(fixture.call("GET"), ("displayName", "labels")))
    snapshot.match(
        "update-without-mask", outcome(observe, fixture.call("PATCH", body={"displayName": "x"}))
    )
    snapshot.match("anonymous-read", outcome(observe, fixture.call("GET", actor="anonymous")))


def observe_viewer(
    fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations, client: AdminClient
) -> None:
    """Grant viewer on the instance under a condition that names the instance only."""
    policy = fixture.call("POST", ":getIamPolicy", body={"options": {"requestedPolicyVersion": 3}})
    snapshot.match("policy-read", observe.select(policy, ("bindings",)))
    name = f"projects/{fixture.project}/instances/{fixture.instance}"
    expression = (
        'resource.type == "bigtableadmin.googleapis.com/Instance" && '
        f'resource.service == "bigtableadmin.googleapis.com" && resource.name == "{name}"'
    )
    granted = fixture.call(
        "POST",
        ":setIamPolicy",
        body={
            "policy": {
                "version": 3,
                "etag": policy.json().get("etag", ""),
                "bindings": [
                    {
                        "role": "roles/bigtable.viewer",
                        "members": ["serviceAccount:" + client.emails["allowed"]],
                        "condition": {"title": "instance-only", "expression": expression},
                    }
                ],
            }
        },
    )
    snapshot.match("viewer-granted", observe.select(granted, ("bindings", "version")))
    allowed = fixture.wait_until(lambda: fixture.call("GET", actor="allowed"), {403}, 180)
    snapshot.match("viewer-read", {"http_status": allowed.status})
    snapshot.match(
        "viewer-cannot-create-table",
        outcome(
            observe,
            fixture.call(
                "POST", "/tables", actor="allowed", body={"tableId": "denied", "table": {}}
            ),
        ),
    )


def observe_tables(fixture: InstanceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Observe table list order and paging, refused bodies, and deletion protection."""
    zero = fixture.call("POST", "/tables", body={"tableId": "t0", "table": {}})
    if zero.status != 200:
        raise AssertionError(f"Second table create returned HTTP {zero.status}")
    snapshot.match(
        "condition-excludes-table",
        outcome(observe, fixture.call("GET", "/tables/t1", actor="allowed")),
    )
    listed = fixture.call("GET", "/tables")
    snapshot.match(
        "tables-list",
        {
            "http_status": listed.status,
            "fields": sorted(listed.json()),
            "order": [observe.text(t["name"]) for t in listed.json().get("tables", [])],
        },
    )
    first = fixture.call("GET", "/tables", query={"pageSize": "1"})
    following = first.json().get("nextPageToken", "")
    second = fixture.call("GET", "/tables", query={"pageSize": "1", "pageToken": following})
    snapshot.match(
        "tables-paged",
        {
            "first_count": len(first.json().get("tables", [])),
            "first_has_next": bool(following),
            "second_status": second.status,
            "second_count": len(second.json().get("tables", [])),
        },
    )
    for label, query in (
        ("tables-bad-token", {"pageToken": "not-a-page-token"}),
        ("tables-negative-size", {"pageSize": "-1"}),
    ):
        response = fixture.call("GET", "/tables", query=query)
        snapshot.match(label, outcome(observe, response))
    snapshot.match(
        "table-unknown-field",
        outcome(
            observe,
            fixture.call("POST", "/tables", body={"tableId": "t9", "table": {}, "bogus": 1}),
        ),
    )
    snapshot.match(
        "drop-unknown-family",
        outcome(
            observe,
            fixture.call(
                "POST",
                "/tables/t1:modifyColumnFamilies",
                body={"modifications": [{"id": "nope", "drop": True}]},
            ),
        ),
    )
    protect = fixture.call(
        "PATCH",
        "/tables/t1",
        query={"updateMask": "deletionProtection"},
        body={"deletionProtection": True},
    )
    snapshot.match("protect-started", started(observe, protect))
    snapshot.match("protect-operation", operation(fixture.finish(protect)))
    snapshot.match("protected-delete", outcome(observe, fixture.call("DELETE", "/tables/t1")))
    unprotect = fixture.finish(
        fixture.call(
            "PATCH",
            "/tables/t1",
            query={"updateMask": "deletionProtection"},
            body={"deletionProtection": False},
        )
    )
    snapshot.match("unprotect-operation", operation(unprotect))


def run_scenario(fixture: InstanceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.cluster: "<cluster>",
            fixture.second: "<second>",
            fixture.instance: "<instance>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
            **{email: "<" + actor + ">" for actor, email in client.emails.items()},
        }
    )
    snapshot.match("instance-started", started(observe, created))
    snapshot.match("instance-operation", operation(fixture.finish(created)))
    snapshot.match(
        "instance-read",
        observe.select(fixture.call("GET"), ("name", "displayName", "state", "type", "labels")),
    )
    clusters = fixture.call("GET", "/clusters")
    snapshot.match(
        "clusters",
        {
            "http_status": clusters.status,
            "clusters": [
                {
                    "fields": sorted(item),
                    "name": observe.text(item["name"]),
                    "location": observe.text(item["location"]),
                    "state": item.get("state"),
                    "serveNodes": item.get("serveNodes"),
                    "defaultStorageType": item.get("defaultStorageType"),
                }
                for item in clusters.json().get("clusters", [])
            ],
        },
    )
    snapshot.match("denied-read", outcome(observe, fixture.call("GET", actor="denied")))
    observe_instances(fixture, snapshot, observe)
    observe_viewer(fixture, snapshot, observe, client)
    table = fixture.call(
        "POST", "/tables", body={"tableId": "t1", "table": {"columnFamilies": {"cf1": {}}}}
    )
    snapshot.match(
        "table-created", observe.select(table, ("name", "columnFamilies", "granularity"))
    )
    modified = fixture.call(
        "POST",
        "/tables/t1:modifyColumnFamilies",
        body={"modifications": [{"id": "cf2", "create": {}}]},
    )
    snapshot.match("families-modified", observe.select(modified, ("name", "columnFamilies")))
    snapshot.match(
        "table-read", observe.select(fixture.call("GET", "/tables/t1"), ("name", "columnFamilies"))
    )
    snapshot.match(
        "duplicate-table",
        outcome(observe, fixture.call("POST", "/tables", body={"tableId": "t1", "table": {}})),
    )
    observe_tables(fixture, snapshot, observe)
    dropped = fixture.call("DELETE", "/tables/t1")
    snapshot.match("table-deleted", {"http_status": dropped.status, "body": dropped.json()})
    snapshot.match("table-absent", outcome(observe, fixture.call("GET", "/tables/t1")))
    snapshot.match("duplicate-instance", outcome(observe, fixture.create_instance()))
    deleted = fixture.call("DELETE")
    snapshot.match("instance-deleted", {"http_status": deleted.status, "body": deleted.json()})
    snapshot.match("instance-absent", outcome(observe, fixture.call("GET")))


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned instances after an interrupted run."""
    parser = argparse.ArgumentParser(
        description="Delete only the owned instance after an interrupted run"
    )
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, "cleanup")
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = AdminClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    InstanceFixture(
        client,
        Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE,
        manifest=args.manifest,
    ).cleanup()
    print("Owned Bigtable cleanup passed.")  # noqa: T201 -- CLI result


if __name__ == "__main__":
    main()
