"""Bounded BigQuery API-surface recording: error bodies, patch merges, list order and paging,
jobs.list, and a conditional table grant.

The fixture identity holds roles/bigquery.admin on the project. The run owns
two datasets (the second only for list order) and attempts a third with an
unknown field, which GCP refuses; cleanup deletes all three if present. Every
query reads no rows, so none bills bytes.
"""

import json
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

import pytest

from gcp_validation.evidence import Inconclusive, SnapshotCase, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient
from gcp_validation.services.databases.bigquery import (
    LIVE_ENDPOINT,
    SCHEMA,
    BigQueryClient,
    DatasetFixture,
)

SERVICE = "bigquery"
LIMITS = {
    "max_datasets": 3,
    "max_tables": 3,
    "max_rows": 0,
    "max_queries": 6,
    "max_run_seconds": 900,
}
VIEWER = "roles/bigquery.dataViewer"


def load_scope(filename: str, sandbox: Any, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class SurfaceClient(BigQueryClient):
    """BigQueryClient plus an anonymous caller that sends no credentials."""

    def request(self, actor, method, path, *, query=None, body=None, timeout=30):
        if actor != "anonymous":
            return super().request(actor, method, path, query=query, body=body, timeout=timeout)
        if not path.startswith("/bigquery/v2/projects/" + self.target.project + "/"):
            raise ValueError("Select an explicit BigQuery path in the selected project")
        url = self.endpoint + path + ("?" + urlencode(query) if query else "")
        try:
            response = self.opener.open(Request(url, method=method), timeout=timeout)  # noqa: S310 -- fixed endpoint
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive("Transport failed; no cloud fallback is available") from None
        with response:
            return Response(response.code, response.read(1 << 20))


class SurfaceFixture(DatasetFixture):
    """The owned dataset plus a second one for list order; both carry the run's labels."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, manifest=manifest)
        self.second, self.refused = self.dataset + "_b", self.dataset + "_x"
        self.labels = {**self.labels, "extra": "1"}

    def owned(self, response):
        if response.json().get("labels", {}).get("gcp-local-run") != self.manifest["run_id"]:
            raise ValueError(
                "Refusing BigQuery cleanup: dataset ownership does not match the manifest"
            )

    def dataset_body(self, dataset, **fields):
        return {
            "datasetReference": {"projectId": self.project, "datasetId": dataset},
            "location": "US",
            "labels": self.labels,
            **fields,
        }

    def create(self, dataset, **fields):
        return self.client.request(
            "fixture",
            "POST",
            f"{self.base}/datasets",
            timeout=self.remaining(),
            body=self.dataset_body(dataset, **fields),
        )

    def setup(self):
        self.acquire()
        self.client.verify_project()
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create(self.dataset, friendlyName="gcp-local", description="owned")

    def sql(self, statement, *, actor="fixture"):
        if self.manifest["queries"] >= LIMITS["max_queries"]:
            raise ValueError("Query limit reached")
        self.manifest["queries"] += 1
        self.save()
        return self.client.request(
            actor,
            "POST",
            f"{self.base}/queries",
            timeout=self.remaining(),
            body={
                "query": statement.replace("{ds}", self.dataset),
                "useLegacySql": False,
                "location": "US",
                "timeoutMs": 20000,
            },
        )

    def cleanup(self):
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for dataset in (self.dataset, self.second, self.refused):
                path = f"{self.base}/datasets/{dataset}"
                current = self.client.request("fixture", "GET", path)
                if current.status == 200:
                    self.owned(current)
                    deleted = self.client.request(
                        "fixture", "DELETE", path, query={"deleteContents": "true"}
                    )
                    if deleted.status not in {200, 204, 404}:
                        raise ValueError(f"Owned dataset delete returned HTTP {deleted.status}")
                elif current.status != 404:
                    raise ValueError(f"Owned dataset read returned HTTP {current.status}")
                if self.client.request("fixture", "GET", path).status != 404:
                    raise ValueError("Owned dataset remains after deletion")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()


def bq_error(observe: Observations, response: Response) -> dict[str, Any]:
    """Select a v2 error: status, message, errors[] entries, and google.rpc details; or a success's
    status and fields."""
    if response.status < 400:
        return {"http_status": response.status, "fields": sorted(response.json())}
    error = response.json().get("error", {})
    details = []
    for detail in error.get("details") or ():
        selected = {"type": detail.get("@type")}
        for key in ("reason", "domain", "metadata", "fieldViolations"):
            if key in detail:
                selected[key] = observe.value(detail[key])
        details.append(selected)
    entries = [
        {
            key: observe.value(entry.get(key))
            for key in ("reason", "domain", "message", "location", "locationType")
            if key in entry
        }
        for entry in error.get("errors") or ()
    ]
    selected = {
        "http_status": response.status,
        "code": error.get("code"),
        "status": error.get("status"),
        "message": observe.text(error.get("message", "")),
        "errors": entries,
        "details": details,
    }
    check_observation(selected)
    return selected


def described(observe: Observations, response: Response) -> dict[str, Any]:
    """Select a dataset or table document's descriptors."""
    return observe.select(response, ("labels", "friendlyName", "description"))


def listing(fixture: SurfaceFixture, path: str, **query: str) -> Response:
    return fixture.client.request(
        "fixture", "GET", f"{fixture.base}{path}", query=query or None, timeout=fixture.remaining()
    )


def observe_paging(
    fixture: SurfaceFixture,
    snapshot: SnapshotCase,
    observe: Observations,
    label: str,
    path: str,
    field: str,
) -> None:
    """Page one item at a time and refuse bad page parameters."""
    first = listing(fixture, path, maxResults="1")
    following = first.json().get("nextPageToken", "")
    second = listing(fixture, path, maxResults="1", pageToken=following)
    snapshot.match(
        label + "-paged",
        {
            "first_status": first.status,
            "first_count": len(first.json().get(field, [])),
            "first_has_next": bool(following),
            "second_status": second.status,
            "second_count": len(second.json().get(field, [])),
        },
    )
    for suffix, query in (
        ("bad-token", {"pageToken": "not-a-page-token"}),
        ("negative-size", {"maxResults": "-1"}),
        ("mistyped-size", {"maxResults": "many"}),
    ):
        response = listing(fixture, path, **query)
        snapshot.match(f"{label}-{suffix}", bq_error(observe, response))


def observe_datasets(
    fixture: SurfaceFixture, snapshot: SnapshotCase, observe: Observations, created: Response
) -> None:
    """Create, refuse, read, patch, and list datasets."""
    snapshot.match("dataset-created", described(observe, created))
    snapshot.match("dataset-duplicate", bq_error(observe, fixture.create(fixture.dataset)))
    snapshot.match(
        "dataset-unknown-field", bq_error(observe, fixture.create(fixture.refused, bogus=1))
    )
    snapshot.match("dataset-missing", bq_error(observe, fixture.call("GET", "_absent")))
    snapshot.match("dataset-denied", bq_error(observe, fixture.call("GET", actor="denied")))
    snapshot.match("dataset-anonymous", bq_error(observe, fixture.call("GET", actor="anonymous")))
    kept = {key: value for key, value in fixture.labels.items() if key != "extra"}
    snapshot.match(
        "dataset-patch-labels", described(observe, fixture.call("PATCH", body={"labels": kept}))
    )
    snapshot.match(
        "dataset-patch-name",
        described(observe, fixture.call("PATCH", body={"friendlyName": "renamed"})),
    )
    # The google Terraform provider sends accessPolicyVersion on dataset reads and writes.
    for label, version in (("v3", "3"), ("v1", "1"), ("v2", "2"), ("mistyped", "many")):
        snapshot.match(
            "dataset-policy-version-" + label,
            bq_error(observe, fixture.call("GET", query={"accessPolicyVersion": version})),
        )
    snapshot.match(
        "dataset-patch-policy-v3",
        bq_error(observe, fixture.call("PATCH", query={"accessPolicyVersion": "3"}, body={})),
    )
    second = fixture.create(fixture.second)
    if second.status != 200:
        raise AssertionError(f"Second dataset create returned HTTP {second.status}")
    listed = listing(fixture, "/datasets", maxResults="1000").json()
    owned = [
        item
        for item in listed.get("datasets", [])
        if item.get("datasetReference", {}).get("datasetId") in (fixture.dataset, fixture.second)
    ]
    snapshot.match(
        "datasets-list",
        {
            "fields": sorted(listed),
            "owned_order": [observe.text(item["datasetReference"]["datasetId"]) for item in owned],
            "item_fields": sorted(owned[0]) if owned else [],
            "item": observe.value(
                {
                    key: owned[0].get(key)
                    for key in (
                        "kind",
                        "id",
                        "datasetReference",
                        "labels",
                        "location",
                        "friendlyName",
                    )
                }
            )
            if owned
            else None,
        },
    )
    observe_paging(fixture, snapshot, observe, "datasets", "/datasets", "datasets")


def observe_tables(fixture: SurfaceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Create, refuse, read, patch, and list tables."""

    def create(table, **fields):
        return fixture.call(
            "POST",
            "/tables",
            body={
                "tableReference": {
                    "projectId": fixture.project,
                    "datasetId": fixture.dataset,
                    "tableId": table,
                },
                "schema": SCHEMA,
                **fields,
            },
        )

    snapshot.match(
        "table-created",
        described(
            observe,
            create("t1", labels={"a": "1", "b": "2"}, friendlyName="one", description="first"),
        ),
    )
    zero = create("t0")
    if zero.status != 200:
        raise AssertionError(f"Second table create returned HTTP {zero.status}")
    snapshot.match("table-duplicate", bq_error(observe, create("t1")))
    snapshot.match("table-bad-id", bq_error(observe, create("bad id!")))
    snapshot.match("table-missing", bq_error(observe, fixture.call("GET", "/tables/absent")))
    snapshot.match(
        "table-denied", bq_error(observe, fixture.call("GET", "/tables/t1", actor="denied"))
    )
    snapshot.match(
        "table-patch-name",
        described(observe, fixture.call("PATCH", "/tables/t1", body={"friendlyName": "renamed"})),
    )
    snapshot.match(
        "table-patch-labels",
        described(observe, fixture.call("PATCH", "/tables/t1", body={"labels": {"a": "1"}})),
    )
    path = f"/datasets/{fixture.dataset}/tables"
    listed = listing(fixture, path).json()
    tables = listed.get("tables", [])
    snapshot.match(
        "tables-list",
        {
            "fields": sorted(listed),
            "order": [item.get("tableReference", {}).get("tableId") for item in tables],
            "totalItems": listed.get("totalItems"),
            "item_fields": sorted(tables[0]) if tables else [],
            "item": observe.value(
                {
                    key: tables[0].get(key)
                    for key in ("kind", "id", "tableReference", "type", "labels")
                }
            )
            if tables
            else None,
        },
    )
    observe_paging(fixture, snapshot, observe, "tables", path, "tables")


def observe_jobs(fixture: SurfaceFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Two queries, then jobs.list newest first, and three refused queries."""
    ids = []
    for label, statement in (("q1", "SELECT 1 AS x"), ("q2", "SELECT name FROM {ds}.t0")):
        response = fixture.sql(statement)
        if response.status != 200:
            raise AssertionError(f"Query {label} returned HTTP {response.status}")
        ids.append(response.json()["jobReference"]["jobId"])
        observe.aliases[ids[-1]] = "<" + label + ">"
    deadline = time.monotonic() + (60 if fixture.client.sandbox else 0)
    while True:
        listed = listing(fixture, "/jobs", maxResults="2")
        jobs = listed.json().get("jobs", [])
        if [job.get("jobReference", {}).get("jobId") for job in jobs] == ids[
            ::-1
        ] or time.monotonic() >= deadline:
            break
        time.sleep(5)  # A new job can take a moment to appear in the listing.
    value = listed.json()
    snapshot.match(
        "jobs-list",
        {
            "http_status": listed.status,
            "fields": sorted(value),
            "order": [observe.text(job.get("jobReference", {}).get("jobId", "")) for job in jobs],
            "item_fields": sorted(jobs[0]) if jobs else [],
            "item_state": jobs[0].get("state") if jobs else None,
            "item_status": jobs[0].get("status") if jobs else None,
            "item_configuration_fields": sorted(jobs[0].get("configuration", {})) if jobs else [],
            "has_next": bool(value.get("nextPageToken")),
        },
    )
    full = listing(fixture, "/jobs", maxResults="1", projection="full").json().get("jobs", [])
    snapshot.match(
        "jobs-list-full",
        {
            "item_fields": sorted(full[0]) if full else [],
            "query_echoed": bool(full)
            and "query" in full[0].get("configuration", {}).get("query", {}),
        },
    )
    response = listing(fixture, "/jobs", pageToken="not-a-page-token")
    snapshot.match("jobs-bad-token", bq_error(observe, response))
    snapshot.match("query-invalid", bq_error(observe, fixture.sql("SELEC 1")))
    snapshot.match(
        "query-missing-table", bq_error(observe, fixture.sql("SELECT 1 FROM {ds}.absent"))
    )
    snapshot.match("query-denied", bq_error(observe, fixture.sql("SELECT 1 AS x", actor="denied")))


def observe_conditions(
    fixture: SurfaceFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Grant dataViewer on t1 and t0 under conditions on resource.type, service, and name.

    Only t1's condition names t1, so the allowed caller reads t1's rows but not t0's.
    """
    member = "serviceAccount:" + fixture.client.emails["allowed"]
    name = f"projects/{fixture.project}/datasets/{fixture.dataset}/tables/t1"
    expression = (
        'resource.type == "bigquery.googleapis.com/Table" && '
        f'resource.service == "bigquery.googleapis.com" && resource.name == "{name}"'
    )
    results = {}
    for table in ("t1", "t0"):
        policy = fixture.call(
            "POST", f"/tables/{table}:getIamPolicy", body={"options": {"requestedPolicyVersion": 3}}
        ).json()
        granted = fixture.call(
            "POST",
            f"/tables/{table}:setIamPolicy",
            body={
                "policy": {
                    "version": 3,
                    "etag": policy.get("etag", ""),
                    "bindings": [
                        {
                            "role": VIEWER,
                            "members": [member],
                            "condition": {"title": "only-t1", "expression": expression},
                        }
                    ],
                }
            },
        )
        results[table] = granted
    granted = results["t1"]
    snapshot.match(
        "conditional-grant",
        bq_error(observe, granted)
        if granted.status >= 400
        else {
            "http_status": granted.status,
            "version": granted.json().get("version"),
            "bindings": observe.value(granted.json().get("bindings")),
        },
    )
    if granted.status != 200:
        return
    read = fixture.wait_until(
        lambda: fixture.call("GET", "/tables/t1/data", actor="allowed"), {403}, 180
    )
    snapshot.match("conditional-read-matching", {"http_status": read.status})
    snapshot.match(
        "conditional-read-other",
        bq_error(observe, fixture.call("GET", "/tables/t0/data", actor="allowed")),
    )


def run_scenario(fixture: SurfaceFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.dataset: "<dataset>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
            **{email: "<" + actor + ">" for actor, email in client.emails.items()},
        }
    )
    if created.status != 200:
        raise AssertionError(
            f"Dataset create returned HTTP {created.status}: {json.dumps(created.json())[:200]}"
        )
    fixture.owned(created)
    observe_datasets(fixture, snapshot, observe, created)
    observe_tables(fixture, snapshot, observe)
    observe_jobs(fixture, snapshot, observe)
    observe_conditions(fixture, snapshot, observe)
    snapshot.match("dataset-in-use", bq_error(observe, fixture.call("DELETE")))


ADMIN = "roles/bigquery.admin"


def local_admin(target: Any, *, grant: bool) -> None:
    """Grant or revoke the fixture's project-level BigQuery admin role locally.

    The sandbox fixture holds roles/bigquery.admin as a standing role
    (docs/contributing/gcp-sandbox.md); the conformance server's fixture holds
    only dataOwner and jobUser, which other scenarios rely on, so the local run
    stands in for the standing role for this scenario only.
    """
    client = RestClient(target, "https://cloudresourcemanager.googleapis.com")
    path = f"/v1/projects/{target.project}"
    policy = client.request("fixture", "POST", path + ":getIamPolicy", body={}).json()
    member = "serviceAccount:" + client.emails["fixture"]
    bindings = [b for b in policy.get("bindings", []) if b.get("role") != ADMIN]
    members = [
        m
        for b in policy.get("bindings", [])
        if b.get("role") == ADMIN
        for m in b["members"]
        if m != member
    ]
    members += [member] if grant else []
    if members:
        bindings.append({"role": ADMIN, "members": members})
    updated = client.request(
        "fixture",
        "POST",
        path + ":setIamPolicy",
        body={"policy": {"etag": policy.get("etag", ""), "bindings": bindings}},
    )
    if updated.status != 200:
        raise AssertionError(f"Local admin grant returned HTTP {updated.status}")


@pytest.fixture
def bigquery_api(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "bigquery-api-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = SurfaceFixture(
        SurfaceClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state
    )
    try:
        if gcp_target.mode == "local":
            local_admin(gcp_target, grant=True)
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if gcp_target.mode == "local":
            local_admin(gcp_target, grant=False)
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"datasets_absent": True})


@pytest.mark.gcp_scenario(
    id="bigquery-api-surface",
    service="bigquery",
    transport="rest-v1",
    versions={
        "sdk": "urllib;google-auth/2.56.2",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/services/databases/bigquery.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
    ),
)
def test_bigquery_api(bigquery_api, gcp_snapshot):
    fixture, created = bigquery_api
    run_scenario(fixture, gcp_snapshot, created)
