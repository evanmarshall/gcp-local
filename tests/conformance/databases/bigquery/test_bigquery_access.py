"""Bounded BigQuery dataset access recording: access entries, their IAM reflection, and
authorized views and datasets.

The fixture identity holds roles/bigquery.admin on the project. The allowed identity holds
roles/bigquery.jobUser on the project for the run only (granted before and removed after a
live run; the local run grants and revokes it through the emulator's project IAM). It reaches
the views dataset through the standing group gcp-local-nested@stitchwink.com, which contains
gcp-local-allowed@stitchwink.com, which contains it; it never holds access to the source
dataset. The denied identity holds nothing. The run owns two datasets and three tiny tables.
"""

from pathlib import Path
from typing import Any

import pytest

from gcp_validation.evidence import SnapshotCase, check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient
from gcp_validation.services.databases.bigquery import (
    LIVE_ENDPOINT,
    BigQueryClient,
    DatasetFixture,
    rows_of,
)

SERVICE = "bigquery"
LIMITS = {
    "max_datasets": 2,
    "max_tables": 3,
    "max_rows": 2,
    "max_queries": 14,
    "max_run_seconds": 900,
}
GROUP = "gcp-local-nested@stitchwink.com"
ABSENT_GROUP = "gcp-local-absent@stitchwink.com"
JOB_USER = "roles/bigquery.jobUser"


def load_scope(filename: str, sandbox: Any, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class AccessFixture(DatasetFixture):
    """The owned source dataset plus a views dataset; both carry the run's labels."""

    def __init__(self, client, state_dir, *, manifest=None):
        super().__init__(client, state_dir, manifest=manifest)
        self.views = self.dataset + "_v"

    def dataset_path(self, dataset: str) -> str:
        return f"{self.base}/datasets/{dataset}"

    def on(self, dataset: str, method: str, suffix: str = "", **options: Any) -> Response:
        """Call one owned dataset's path as an identity."""
        actor = options.pop("actor", "fixture")
        return self.client.request(
            actor, method, self.dataset_path(dataset) + suffix, timeout=self.remaining(), **options
        )

    def sql(self, statement: str, *, actor: str = "fixture") -> Response:
        """Send one jobs.query; {src} and {views} name the owned datasets."""
        if self.manifest["queries"] >= LIMITS["max_queries"]:
            raise ValueError("Query limit reached")
        self.manifest["queries"] += 1
        self.save()
        body = {
            "query": statement.replace("{src}", self.dataset).replace("{views}", self.views),
            "useLegacySql": False,
            "location": "US",
            "timeoutMs": 30000,
        }
        return self.client.request(
            actor, "POST", f"{self.base}/queries", timeout=self.remaining(), body=body
        )

    def add_access(self, dataset: str, *entries: dict[str, Any]) -> Response:
        """Read the dataset and patch its access list with entries appended."""
        current = self.on(dataset, "GET").json().get("access", [])
        return self.on(dataset, "PATCH", body={"access": [*current, *entries]})

    def setup(self):
        created = super().setup()
        views = self.client.request(
            "fixture",
            "POST",
            f"{self.base}/datasets",
            timeout=self.remaining(),
            body={
                "datasetReference": {"projectId": self.project, "datasetId": self.views},
                "location": "US",
                "labels": self.labels,
            },
        )
        if views.status != 200:
            raise AssertionError(f"Views dataset create returned HTTP {views.status}")
        return created

    def cleanup(self):
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for dataset in (self.views, self.dataset):
                path = self.dataset_path(dataset)
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
    """Select a v2 error's status, message, and errors[] entries, or a success's fields."""
    if response.status < 400:
        return {"http_status": response.status, "fields": sorted(response.json())}
    error = response.json().get("error", {})
    selected = {
        "http_status": response.status,
        "status": error.get("status"),
        "message": observe.text(error.get("message", "")),
        "errors": [
            {key: observe.value(entry.get(key)) for key in ("reason", "message") if key in entry}
            for entry in error.get("errors") or ()
        ],
    }
    check_observation(selected)
    return selected


def access_of(observe: Observations, response: Response) -> dict[str, Any]:
    """Select a dataset's access list as returned, in order."""
    return {"http_status": response.status, "access": observe.value(response.json().get("access"))}


def result(response: Response) -> dict[str, Any]:
    """Select a jobs.query result's status, schema, and rows."""
    value = response.json()
    return {"http_status": response.status, "schema": value.get("schema"), "rows": rows_of(value)}


def policy_of(observe: Observations, response: Response) -> dict[str, Any]:
    """Select a getIamPolicy answer: the refusal, or the bindings with sorted members."""
    if response.status >= 400:
        return bq_error(observe, response)
    bindings = [
        {"role": b.get("role"), "members": sorted(observe.value(b.get("members", [])))}
        for b in response.json().get("bindings", [])
    ]
    return {"http_status": response.status, "bindings": sorted(bindings, key=lambda b: b["role"])}


def build(fixture: AccessFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """A source table, two views over it, and the default access list."""
    for label, statement in (
        ("source-table", "CREATE TABLE {src}.t AS SELECT 'ada' AS name UNION ALL SELECT 'grace'"),
        ("view", "CREATE VIEW {views}.v AS SELECT name FROM {src}.t"),
        ("second-view", "CREATE VIEW {views}.v2 AS SELECT UPPER(name) AS name FROM {src}.t"),
    ):
        response = fixture.sql(statement)
        if response.status != 200:
            raise AssertionError(f"{label} returned HTTP {response.status}")
    snapshot.match("default-access", access_of(observe, fixture.on(fixture.dataset, "GET")))
    snapshot.match(
        "default-dataset-iam",
        policy_of(observe, fixture.on(fixture.dataset, "POST", ":getIamPolicy", body={})),
    )


def grant(fixture: AccessFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Grant the views dataset through a group and authorize one view on the source."""
    allowed = fixture.client.emails["allowed"]
    snapshot.match(
        "baseline-view-denied",
        bq_error(observe, fixture.sql("SELECT name FROM {views}.v", actor="allowed")),
    )
    snapshot.match(
        "group-entry-added",
        access_of(
            observe,
            fixture.add_access(
                fixture.views,
                {"role": "READER", "groupByEmail": GROUP},
                {"role": "roles/bigquery.dataEditor", "iamMember": "serviceAccount:" + allowed},
                {"role": "WRITER", "userByEmail": allowed},
            ),
        ),
    )
    view = {"projectId": fixture.project, "datasetId": fixture.views, "tableId": "v"}
    snapshot.match(
        "view-entry-added", access_of(observe, fixture.add_access(fixture.dataset, {"view": view}))
    )
    snapshot.match(
        "views-dataset-iam",
        policy_of(observe, fixture.on(fixture.views, "POST", ":getIamPolicy", body={})),
    )
    snapshot.match(
        "source-dataset-iam",
        policy_of(observe, fixture.on(fixture.dataset, "POST", ":getIamPolicy", body={})),
    )


def read(fixture: AccessFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """The authorized view reads the source; the source itself and the other view do not."""
    authorized = fixture.wait_until(
        lambda: fixture.sql("SELECT name FROM {views}.v ORDER BY name", actor="allowed"), {403}, 240
    )
    snapshot.match("authorized-view-read", result(authorized))
    snapshot.match(
        "unauthorized-view-denied",
        bq_error(observe, fixture.sql("SELECT name FROM {views}.v2", actor="allowed")),
    )
    snapshot.match(
        "source-query-denied",
        bq_error(observe, fixture.sql("SELECT name FROM {src}.t", actor="allowed")),
    )
    snapshot.match(
        "source-rows-denied",
        bq_error(observe, fixture.on(fixture.dataset, "GET", "/tables/t/data", actor="allowed")),
    )
    snapshot.match(
        "source-table-denied",
        bq_error(observe, fixture.on(fixture.dataset, "GET", "/tables/t", actor="allowed")),
    )
    entry = {
        "dataset": {
            "dataset": {"projectId": fixture.project, "datasetId": fixture.views},
            "targetTypes": ["VIEWS"],
        }
    }
    snapshot.match(
        "dataset-entry-added", access_of(observe, fixture.add_access(fixture.dataset, entry))
    )
    second = fixture.wait_until(
        lambda: fixture.sql("SELECT name FROM {views}.v2 ORDER BY name", actor="allowed"),
        {403},
        240,
    )
    snapshot.match("authorized-dataset-read", result(second))
    snapshot.match(
        "denied-identity-view",
        bq_error(observe, fixture.sql("SELECT name FROM {views}.v", actor="denied")),
    )
    snapshot.match(
        "denied-identity-dataset",
        bq_error(observe, fixture.on(fixture.views, "GET", actor="denied")),
    )


def refuse(fixture: AccessFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Access entries GCP refuses."""
    absent_view = {"projectId": fixture.project, "datasetId": fixture.views, "tableId": "absent"}
    for label, entry in (
        ("missing-view-entry", {"view": absent_view}),
        ("missing-group-entry", {"role": "READER", "groupByEmail": ABSENT_GROUP}),
        ("unknown-role-entry", {"role": "READ", "specialGroup": "projectReaders"}),
        ("roleless-member-entry", {"userByEmail": fixture.client.emails["denied"]}),
    ):
        snapshot.match(label, bq_error(observe, fixture.add_access(fixture.views, entry)))
    snapshot.match(
        "dataset-set-iam",
        bq_error(
            observe,
            fixture.on(fixture.views, "POST", ":setIamPolicy", body={"policy": {"bindings": []}}),
        ),
    )


def run_scenario(fixture: AccessFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.views: "<views>",
            fixture.dataset: "<dataset>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
            **{email: "<" + actor + ">" for actor, email in client.emails.items()},
        }
    )
    if created.status != 200:
        raise AssertionError(f"Dataset create returned HTTP {created.status}")
    fixture.owned(created)
    build(fixture, snapshot, observe)
    grant(fixture, snapshot, observe)
    read(fixture, snapshot, observe)
    refuse(fixture, snapshot, observe)


def local_job_user(target: Any, *, grant: bool) -> None:
    """Grant or revoke the allowed identity's project-level job user role locally.

    A live run has the operator add exactly this binding before the run and remove it after;
    the local run stands in for that through the emulator's project IAM.
    """
    client = RestClient(target, "https://cloudresourcemanager.googleapis.com")
    path = f"/v1/projects/{target.project}"
    policy = client.request("fixture", "POST", path + ":getIamPolicy", body={}).json()
    member = "serviceAccount:" + client.emails["allowed"]
    current = policy.get("bindings", [])
    bindings = [b for b in current if b.get("role") != JOB_USER]
    members = next((b["members"] for b in current if b.get("role") == JOB_USER), [])
    members = [m for m in members if m != member] + ([member] if grant else [])
    if members:
        bindings.append({"role": JOB_USER, "members": members})
    updated = client.request(
        "fixture",
        "POST",
        path + ":setIamPolicy",
        body={"policy": {"etag": policy.get("etag", ""), "bindings": bindings}},
    )
    if updated.status != 200:
        raise AssertionError(f"Local job user grant returned HTTP {updated.status}")


@pytest.fixture
def bigquery_access(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "bigquery-access-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = AccessFixture(
        BigQueryClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state
    )
    try:
        if gcp_target.mode == "local":
            local_job_user(gcp_target, grant=True)
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if gcp_target.mode == "local":
            local_job_user(gcp_target, grant=False)
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"datasets_absent": True})


@pytest.mark.gcp_scenario(
    id="bigquery-dataset-access",
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
def test_bigquery_access(bigquery_access, gcp_snapshot):
    fixture, created = bigquery_access
    run_scenario(fixture, gcp_snapshot, created)
