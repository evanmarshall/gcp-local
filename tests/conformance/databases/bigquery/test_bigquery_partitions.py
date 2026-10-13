"""Bounded BigQuery partitioning recording: time-unit, ingestion-time, and integer-range
partitioning, clustering, requirePartitionFilter, the pseudo-columns, and table, partition,
and dataset default expiration.

The fixture identity holds roles/bigquery.admin on the project. The run owns one dataset;
every table holds at most three rows written by DML, so no query reads more than a few bytes.
"""

import time
from pathlib import Path
from typing import Any

import pytest

from gcp_validation.evidence import SnapshotCase, check_observation
from gcp_validation.live_rest import LiveScope, Observations, Response
from gcp_validation.services.databases.bigquery import (
    LIVE_ENDPOINT,
    BigQueryClient,
    DatasetFixture,
    rows_of,
)

SERVICE = "bigquery"
LIMITS = {
    "max_datasets": 1,
    "max_tables": 14,
    "max_rows": 6,
    "max_queries": 16,
    "max_run_seconds": 900,
}
DAY_MS = 86_400_000
FIELDS = [
    {"name": "ts", "type": "TIMESTAMP"},
    {"name": "d", "type": "DATE"},
    {"name": "name", "type": "STRING"},
    {"name": "n", "type": "INTEGER"},
]
# A timestamp before every row; the date-only literal form reads in the engine's
# default zone (America/Los_Angeles), not UTC, so the scenario spells the time.
SINCE = "TIMESTAMP '2000-01-01 00:00:00'"
SELECTED = ("timePartitioning", "rangePartitioning", "clustering", "requirePartitionFilter")


def load_scope(filename: str, sandbox: Any, mode: str) -> LiveScope:
    """Load the approved scope file for this scenario's limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class PartitionFixture(DatasetFixture):
    """The owned dataset, with table creation and queries counted against this scope."""

    def table(self, table: str, **fields: Any) -> Response:
        """Send one tables.insert with the shared schema unless another is given."""
        body = {
            "tableReference": {
                "projectId": self.project,
                "datasetId": self.dataset,
                "tableId": table,
            },
            "schema": {"fields": FIELDS},
            **fields,
        }
        return self.call("POST", "/tables", body=body)

    def sql(self, statement: str) -> Response:
        """Send one jobs.query; statements name the owned dataset as {ds} and SINCE as {since}."""
        if self.manifest["queries"] >= LIMITS["max_queries"]:
            raise ValueError("Query limit reached")
        self.manifest["queries"] += 1
        self.save()
        body = {
            "query": statement.replace("{ds}", self.dataset).replace("{since}", SINCE),
            "useLegacySql": False,
            "location": "US",
            "timeoutMs": 30000,
        }
        return self.client.request(
            "fixture", "POST", f"{self.base}/queries", timeout=self.remaining(), body=body
        )


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


def result(response: Response) -> dict[str, Any]:
    """Select a jobs.query result's status, schema, rows, and DML count."""
    value = response.json()
    return {
        "http_status": response.status,
        "statementType": value.get("statementType"),
        "numDmlAffectedRows": value.get("numDmlAffectedRows"),
        "schema": value.get("schema"),
        "rows": rows_of(value),
    }


def create_tables(fixture: PartitionFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Create and read each partitioning kind; refuse the invalid ones."""
    events = fixture.table(
        "events",
        timePartitioning={"type": "DAY", "field": "ts", "expirationMs": str(DAY_MS)},
        requirePartitionFilter=True,
        clustering={"fields": ["name", "n"]},
    )
    snapshot.match("time-partitioned-created", observe.select(events, SELECTED))
    snapshot.match(
        "time-partitioned-read", observe.select(fixture.call("GET", "/tables/events"), SELECTED)
    )
    ingest = fixture.table(
        "ingest", schema={"fields": FIELDS[2:]}, timePartitioning={"type": "DAY"}
    )
    snapshot.match("ingestion-created", observe.select(ingest, SELECTED))
    units = {}
    for table, partitioning in (
        ("hourly", {"type": "HOUR", "field": "ts"}),
        ("monthly", {"type": "MONTH", "field": "d"}),
        ("yearly", {"type": "YEAR"}),
    ):
        units[table] = observe.select(fixture.table(table, timePartitioning=partitioning), SELECTED)
    snapshot.match("time-units-created", units)
    ranged = fixture.table(
        "ranged",
        rangePartitioning={"field": "n", "range": {"start": "0", "end": "100", "interval": "10"}},
        clustering={"fields": ["d"]},
    )
    snapshot.match("range-created", observe.select(ranged, SELECTED))
    refusals = {
        "clustering-too-many": {
            "schema": {"fields": [*FIELDS, {"name": "e", "type": "STRING"}]},
            "clustering": {"fields": ["ts", "d", "name", "n", "e"]},
        },
        "clustering-repeated": {"clustering": {"fields": ["n", "n"]}},
        "clustering-field-missing": {"clustering": {"fields": ["absent"]}},
        "partition-field-missing": {"timePartitioning": {"type": "DAY", "field": "absent"}},
        "partition-field-type": {"timePartitioning": {"type": "DAY", "field": "name"}},
        "partition-type-unknown": {"timePartitioning": {"type": "WEEK"}},
        "range-field-type": {
            "rangePartitioning": {
                "field": "ts",
                "range": {"start": "0", "end": "10", "interval": "1"},
            }
        },
        "range-interval-zero": {
            "rangePartitioning": {
                "field": "n",
                "range": {"start": "0", "end": "10", "interval": "0"},
            }
        },
        "both-partitionings": {
            "timePartitioning": {"type": "DAY", "field": "ts"},
            "rangePartitioning": {
                "field": "n",
                "range": {"start": "0", "end": "10", "interval": "1"},
            },
        },
    }
    for label, fields in refusals.items():
        snapshot.match(
            label, bq_error(observe, fixture.table("r_" + label.replace("-", "_"), **fields))
        )
    listed = fixture.call("GET", "/tables").json().get("tables", [])
    items = {item["tableReference"]["tableId"]: item for item in listed}
    snapshot.match(
        "tables-list-items",
        {
            table: observe.value(
                {
                    "fields": sorted(items.get(table, {})),
                    **{k: items.get(table, {}).get(k) for k in SELECTED},
                }
            )
            for table in ("events", "ingest", "ranged")
        },
    )


def query_partitions(
    fixture: PartitionFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """requirePartitionFilter, partition expiration on read, and the pseudo-columns."""
    snapshot.match(
        "require-filter-refused", bq_error(observe, fixture.sql("SELECT name FROM {ds}.events"))
    )
    snapshot.match(
        "require-filter-delete-refused",
        bq_error(observe, fixture.sql("DELETE FROM {ds}.events WHERE name = 'x'")),
    )
    snapshot.match(
        "insert-into-expiring-partitions",
        result(
            fixture.sql(
                "INSERT INTO {ds}.events (ts, name, n) VALUES"
                " (CURRENT_TIMESTAMP(), 'new', 1), (TIMESTAMP '2001-01-01 00:00:00', 'old', 2)"
            )
        ),
    )
    snapshot.match(
        "expired-partition-unread",
        result(fixture.sql("SELECT name FROM {ds}.events WHERE ts > {since} ORDER BY name")),
    )
    snapshot.match(
        "ingestion-insert",
        result(
            fixture.sql(
                "INSERT INTO {ds}.ingest (name, n) VALUES ('now', 1);"
                " INSERT INTO {ds}.ingest (_PARTITIONTIME, name, n)"
                " VALUES (TIMESTAMP '2020-01-01 00:00:00', 'past', 2)"
            )
        ),
    )
    snapshot.match(
        "pseudo-columns",
        result(
            fixture.sql(
                "SELECT name, _PARTITIONTIME AS pt, _PARTITIONDATE AS pd,"
                " _PARTITIONTIME = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS current_partition,"
                " _PARTITIONDATE = CURRENT_DATE() AS today FROM {ds}.ingest WHERE name = 'past'"
                " UNION ALL SELECT name, NULL, NULL,"
                " _PARTITIONTIME = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY),"
                " _PARTITIONDATE = CURRENT_DATE() FROM {ds}.ingest WHERE name = 'now' ORDER BY name"
            )
        ),
    )
    snapshot.match(
        "star-excludes-pseudo-columns",
        result(fixture.sql("SELECT * FROM {ds}.ingest ORDER BY name")),
    )
    snapshot.match(
        "pseudo-column-on-column-partitioned",
        bq_error(
            observe,
            fixture.sql("SELECT _PARTITIONTIME FROM {ds}.hourly WHERE ts > {since}"),
        ),
    )


def patch_tables(fixture: PartitionFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Patch clustering, the filter requirement, and partition expiration; refuse a type change."""
    for label, body in (
        ("clustering-patched", {"clustering": {"fields": ["n"]}}),
        ("require-filter-patched", {"requirePartitionFilter": False}),
        (
            "partition-expiration-patched",
            {"timePartitioning": {"type": "DAY", "field": "ts", "expirationMs": str(2 * DAY_MS)}},
        ),
    ):
        snapshot.match(
            label, observe.select(fixture.call("PATCH", "/tables/events", body=body), SELECTED)
        )
    snapshot.match(
        "partition-type-change",
        bq_error(
            observe,
            fixture.call(
                "PATCH",
                "/tables/events",
                body={"timePartitioning": {"type": "MONTH", "field": "ts"}},
            ),
        ),
    )
    snapshot.match(
        "require-filter-off",
        result(fixture.sql("SELECT name FROM {ds}.events ORDER BY name")),
    )


def expiration(fixture: PartitionFixture, snapshot: SnapshotCase, observe: Observations) -> None:
    """Table expirationTime and the dataset defaults that new tables inherit."""
    now_ms = int(time.time() * 1000)
    sent = str(now_ms + 2 * DAY_MS)
    expiring = fixture.table("expiring", expirationTime=sent)
    snapshot.match(
        "expiration-set",
        {"http_status": expiring.status, "as_sent": expiring.json().get("expirationTime") == sent},
    )
    later = str(now_ms + 3 * DAY_MS)
    patched = fixture.call("PATCH", "/tables/expiring", body={"expirationTime": later})
    snapshot.match(
        "expiration-patched",
        {"http_status": patched.status, "as_sent": patched.json().get("expirationTime") == later},
    )
    cleared = fixture.call("PATCH", "/tables/expiring", body={"expirationTime": None})
    snapshot.match("expiration-cleared", observe.select(cleared, ("expirationTime",)))
    snapshot.match(
        "expiration-past",
        bq_error(observe, fixture.table("past", expirationTime=str(now_ms - DAY_MS))),
    )
    defaults = {
        "defaultTableExpirationMs": str(DAY_MS),
        "defaultPartitionExpirationMs": str(2 * DAY_MS),
    }
    snapshot.match(
        "dataset-defaults-set",
        observe.select(fixture.call("PATCH", body=defaults), tuple(defaults)),
    )
    inserted = fixture.table("inherits", timePartitioning={"type": "DAY", "field": "ts"}).json()
    read = fixture.call("GET", "/tables/inherits").json()
    plain = fixture.table("plain").json()
    snapshot.match(
        "dataset-defaults-applied",
        {
            label: {
                "expiration_after_creation_ms": int(value["expirationTime"])
                - int(value["creationTime"])
                if "expirationTime" in value
                else None,
                "timePartitioning": value.get("timePartitioning"),
            }
            for label, value in (("inserted", inserted), ("read", read), ("unpartitioned", plain))
        },
    )
    snapshot.match(
        "dataset-default-too-short",
        bq_error(observe, fixture.call("PATCH", body={"defaultTableExpirationMs": "1000"})),
    )
    snapshot.match(
        "dataset-defaults-cleared",
        observe.select(fixture.call("PATCH", body=dict.fromkeys(defaults)), tuple(defaults)),
    )


def run_scenario(fixture: PartitionFixture, snapshot: SnapshotCase, created: Response) -> None:
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
        raise AssertionError(f"Dataset create returned HTTP {created.status}")
    fixture.owned(created)
    create_tables(fixture, snapshot, observe)
    query_partitions(fixture, snapshot, observe)
    patch_tables(fixture, snapshot, observe)
    expiration(fixture, snapshot, observe)


@pytest.fixture
def bigquery_partitions(gcp_snapshot, gcp_target, pytestconfig):
    sandbox, scope = pytestconfig.gcp_sandbox, None
    if gcp_target.mode == "local":
        state = pytestconfig.gcp_artifacts / "bigquery-partitions-resources"
    else:
        scope = load_scope(pytestconfig.getoption("gcp_live_scope_file"), sandbox, gcp_target.mode)
        state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
        gcp_snapshot.context["scope"] = scope.evidence()
    fixture = PartitionFixture(
        BigQueryClient(gcp_target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope), state
    )
    try:
        created = fixture.setup()
        yield fixture, created
    finally:
        fixture.cleanup()
        if fixture.manifest["cleanup"] == "passed":
            gcp_snapshot.match("cleanup", {"dataset_absent": True})


@pytest.mark.gcp_scenario(
    id="bigquery-partitioning-expiration",
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
def test_bigquery_partitions(bigquery_partitions, gcp_snapshot):
    fixture, created = bigquery_partitions
    run_scenario(fixture, gcp_snapshot, created)
