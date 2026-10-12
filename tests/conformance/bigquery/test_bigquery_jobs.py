"""Query jobs as google-cloud-bigquery 3.46.1 runs them: client.query().result().

Request shapes come from that client against the conformance server with
-trace: jobs.insert with a client-chosen job id and useLegacySql false, then
jobs.get polling with location, then jobs.getQueryResults with maxResults=0,
a first page, and a pageToken page that asks for fields. The fixture identity
holds only dataOwner and jobUser, so its own job reads exercise the creator
rule rather than bigquery.jobs.get.
"""

import time

import pytest

from gcp_validation.bigquery import LIMITS, ROWS, SCHEMA, rows_of
from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import Observations

PAGE_FIELDS = "jobReference,totalRows,pageToken,rows"


def insert(fixture, job_id, sql, *, actor="fixture"):
    """Send one jobs.insert, counted against the scope's query limit."""
    if fixture.manifest["queries"] >= LIMITS["max_queries"]:
        raise ValueError("Query limit reached")
    fixture.manifest["queries"] += 1
    fixture.save()
    return fixture.client.request(
        actor,
        "POST",
        f"{fixture.base}/jobs",
        timeout=fixture.remaining(),
        body={
            "jobReference": {"projectId": fixture.project, "jobId": job_id},
            "configuration": {"query": {"query": sql, "useLegacySql": False}},
        },
    )


def job_shape(observe, response, sql):
    """Select the job fields the Python client reads."""
    value = response.json()
    configuration = value.get("configuration", {})
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "job_reference_fields": sorted(value.get("jobReference", {})),
        "job_type": configuration.get("jobType"),
        "query_echoed": configuration.get("query", {}).get("query") == sql,
        "use_legacy_sql": configuration.get("query", {}).get("useLegacySql"),
        "state_known": value.get("status", {}).get("state") in {"PENDING", "RUNNING", "DONE"},
        "user_email": observe.value(value.get("user_email")),
    }


def page_shape(response):
    """Select one getQueryResults page."""
    value = response.json()
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "totalRows": value.get("totalRows"),
        "jobComplete": value.get("jobComplete"),
        "row_count": len(value.get("rows", [])),
        "has_page_token": "pageToken" in value,
        "schema": value.get("schema"),
    }


@pytest.mark.gcp_scenario(
    id="bigquery-jobs-insert-query",
    service="bigquery",
    transport="rest-v1",
    versions={
        "sdk": "google-cloud-bigquery/3.46.1 request shapes",
        "provider": "not-applicable",
        "backend": "not-applicable",
    },
    contract_files=(
        "gcp_validation/bigquery.py",
        "gcp_validation/live_rest.py",
        "gcp_validation/sandbox.py",
        "tests/conformance/bigquery/conftest.py",
    ),
)
def test_bigquery_jobs_insert_query(bigquery, gcp_snapshot):
    fixture, created = bigquery
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
    fixture.call(
        "POST",
        "/tables",
        body={
            "tableReference": {
                "projectId": fixture.project,
                "datasetId": fixture.dataset,
                "tableId": "t1",
            },
            "schema": SCHEMA,
        },
    ).json()
    fixture.call(
        "POST",
        "/tables/t1/insertAll",
        body={"kind": "bigquery#tableDataInsertAllRequest", "rows": ROWS},
    )
    deadline = time.monotonic() + (60 if client.sandbox else 0)
    while (
        len(rows_of(fixture.call("GET", "/tables/t1/data").json())) < len(ROWS)
        and time.monotonic() < deadline
    ):
        time.sleep(5)  # Streamed rows can take a moment to become readable.

    sql = f"SELECT name FROM `{fixture.project}.{fixture.dataset}.t1`"  # noqa: S608 -- owned names
    job_id = fixture.dataset + "_q1"
    inserted = insert(fixture, job_id, sql)
    gcp_snapshot.match("job-inserted", job_shape(observe, inserted, sql))
    jobs = f"{fixture.base}/jobs/{job_id}"
    deadline = time.monotonic() + (60 if client.sandbox else 0)
    while True:
        polled = client.request(
            "fixture", "GET", jobs, query={"location": "US"}, timeout=fixture.remaining()
        )
        if polled.json().get("status", {}).get("state") == "DONE" or time.monotonic() >= deadline:
            break
        time.sleep(2)
    status = polled.json().get("status", {})
    if status.get("state") != "DONE":
        raise Inconclusive("Query job did not finish within the deadline")
    gcp_snapshot.match(
        "job-done",
        {
            **job_shape(observe, polled, sql),
            "state": status.get("state"),
            "has_error": "errorResult" in status,
            "statement_type": polled.json()
            .get("statistics", {})
            .get("query", {})
            .get("statementType"),
        },
    )

    results = f"{fixture.base}/queries/{job_id}"
    waited = client.request(
        "fixture",
        "GET",
        results,
        query={"maxResults": 0, "location": "US"},
        timeout=fixture.remaining(),
    )
    gcp_snapshot.match("results-wait", page_shape(waited))
    first = client.request(
        "fixture",
        "GET",
        results,
        timeout=fixture.remaining(),
        query={"maxResults": 1, "location": "US", "formatOptions.useInt64Timestamp": "true"},
    )
    gcp_snapshot.match("results-first-page", page_shape(first))
    token = first.json().get("pageToken")
    if not token:
        raise AssertionError("The first one-row page of a two-row result carried no pageToken")
    second = client.request(
        "fixture",
        "GET",
        results,
        timeout=fixture.remaining(),
        query={
            "fields": PAGE_FIELDS,
            "maxResults": 1,
            "location": "US",
            "pageToken": token,
            "formatOptions.useInt64Timestamp": "true",
        },
    )
    gcp_snapshot.match("results-second-page", page_shape(second))
    # Row order is unspecified without ORDER BY, so the pages are compared as a set.
    names = sorted(rows_of(first.json()) + rows_of(second.json()))
    gcp_snapshot.match("results-rows", {"rows": names})

    gcp_snapshot.match("duplicate-job-id", observe.error(insert(fixture, job_id, sql)))
    gcp_snapshot.match(
        "denied-job-get",
        observe.error(
            client.request(
                "denied", "GET", jobs, query={"location": "US"}, timeout=fixture.remaining()
            )
        ),
    )
    gcp_snapshot.match(
        "denied-results",
        observe.error(
            client.request(
                "denied",
                "GET",
                results,
                query={"maxResults": 0, "location": "US"},
                timeout=fixture.remaining(),
            )
        ),
    )
    gcp_snapshot.match(
        "denied-insert",
        observe.error(insert(fixture, fixture.dataset + "_q2", sql, actor="denied")),
    )
    missing = client.request(
        "fixture",
        "GET",
        f"{fixture.base}/jobs/{fixture.dataset}_q2",
        query={"location": "US"},
        timeout=fixture.remaining(),
    )
    gcp_snapshot.match("denied-job-absent", observe.error(missing))
