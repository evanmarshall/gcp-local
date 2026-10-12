"""Bounded Firestore query and transaction flow in a run-owned collection."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from gcp_validation.config import Target
from gcp_validation.evidence import Inconclusive, SnapshotCase, now
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response, RestClient
from gcp_validation.sandbox import Sandbox

SERVICE = "firestore"
LIMITS = {"max_documents": 4, "max_collections": 2, "max_queries": 6, "max_run_seconds": 600}
LIVE_ENDPOINT = "https://firestore.googleapis.com"
CITIES = {"sf": ("CA", 870000), "la": ("CA", 3900000), "nyc": ("NY", 8300000)}
NESTED = "sf/landmarks/ggb"


def stream_error(observe: Observations, response: Response) -> dict[str, Any]:
    """Observe an error from a streamed REST method, which Firestore wraps in a JSON array."""
    value = response.json()
    streamed = isinstance(value, list)
    if streamed:
        value = value[0] if value else {}
    selected = observe.error(Response(response.status, json.dumps(value).encode()))
    selected["streamed"] = streamed
    return selected


def load_scope(filename: str, sandbox: Sandbox) -> LiveScope:
    """Load the separately approved live scope for this recorder."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox)


def city(state: str, population: int) -> dict:
    """Render one city's Firestore fields."""
    return {"state": {"stringValue": state}, "population": {"integerValue": str(population)}}


def created_write(name: str, fields: dict) -> dict:
    """Render a create-only write."""
    return {"update": {"name": name, "fields": fields}, "currentDocument": {"exists": False}}


class QueryFixture(OwnedRun):
    """Documents live only under the run's own collection; cleanup deletes only those."""

    def __init__(self, client: RestClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the run's collection and database paths."""
        super().__init__(
            client,
            state_dir,
            SERVICE + "-query",
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: v.get("collection") == "glq-" + v["run_id"],
        )
        self.manifest.setdefault("collection", "glq-" + self.manifest["run_id"])
        self.collection = self.manifest["collection"]
        self.database = f"projects/{self.project}/databases/(default)"
        self.root = f"/v1/{self.database}/documents"

    def name(self, path: str) -> str:
        """Name a document under the run collection."""
        return f"{self.database}/documents/{self.collection}/{path}"

    def owned(self) -> list[str]:
        """List every document the run may create."""
        return [self.name(key) for key in CITIES] + [self.name(NESTED)]

    def call(
        self,
        method: str,
        suffix: str,
        *,
        actor: str = "fixture",
        body: dict | None = None,
        query: dict | None = None,
    ) -> Response:
        """Send one request under the documents root."""
        return self.client.request(
            actor, method, self.root + suffix, body=body, query=query, timeout=self.remaining()
        )

    def setup(self) -> Response:
        """Create the run's documents in one commit after confirming the collection is empty."""
        self.acquire()
        self.client.verify_project()
        listing = self.call("GET", "/" + self.collection, query={"pageSize": 1})
        if listing.status != 200:
            raise Inconclusive(f"Collection listing returned HTTP {listing.status}")
        if listing.json().get("documents"):
            raise Inconclusive("The run collection unexpectedly holds documents")
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        writes = [created_write(self.name(key), city(*value)) for key, value in CITIES.items()]
        writes.append(created_write(self.name(NESTED), {"type": {"stringValue": "bridge"}}))
        return self.call("POST", ":commit", body={"writes": writes})

    def cleanup(self) -> None:
        """Delete only the run's documents and confirm none remain."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            names = self.owned()
            if any(f"/documents/{self.collection}/" not in name for name in names):
                raise ValueError("Refusing Firestore cleanup outside the run collection")
            deleted = self.client.request(
                "fixture",
                "POST",
                self.root + ":commit",
                body={"writes": [{"delete": name} for name in names]},
            )
            if deleted.status != 200:
                raise ValueError(f"Owned document delete returned HTTP {deleted.status}")
            remaining = self.client.request(
                "fixture", "POST", self.root + ":batchGet", body={"documents": names}
            )
            if remaining.status != 200 or any("found" in item for item in remaining.json()):
                raise ValueError("Owned documents remain after deletion")
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()


def names(observe: Observations, response: Response, key: str) -> dict:
    """Select the status and aliased document names from a streamed response."""
    found = [observe.text(item[key]["name"]) for item in response.json() if key in item]
    return {"http_status": response.status, "names": found}


def check_queries(fixture: QueryFixture, snapshot: SnapshotCase, observe: Observations) -> dict:
    """Observe a filtered query, a count, a subcollection query, and collection IDs.

    The query filters on one field only: an equality filter ordered by another
    field needs a composite index in Firestore, which the emulator does not
    enforce.
    """
    query = {
        "structuredQuery": {
            "from": [{"collectionId": fixture.collection}],
            "where": {
                "fieldFilter": {
                    "field": {"fieldPath": "state"},
                    "op": "EQUAL",
                    "value": {"stringValue": "CA"},
                }
            },
            "limit": 2,
        }
    }
    snapshot.match(
        "query", names(observe, fixture.call("POST", ":runQuery", body=query), "document")
    )
    count = fixture.call(
        "POST",
        ":runAggregationQuery",
        body={
            "structuredAggregationQuery": {
                "structuredQuery": {"from": [{"collectionId": fixture.collection}]},
                "aggregations": [{"alias": "n", "count": {}}],
            }
        },
    )
    counts = [item["result"]["aggregateFields"]["n"] for item in count.json() if "result" in item]
    snapshot.match("count", {"http_status": count.status, "counts": counts})
    nested = fixture.call(
        "POST",
        f"/{fixture.collection}/sf:runQuery",
        body={"structuredQuery": {"from": [{"collectionId": "landmarks"}]}},
    )
    snapshot.match("nested-query", names(observe, nested, "document"))
    collections = fixture.call("POST", f"/{fixture.collection}/sf:listCollectionIds", body={})
    snapshot.match("collection-ids", observe.select(collections, ("collectionIds",)))
    return query


def check_transactions(
    fixture: QueryFixture, snapshot: SnapshotCase, observe: Observations
) -> None:
    """Observe a transaction read and commit, then a rollback."""
    begun = fixture.call("POST", ":beginTransaction", body={})
    snapshot.match("begin", {"http_status": begun.status, "fields": sorted(begun.json())})
    transaction = begun.json().get("transaction")
    read = fixture.call(
        "POST", ":batchGet", body={"documents": [fixture.name("sf")], "transaction": transaction}
    )
    snapshot.match("transaction-read", names(observe, read, "found"))
    write = {
        "update": {"name": fixture.name("sf"), "fields": city("CA", 1)},
        "currentDocument": {"exists": True},
    }
    committed = fixture.call(
        "POST", ":commit", body={"transaction": transaction, "writes": [write]}
    )
    snapshot.match(
        "transaction-commit",
        {
            "http_status": committed.status,
            "fields": sorted(committed.json()),
            "write_results": len(committed.json().get("writeResults", [])),
        },
    )
    after = fixture.call("GET", f"/{fixture.collection}/sf")
    snapshot.match(
        "after-commit",
        {
            "http_status": after.status,
            "population": after.json().get("fields", {}).get("population"),
        },
    )
    discarded = fixture.call("POST", ":beginTransaction", body={})
    rolled = fixture.call(
        "POST", ":rollback", body={"transaction": discarded.json().get("transaction")}
    )
    snapshot.match("rollback", {"http_status": rolled.status, "body": rolled.json()})


def run_scenario(fixture: QueryFixture, snapshot: SnapshotCase, created: Response) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.collection: "<collection>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
        }
    )
    snapshot.match(
        "created",
        {
            "http_status": created.status,
            "write_results": len(created.json().get("writeResults", [])),
        },
    )
    query = check_queries(fixture, snapshot, observe)
    check_transactions(fixture, snapshot, observe)
    denied = fixture.call("POST", ":runQuery", actor="denied", body=query)
    snapshot.match("denied-query", stream_error(observe, denied))
    denied = fixture.call(
        "POST", ":commit", actor="denied", body={"writes": [{"delete": fixture.name("nyc")}]}
    )
    snapshot.match("denied-commit", observe.error(denied))


def main(argv: list[str] | None = None) -> None:
    """Delete only the run documents after an interrupted run."""
    parser = argparse.ArgumentParser(
        description="Delete only the run documents after an interrupted run"
    )
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox)
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RestClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    QueryFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned Firestore query cleanup passed.\n")


if __name__ == "__main__":
    main()
