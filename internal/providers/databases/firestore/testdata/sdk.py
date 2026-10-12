"""Official Python Firestore client with local ADC over the gRPC gateway, in loopback isolation."""

import importlib.metadata
import ipaddress
import os
import socket
import sys
import threading
from urllib.parse import urlsplit

import google.auth
import grpc
import requests
from google.api_core.exceptions import Conflict, PermissionDenied
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.services.firestore import FirestoreClient
from google.cloud.firestore_v1.services.firestore.transports.grpc import FirestoreGrpcTransport

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
CITIES = {
    "sf": {"name": "San Francisco", "state": "CA", "population": 870000},
    "la": {"name": "Los Angeles", "state": "CA", "population": 3900000},
    "sac": {"name": "Sacramento", "state": "CA", "population": 520000},
    "nyc": {"name": "New York", "state": "NY", "population": 8300000},
}


class LocalRequest(Request):
    """Credential refresh that may only reach a loopback token endpoint."""

    def __call__(self, url, *args, **kwargs):
        parsed = urlsplit(url)
        if parsed.scheme != "http" or not ipaddress.ip_address(parsed.hostname).is_loopback:
            raise AssertionError("SDK tried a nonlocal credential request")
        kwargs["timeout"] = 5
        kwargs["allow_redirects"] = False
        return super().__call__(url, *args, **kwargs)


def local_client(credentials, project, request, endpoint):
    """Build firestore.Client on an authenticated local gRPC channel.

    The library has no public channel argument, so the GAPIC client is
    attached through its private _firestore_api_internal attribute.
    """
    channel = secure_authorized_channel(
        credentials, request, endpoint, ssl_credentials=grpc.local_channel_credentials()
    )
    client = firestore.Client(project=project, credentials=credentials)
    client._firestore_api_internal = FirestoreClient(  # noqa: SLF001 -- no public channel hook
        transport=FirestoreGrpcTransport(channel=channel)
    )
    return client


def check_queries(db, cities):
    """Filters, ordering, limits, aggregation, and batch reads."""
    names = [
        snapshot.id
        for snapshot in cities.where(filter=FieldFilter("state", "==", "CA"))
        .order_by("population", direction=firestore.Query.DESCENDING)
        .limit(2)
        .stream(timeout=10)
    ]
    assert names == ["la", "sf"], names
    big = [
        s.id
        for s in cities.where(filter=FieldFilter("population", ">", 1000000))
        .order_by("population")
        .stream(timeout=10)
    ]
    assert big == ["la", "nyc"], big
    count = cities.where(filter=FieldFilter("state", "==", "CA")).count().get(timeout=10)
    assert count[0][0].value == 3, count
    found = {
        s.id: s.exists
        for s in db.get_all([cities.document("sf"), cities.document("missing")], timeout=10)
    }
    assert found == {"sf": True, "missing": False}, found


def check_transactions(db, cities):
    """A read-modify-write transaction commits; a failing one rolls back."""

    @firestore.transactional
    def move(transaction, source, target, amount):
        source_population = source.get(transaction=transaction).get("population")
        target_population = target.get(transaction=transaction).get("population")
        transaction.update(source, {"population": source_population - amount})
        transaction.update(target, {"population": target_population + amount})

    move(db.transaction(), cities.document("la"), cities.document("sac"), 1000)
    assert cities.document("la").get(timeout=10).get("population") == 3899000
    assert cities.document("sac").get(timeout=10).get("population") == 521000

    @firestore.transactional
    def fail(transaction, document):
        document.get(transaction=transaction)
        transaction.update(document, {"population": 0})
        raise RuntimeError("abort")

    try:
        fail(db.transaction(), cities.document("sf"))
        raise AssertionError("failing transaction committed")
    except RuntimeError:
        pass
    assert cities.document("sf").get(timeout=10).get("population") == 870000


def check_subcollections(db, cities):
    """Nested documents, collection listing, and collection-group queries."""
    landmarks = cities.document("sf").collection("landmarks")
    landmarks.document("ggb").set({"name": "Golden Gate Bridge", "type": "bridge"}, timeout=10)
    cities.document("la").collection("landmarks").document("griffith").set(
        {"name": "Griffith Park", "type": "park"}, timeout=10
    )
    assert landmarks.document("ggb").get(timeout=10).get("type") == "bridge"
    assert [c.id for c in cities.document("sf").collections(timeout=10)] == ["landmarks"]
    bridges = [
        s.reference.path
        for s in db.collection_group("landmarks")
        .where(filter=FieldFilter("type", "==", "bridge"))
        .stream(timeout=10)
    ]
    assert bridges == [f"{cities.id}/sf/landmarks/ggb"], bridges
    try:
        landmarks.document("ggb").create({"name": "duplicate"}, timeout=10)
        raise AssertionError("duplicate create succeeded")
    except Conflict:
        pass


def check_listen(cities):
    """A document listener receives the current snapshot."""
    seen = threading.Event()

    def on_snapshot(snapshots, _changes, _read_time):
        if snapshots and snapshots[0].exists:
            seen.set()

    watch = cities.document("nyc").on_snapshot(on_snapshot)
    try:
        assert seen.wait(15), "listener received no snapshot"
    finally:
        watch.unsubscribe()


def check_denied(credentials_file, request, endpoint, cities):
    """Another project's principal is refused for reads and writes."""
    denied, _ = google.auth.load_credentials_from_file(
        credentials_file, scopes=SCOPES, request=request
    )
    other = local_client(denied, os.environ["GOOGLE_CLOUD_PROJECT"], request, endpoint)
    collection = other.collection(cities.id)
    batch = other.batch()
    batch.delete(collection.document("sf"))
    for attempt in (
        lambda: list(collection.where(filter=FieldFilter("state", "==", "CA")).stream(timeout=10)),
        lambda: collection.document("sf").set({"name": "no"}, timeout=10),
        lambda: batch.commit(timeout=10),
    ):
        try:
            attempt()
            raise AssertionError("denied principal succeeded")
        except PermissionDenied:
            pass


def main():
    assert os.environ.get("GCP_LOCAL_ISOLATED") == "1"
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    assert "FIRESTORE_EMULATOR_HOST" not in os.environ
    endpoint = sys.argv[1]
    assert ipaddress.ip_address(endpoint.rsplit(":", 1)[0]).is_loopback
    session = requests.Session()
    session.trust_env = False
    request = LocalRequest(session=session)
    credentials, project = google.auth.default(scopes=SCOPES, request=request)
    assert project == os.environ["GOOGLE_CLOUD_PROJECT"]
    db = local_client(credentials, project, request, endpoint)
    cities = db.collection("sdk-cities")
    try:
        batch = db.batch()
        for city_id, fields in CITIES.items():
            batch.set(cities.document(city_id), fields)
        batch.commit(timeout=10)
        # BulkWriter sends BatchWrite; recursive_delete below removes it.
        writer = db.bulk_writer()
        writer.create(cities.document("bulk"), {"name": "bulk"})
        writer.close()
        assert cities.document("bulk").get(timeout=10).exists
        check_queries(db, cities)
        check_transactions(db, cities)
        check_subcollections(db, cities)
        check_listen(cities)
        check_denied(os.environ["GCP_LOCAL_DENIED_ADC"], request, endpoint, cities)
    finally:
        db.recursive_delete(cities)
    assert list(cities.limit(1).stream(timeout=10)) == []
    print(
        "google-cloud-firestore",
        importlib.metadata.version("google-cloud-firestore"),
        "grpcio",
        importlib.metadata.version("grpcio"),
    )


if __name__ == "__main__":
    main()
