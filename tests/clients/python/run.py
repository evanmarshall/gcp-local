"""Drive one service workflow with the pinned Python Google Cloud clients against gcp-local.

scripts/run_clients.py starts the emulator and runs `run.py SERVICE` once per
service, at each client's default transport. The process exits 0 when the
workflow passes and prints the failing step otherwise.
"""

import datetime as dt
import os
import signal
import sys
import time

import google.auth
import grpc
import requests
from google.api_core import exceptions
from google.api_core.client_options import ClientOptions
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import (
    bigquery,
    bigtable,
    firestore,
    iam_admin_v1,
    kms,
    pubsub_v1,
    secretmanager,
    spanner,
    storage,
    tasks_v2,
)
from google.cloud import logging as cloud_logging
from google.cloud.bigtable import column_family, enums
from google.cloud.firestore_v1.services.firestore import FirestoreClient

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
HTTP = os.environ["GCP_LOCAL_HTTP"]
GRPC = os.environ["GCP_LOCAL_GRPC"]
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PAYLOAD = b"hello from python"


def credentials(*, denied=False):
    """Load local ADC (the granted identity) or the ungranted identity's ADC file.

    Both request cloud-platform: the local token service refuses most client
    libraries' other default scopes (#52).
    """
    if denied:
        loaded, _ = google.auth.load_credentials_from_file(
            os.environ["GCP_LOCAL_DENIED_ADC"], scopes=SCOPES
        )
        return loaded
    return google.auth.default(scopes=SCOPES)[0]


def channel(*, denied=False):
    """Open a plaintext loopback gRPC channel that still carries ADC.

    `local_channel_credentials` lets gRPC attach call credentials without TLS.
    Only generated clients take a channel; handwritten wrappers do not.
    """
    session = requests.Session()
    session.trust_env = False
    return secure_authorized_channel(
        credentials(denied=denied),
        Request(session=session),
        GRPC,
        ssl_credentials=grpc.local_channel_credentials(),
    )


def generated(client_class, *, denied=False):
    """Build a generated (GAPIC) client on the gRPC gateway."""
    return client_class(
        transport=client_class.get_transport_class("grpc")(channel=channel(denied=denied))
    )


def expect_denied(step, call):
    """Require the ungranted identity's call to be refused with permission denied."""
    try:
        call()
    except exceptions.Forbidden:
        return
    except exceptions.GoogleAPICallError as error:
        raise AssertionError(f"{step}: want permission denied, got {error!r}") from error
    raise AssertionError(f"{step}: the ungranted identity was allowed")


def run_storage():
    client = storage.Client(
        project=PROJECT, credentials=credentials(), client_options={"api_endpoint": HTTP}
    )
    bucket = client.bucket(f"python-matrix-{PROJECT}")
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"
    bucket.soft_delete_policy.retention_duration_seconds = 0
    bucket = client.create_bucket(bucket, location="us-central1")
    blob = bucket.blob("hello.txt")
    blob.upload_from_string(PAYLOAD)
    assert blob.download_as_bytes() == PAYLOAD, "download"
    assert [b.name for b in client.list_blobs(bucket)] == ["hello.txt"], "list objects"
    other = storage.Client(
        project=PROJECT, credentials=credentials(denied=True), client_options={"api_endpoint": HTTP}
    )
    expect_denied("ungranted object get", other.bucket(bucket.name).blob("hello.txt").reload)
    blob.delete()
    bucket.delete()


def run_bigquery():
    client = bigquery.Client(
        project=PROJECT, credentials=credentials(), client_options={"api_endpoint": HTTP}
    )
    dataset = bigquery.Dataset(f"{PROJECT}.python_matrix")
    dataset.location = "US"
    client.create_dataset(dataset)
    schema = [bigquery.SchemaField("name", "STRING"), bigquery.SchemaField("age", "INTEGER")]
    table = client.create_table(bigquery.Table(f"{PROJECT}.python_matrix.people", schema=schema))
    errors = client.insert_rows_json(
        table, [{"name": "ada", "age": 36}, {"name": "grace", "age": 45}]
    )
    assert not errors, errors
    query = f"SELECT name, age FROM `{PROJECT}.python_matrix.people` WHERE name = 'ada'"  # noqa: S608 -- fixed synthetic project
    rows = [dict(row) for row in client.query(query).result()]
    assert rows == [{"name": "ada", "age": 36}], rows
    assert [t.table_id for t in client.list_tables("python_matrix")] == ["people"], "list tables"
    other = bigquery.Client(
        project=PROJECT, credentials=credentials(denied=True), client_options={"api_endpoint": HTTP}
    )
    expect_denied("ungranted table get", lambda: other.get_table(table.reference))
    client.delete_dataset("python_matrix", delete_contents=True)


def run_pubsub():
    publisher = pubsub_v1.PublisherClient(
        transport=pubsub_v1.PublisherClient.get_transport_class("grpc")(channel=channel())
    )
    subscriber = pubsub_v1.SubscriberClient(
        transport=pubsub_v1.SubscriberClient.get_transport_class("grpc")(channel=channel())
    )
    topic = publisher.topic_path(PROJECT, "python-matrix")
    subscription = subscriber.subscription_path(PROJECT, "python-matrix")
    publisher.create_topic(name=topic)
    subscriber.create_subscription(name=subscription, topic=topic)
    publisher.publish(topic, PAYLOAD).result(timeout=30)
    received = []

    def callback(message):
        received.append(message.data)
        message.ack()

    future = subscriber.subscribe(subscription, callback)
    deadline = time.monotonic() + 30
    while not received and time.monotonic() < deadline and not future.done():
        time.sleep(0.1)
    future.cancel()
    if future.done() and not future.cancelled() and future.exception():
        raise future.exception()
    assert received == [PAYLOAD], f"receive: got {received}"
    listed = [t.name for t in publisher.list_topics(project=f"projects/{PROJECT}")]
    assert topic in listed, "list topics"
    other = pubsub_v1.PublisherClient(
        transport=pubsub_v1.PublisherClient.get_transport_class("grpc")(
            channel=channel(denied=True)
        )
    )
    expect_denied("ungranted publish", lambda: other.publish(topic, b"no").result(timeout=30))
    subscriber.delete_subscription(subscription=subscription)
    publisher.delete_topic(topic=topic)


def firestore_client(*, denied=False):
    """Firestore's handwritten client takes no channel; docs/firestore.md sets its private API."""
    client = firestore.Client(project=PROJECT, credentials=credentials(denied=denied))
    client._firestore_api_internal = generated(FirestoreClient, denied=denied)
    return client


def run_firestore():
    client = firestore_client()
    doc = client.collection("python-matrix-cities").document("sf")
    doc.set({"name": "San Francisco", "population": 870000})
    assert doc.get().get("name") == "San Francisco", "get document"
    query = client.collection("python-matrix-cities").where(
        filter=firestore.FieldFilter("population", ">", 100000)
    )
    assert [d.id for d in query.stream()] == ["sf"], "query"
    other = firestore_client(denied=True)
    expect_denied("ungranted get", other.collection("python-matrix-cities").document("sf").get)
    doc.delete()


def run_secretmanager():
    client = generated(secretmanager.SecretManagerServiceClient)
    secret = client.create_secret(
        parent=f"projects/{PROJECT}",
        secret_id="python-matrix",
        secret={"replication": {"automatic": {}}},
    )
    client.add_secret_version(parent=secret.name, payload={"data": PAYLOAD})
    latest = f"{secret.name}/versions/latest"
    assert client.access_secret_version(name=latest).payload.data == PAYLOAD, "access"
    assert secret.name in [s.name for s in client.list_secrets(parent=f"projects/{PROJECT}")]
    other = generated(secretmanager.SecretManagerServiceClient, denied=True)
    expect_denied("ungranted access", lambda: other.access_secret_version(name=latest))
    client.delete_secret(name=secret.name)


def run_cloudkms():
    client = generated(kms.KeyManagementServiceClient)
    ring = client.create_key_ring(
        parent=f"projects/{PROJECT}/locations/us-central1", key_ring_id="python-matrix", key_ring={}
    )
    key = client.create_crypto_key(
        parent=ring.name,
        crypto_key_id="key",
        crypto_key={"purpose": kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT},
    )
    ciphertext = client.encrypt(name=key.name, plaintext=PAYLOAD).ciphertext
    assert client.decrypt(name=key.name, ciphertext=ciphertext).plaintext == PAYLOAD, "decrypt"
    assert key.name in [k.name for k in client.list_crypto_keys(parent=ring.name)], "list keys"
    other = generated(kms.KeyManagementServiceClient, denied=True)
    expect_denied("ungranted encrypt", lambda: other.encrypt(name=key.name, plaintext=b"no"))
    client.destroy_crypto_key_version(name=f"{key.name}/cryptoKeyVersions/1")


def run_cloudtasks():
    client = generated(tasks_v2.CloudTasksClient)
    location = f"projects/{PROJECT}/locations/us-central1"
    queue = client.create_queue(parent=location, queue={"name": f"{location}/queues/python-matrix"})
    # Scheduled an hour out, so the task stays queued while it is read.
    task = {
        "schedule_time": dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
        "http_request": {"url": "http://127.0.0.1:9/task", "http_method": "POST", "body": PAYLOAD},
    }
    created = client.create_task(parent=queue.name, task=task)
    client.get_task(name=created.name)
    assert created.name in [t.name for t in client.list_tasks(parent=queue.name)], "list tasks"
    other = generated(tasks_v2.CloudTasksClient, denied=True)
    expect_denied("ungranted create task", lambda: other.create_task(parent=queue.name, task=task))
    client.delete_task(name=created.name)
    client.delete_queue(name=queue.name)


def run_iam():
    client = generated(iam_admin_v1.IAMClient)
    project = f"projects/{PROJECT}"
    account = client.create_service_account(
        name=project,
        account_id="python-matrix",
        service_account=iam_admin_v1.ServiceAccount(display_name="Python matrix"),
    )
    name = f"{project}/serviceAccounts/{account.email}"
    client.get_service_account(name=name)
    assert account.email in [a.email for a in client.list_service_accounts(name=project)], "list"
    other = generated(iam_admin_v1.IAMClient, denied=True)
    expect_denied("ungranted get", lambda: other.get_service_account(name=name))
    client.delete_service_account(name=name)


# The handwritten Spanner, Bigtable, and Logging clients take no channel, so
# they get the gRPC address the public way and keep their own credentials.
GRPC_OPTIONS = ClientOptions(api_endpoint=GRPC)


def run_spanner():
    # Built-in metrics would export to monitoring.googleapis.com; nothing may leave loopback.
    client = spanner.Client(
        project=PROJECT,
        credentials=credentials(),
        client_options=GRPC_OPTIONS,
        disable_builtin_metrics=True,
    )
    instance = client.instance(
        "python-matrix",
        configuration_name=f"projects/{PROJECT}/instanceConfigs/emulator-config",
        display_name="Python matrix",
        node_count=1,
    )
    instance.create().result(timeout=60)
    ddl = ["CREATE TABLE Singers (Id INT64 NOT NULL, Name STRING(100)) PRIMARY KEY (Id)"]
    database = instance.database("matrix", ddl_statements=ddl)
    database.create().result(timeout=60)
    with database.batch() as batch:
        batch.insert("Singers", columns=("Id", "Name"), values=[(1, "Ada")])
    with database.snapshot() as snapshot:
        rows = list(snapshot.execute_sql("SELECT Name FROM Singers WHERE Id = 1"))
    assert rows == [["Ada"]], rows
    assert "matrix" in [d.name.rsplit("/", 1)[-1] for d in instance.list_databases()], "list"
    other = spanner.Client(
        project=PROJECT,
        credentials=credentials(denied=True),
        client_options=GRPC_OPTIONS,
        disable_builtin_metrics=True,
    )
    other_database = other.instance("python-matrix").database("matrix")

    def read():
        with other_database.snapshot() as snapshot:
            list(snapshot.execute_sql("SELECT Name FROM Singers"))

    expect_denied("ungranted read", read)
    database.drop()
    instance.delete()


def run_bigtable():
    client = bigtable.Client(
        project=PROJECT,
        credentials=credentials(),
        admin=True,
        client_options=GRPC_OPTIONS,
        admin_client_options=GRPC_OPTIONS,
    )
    instance = client.instance(
        "python-matrix", display_name="Python matrix", instance_type=enums.Instance.Type.PRODUCTION
    )
    cluster = instance.cluster(
        "python-matrix-c1",
        location_id="us-central1-b",
        serve_nodes=1,
        default_storage_type=enums.StorageType.SSD,
    )
    instance.create(clusters=[cluster]).result(timeout=60)
    table = instance.table("matrix")
    table.create(column_families={"cf": column_family.MaxVersionsGCRule(1)})
    row = table.direct_row(b"row-1")
    row.set_cell("cf", b"greeting", PAYLOAD)
    row.commit()
    assert table.read_row(b"row-1").cells["cf"][b"greeting"][0].value == PAYLOAD, "read row"
    assert [t.table_id for t in instance.list_tables()] == ["matrix"], "list tables"
    other = bigtable.Client(
        project=PROJECT, credentials=credentials(denied=True), client_options=GRPC_OPTIONS
    )
    expect_denied(
        "ungranted read", lambda: other.instance("python-matrix").table("matrix").read_row(b"row-1")
    )
    table.delete()
    instance.delete()


def run_logging():
    client = cloud_logging.Client(
        project=PROJECT, credentials=credentials(), client_options=GRPC_OPTIONS
    )
    logger = client.logger("python-matrix")
    logger.log_text("hello from python", severity="INFO")
    log_filter = f'logName="projects/{PROJECT}/logs/python-matrix"'
    entries = [entry.payload for entry in client.list_entries(filter_=log_filter)]
    assert entries == ["hello from python"], entries
    other = cloud_logging.Client(
        project=PROJECT, credentials=credentials(denied=True), client_options=GRPC_OPTIONS
    )
    expect_denied("ungranted write", lambda: other.logger("python-matrix").log_text("no"))
    logger.delete()


WORKFLOWS = {
    "bigquery": run_bigquery,
    "bigtable": run_bigtable,
    "cloudkms": run_cloudkms,
    "cloudtasks": run_cloudtasks,
    "firestore": run_firestore,
    "iam": run_iam,
    "logging": run_logging,
    "pubsub": run_pubsub,
    "secretmanager": run_secretmanager,
    "spanner": run_spanner,
    "storage": run_storage,
}


def timed_out(signum, frame):
    raise TimeoutError("workflow timed out after 90s")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in WORKFLOWS:
        sys.exit(f"usage: run.py SERVICE; services: {', '.join(sorted(WORKFLOWS))}")
    signal.signal(signal.SIGALRM, timed_out)
    signal.alarm(90)
    try:
        WORKFLOWS[sys.argv[1]]()
    except Exception as error:  # noqa: BLE001 -- report any failure as the result line
        print(f"{type(error).__name__}: {error}".replace("\n", " "), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
