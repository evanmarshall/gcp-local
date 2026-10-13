"""Drive one service workflow with the pinned Python Google Cloud clients against gcp-local.

scripts/run/clients.py starts the emulator and runs `run.py SERVICE` once per
service, at each client's default transport. The process exits 0 when the
workflow passes and prints the failing step otherwise.
"""

import datetime as dt
import os
import signal
import sys
import time

import google.auth
import google.auth.credentials
import google.auth.exceptions
import grpc
import requests
from google.api_core import exceptions
from google.api_core.client_options import ClientOptions
from google.auth import impersonated_credentials
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import AuthorizedSession, Request
from google.cloud import (
    bigquery,
    bigtable,
    firestore,
    iam_admin_v1,
    kms,
    pubsub_v1,
    run_v2,
    scheduler_v1,
    secretmanager,
    spanner,
    storage,
    tasks_v2,
)
from google.cloud import logging as cloud_logging
from google.cloud.bigtable import column_family, enums
from google.cloud.firestore_v1.services.firestore import FirestoreClient
from google.iam.v1 import policy_pb2

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
HTTP = os.environ["GCP_LOCAL_HTTP"]
GRPC = os.environ["GCP_LOCAL_GRPC"]
GRPC_TLS = os.environ["GCP_LOCAL_GRPC_TLS"]
PAYLOAD = b"hello from python"
REVISION_IMAGE = "client-matrix/revision:1"


def credentials(*, denied=False):
    """Load local ADC (the granted identity) or the ungranted identity's ADC file.

    Unscoped, as plain ADC loads: each client applies its default scopes.
    """
    if denied:
        loaded, _ = google.auth.load_credentials_from_file(os.environ["GCP_LOCAL_DENIED_ADC"])
        return loaded
    return google.auth.default()[0]


def channel(transport, *, denied=False):
    """Open a plaintext loopback gRPC channel that still carries ADC.

    `local_channel_credentials` lets gRPC attach call credentials without TLS.
    Only generated clients take a channel; handwritten wrappers do not. The
    channel's credentials get the transport's default scopes, as the client
    would apply.
    """
    session = requests.Session()
    session.trust_env = False
    return secure_authorized_channel(
        google.auth.credentials.with_scopes_if_required(
            credentials(denied=denied), None, default_scopes=transport.AUTH_SCOPES
        ),
        Request(session=session),
        GRPC,
        ssl_credentials=grpc.local_channel_credentials(),
    )


def generated(client_class, *, denied=False):
    """Build a generated (GAPIC) client on the gRPC gateway."""
    transport = client_class.get_transport_class("grpc")
    return client_class(transport=transport(channel=channel(transport, denied=denied)))


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
    # Versioning: an overwrite keeps the old generation as noncurrent.
    bucket.versioning_enabled = True
    bucket.patch()
    blob.upload_from_string(b"second")
    versions = list(client.list_blobs(bucket, versions=True))
    generations = [version.generation for version in versions]
    assert generations == sorted(generations), "version order"
    assert len(versions) == 2 and versions[0].time_deleted is not None, "noncurrent generation"
    old = bucket.blob("hello.txt", generation=versions[0].generation)
    assert old.download_as_bytes() == PAYLOAD, "noncurrent download"
    bucket.delete_blob("hello.txt")
    remaining = list(client.list_blobs(bucket, versions=True))
    assert len(remaining) == 2, "a plain delete keeps the generation"
    for version in versions:
        bucket.blob(version.name, generation=version.generation).delete()
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
    publisher = generated(pubsub_v1.PublisherClient)
    subscriber = generated(pubsub_v1.SubscriberClient)
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
    other = generated(pubsub_v1.PublisherClient, denied=True)
    expect_denied("ungranted publish", lambda: other.publish(topic, b"no").result(timeout=30))
    subscriber.delete_subscription(subscription=subscription)
    publisher.delete_topic(topic=topic)


def firestore_client(*, denied=False):
    """Firestore's handwritten client takes no channel.

    docs/services/databases/firestore.md sets its private API.
    """
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


def run_cloudscheduler():
    # A yearly HTTP job: it never runs, so its unreachable target is never called.
    client = generated(scheduler_v1.CloudSchedulerClient)
    location = f"projects/{PROJECT}/locations/us-central1"
    job = client.create_job(parent=location, job={
        "name": f"{location}/jobs/python-matrix",
        "schedule": "0 0 1 1 *",
        "time_zone": "America/New_York",
        "http_target": {"uri": "http://127.0.0.1:9/job", "http_method": "POST", "body": PAYLOAD},
    })
    assert client.get_job(name=job.name).state == scheduler_v1.Job.State.ENABLED, "get job"
    assert job.name in [j.name for j in client.list_jobs(parent=location)], "list jobs"
    assert client.pause_job(name=job.name).state == scheduler_v1.Job.State.PAUSED, "pause job"
    client.resume_job(name=job.name)
    other = generated(scheduler_v1.CloudSchedulerClient, denied=True)
    expect_denied("ungranted run job", lambda: other.run_job(name=job.name))
    client.delete_job(name=job.name)


def run_cloudrun():
    client = generated(run_v2.ServicesClient)
    location = f"projects/{PROJECT}/locations/us-central1"
    # scripts/run/clients.py maps this image to a local revision process.
    service = run_v2.Service(
        template=run_v2.RevisionTemplate(containers=[run_v2.Container(image=REVISION_IMAGE)])
    )
    created = client.create_service(
        parent=location, service=service, service_id="python-matrix"
    ).result(timeout=60)
    client.get_service(name=created.name)
    assert created.name in [s.name for s in client.list_services(parent=location)], "list"
    other = generated(run_v2.ServicesClient, denied=True)
    expect_denied(
        "ungranted create service",
        lambda: other.create_service(parent=location, service=service, service_id="denied"),
    )
    client.delete_service(name=created.name).result(timeout=60)


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


CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"


def impersonated(target, *, denied=False):
    """google.auth impersonated credentials for target, minted at gcp-local."""
    source = google.auth.credentials.with_scopes_if_required(
        credentials(denied=denied), [CLOUD_PLATFORM]
    )
    return impersonated_credentials.Credentials(
        source_credentials=source,
        target_principal=target,
        target_scopes=[CLOUD_PLATFORM],
        lifetime=600,
        iam_endpoint_override=(
            f"{HTTP}/v1/projects/-/serviceAccounts/{target}:generateAccessToken"
        ),
    )


def run_iamcredentials():
    client = generated(iam_admin_v1.IAMClient)
    project = f"projects/{PROJECT}"
    account = client.create_service_account(
        name=project,
        account_id="python-impersonated",
        service_account=iam_admin_v1.ServiceAccount(),
    )
    name = f"{project}/serviceAccounts/{account.email}"
    # scripts/run/clients.py enrolls the granted identity as app@PROJECT.
    client.set_iam_policy(
        request={
            "resource": name,
            "policy": policy_pb2.Policy(
                bindings=[
                    policy_pb2.Binding(
                        role="roles/iam.serviceAccountTokenCreator",
                        members=[f"serviceAccount:app@{PROJECT}.iam.gserviceaccount.com"],
                    ),
                    policy_pb2.Binding(
                        role="roles/iam.serviceAccountViewer",
                        members=[f"serviceAccount:{account.email}"],
                    ),
                ]
            ),
        }
    )
    session = AuthorizedSession(impersonated(account.email))
    session.trust_env = False
    read = session.get(f"{HTTP}/v1/{name}")
    assert read.status_code == 200, f"read: {read.status_code}"
    assert read.json()["email"] == account.email, "read the impersonated account"
    # The account may read only itself: a project-wide list runs as it, not as the caller.
    listed = session.get(f"{HTTP}/v1/{project}/serviceAccounts")
    assert listed.status_code == 403, f"impersonated list: {listed.status_code}"
    refused = ""
    try:
        impersonated(account.email, denied=True).refresh(Request())
    except google.auth.exceptions.RefreshError as error:
        refused = str(error)
    assert "PERMISSION_DENIED" in refused, f"ungranted impersonation: {refused or 'allowed'}"
    client.delete_service_account(name=name)


# The handwritten Spanner, Bigtable, and Logging clients take no channel and
# open TLS, so they get the gateway's TLS address and keep their own
# credentials; GRPC_DEFAULT_SSL_ROOTS_FILE_PATH names the environment's CA.
GRPC_OPTIONS = ClientOptions(api_endpoint=GRPC_TLS)


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
    # The first write also logs the client's instrumentation diagnostic entry.
    written = [p for p in entries if "logging.googleapis.com/diagnostic" not in p]
    assert written == ["hello from python"], entries
    other = cloud_logging.Client(
        project=PROJECT, credentials=credentials(denied=True), client_options=GRPC_OPTIONS
    )
    expect_denied("ungranted write", lambda: other.logger("python-matrix").log_text("no"))
    logger.delete()


WORKFLOWS = {
    "bigquery": run_bigquery,
    "bigtable": run_bigtable,
    "cloudkms": run_cloudkms,
    "cloudscheduler": run_cloudscheduler,
    "cloudtasks": run_cloudtasks,
    "firestore": run_firestore,
    "iam": run_iam,
    "iamcredentials": run_iamcredentials,
    "logging": run_logging,
    "pubsub": run_pubsub,
    "run": run_cloudrun,
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
