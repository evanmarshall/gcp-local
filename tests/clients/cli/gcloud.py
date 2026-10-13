"""Drive one service workflow with the pinned gcloud CLI against gcp-local.

gcloud reads its endpoints and credentials from CLOUDSDK_* variables, so the
whole configuration is the environment built here.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from common import HTTP, PROJECT, TOOLS, expect_denied, main, run

GCLOUD = [sys.executable, f"{TOOLS}/gcloud/google-cloud-sdk/lib/gcloud.py"]


def environment(*, denied=False):
    """Point gcloud at the emulator with local ADC; nothing ambient, no prompts."""
    adc = os.environ["GCP_LOCAL_DENIED_ADC" if denied else "GOOGLE_APPLICATION_CREDENTIALS"]
    return {
        **os.environ,
        "CLOUDSDK_PYTHON": sys.executable,
        "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": adc,
        "CLOUDSDK_CORE_PROJECT": PROJECT,
        "CLOUDSDK_CORE_DISABLE_PROMPTS": "1",
        "CLOUDSDK_CORE_DISABLE_USAGE_REPORTING": "1",
        "CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK": "1",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE": f"{HTTP}/storage/v1/",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_PUBSUB": f"{HTTP}/",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER": f"{HTTP}/",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_CLOUDKMS": f"{HTTP}/",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_IAM": f"{HTTP}/",
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_IAMCREDENTIALS": f"{HTTP}/",
    }


OWNER, DENIED = environment(), environment(denied=True)


def gcloud(*arguments, stdin=None):
    return run([*GCLOUD, *arguments], OWNER, stdin)


# A lifecycle configuration gcloud round-trips: a Delete and a SetStorageClass rule.
LIFECYCLE = [
    {"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["logs/"]}},
    {"action": {"storageClass": "NEARLINE", "type": "SetStorageClass"}, "condition": {"age": 60}},
]


def run_storage():
    bucket = f"gs://gcloud-matrix-{PROJECT}"
    gcloud(
        "storage",
        "buckets",
        "create",
        bucket,
        "--location=us-central1",
        "--uniform-bucket-level-access",
    )
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory, "hello.txt")
        source.write_text("hello from gcloud")
        gcloud("storage", "cp", str(source), f"{bucket}/hello.txt")
        rules = Path(directory, "lifecycle.json")
        rules.write_text(json.dumps({"rule": LIFECYCLE}))
        gcloud("storage", "buckets", "update", bucket, f"--lifecycle-file={rules}")
    described = json.loads(gcloud("storage", "buckets", "describe", bucket, "--format=json"))
    assert described.get("lifecycle_config", {}).get("rule") == LIFECYCLE, "lifecycle round trip"
    gcloud("storage", "buckets", "update", bucket, "--clear-lifecycle")
    described = json.loads(gcloud("storage", "buckets", "describe", bucket, "--format=json"))
    assert "lifecycle_config" not in described, "lifecycle cleared"
    assert gcloud("storage", "cat", f"{bucket}/hello.txt") == "hello from gcloud", "cat"
    assert gcloud("storage", "ls", bucket).split() == [f"{bucket}/hello.txt"], "ls"
    expect_denied("ungranted cat", [*GCLOUD, "storage", "cat", f"{bucket}/hello.txt"], DENIED)
    gcloud("storage", "rm", f"{bucket}/hello.txt")
    gcloud("storage", "buckets", "delete", bucket)


def run_pubsub():
    gcloud("pubsub", "topics", "create", "gcloud-matrix")
    gcloud("pubsub", "subscriptions", "create", "gcloud-matrix", "--topic=gcloud-matrix")
    gcloud("pubsub", "topics", "publish", "gcloud-matrix", "--message=hello from gcloud")
    pulled = gcloud(
        "pubsub",
        "subscriptions",
        "pull",
        "gcloud-matrix",
        "--auto-ack",
        "--format=value(message.data)",
    )
    assert pulled.strip() == "hello from gcloud", f"pull: got {pulled!r}"
    topics = gcloud("pubsub", "topics", "list", "--format=value(name)").split()
    assert f"projects/{PROJECT}/topics/gcloud-matrix" in topics, "list topics"
    expect_denied(
        "ungranted publish",
        [*GCLOUD, "pubsub", "topics", "publish", "gcloud-matrix", "--message=no"],
        DENIED,
    )
    gcloud("pubsub", "subscriptions", "delete", "gcloud-matrix")
    gcloud("pubsub", "topics", "delete", "gcloud-matrix")


def run_secretmanager():
    gcloud(
        "secrets",
        "create",
        "gcloud-matrix",
        "--replication-policy=automatic",
        "--data-file=-",
        stdin="hello from gcloud",
    )
    access = ["secrets", "versions", "access", "latest", "--secret=gcloud-matrix"]
    assert gcloud(*access) == "hello from gcloud", "access"
    assert "gcloud-matrix" in gcloud("secrets", "list", "--format=value(name)").split(), "list"
    expect_denied("ungranted access", [*GCLOUD, *access], DENIED)
    gcloud("secrets", "delete", "gcloud-matrix")


def run_cloudkms():
    key = ["--key=key", "--keyring=gcloud-matrix", "--location=us-central1"]
    gcloud("kms", "keyrings", "create", "gcloud-matrix", "--location=us-central1")
    gcloud(
        "kms",
        "keys",
        "create",
        "key",
        "--keyring=gcloud-matrix",
        "--location=us-central1",
        "--purpose=encryption",
    )
    with tempfile.TemporaryDirectory() as directory:
        plaintext, ciphertext = Path(directory, "plain"), Path(directory, "cipher")
        plaintext.write_text("hello from gcloud")
        encrypt = ["kms", "encrypt", *key, f"--plaintext-file={plaintext}"]
        gcloud(*encrypt, f"--ciphertext-file={ciphertext}")
        decrypted = gcloud(
            "kms", "decrypt", *key, f"--ciphertext-file={ciphertext}", "--plaintext-file=-"
        )
        assert decrypted == "hello from gcloud", "decrypt"
        listed = gcloud(
            "kms",
            "keys",
            "list",
            "--keyring=gcloud-matrix",
            "--location=us-central1",
            "--format=value(name)",
        )
        assert listed.strip().endswith("/cryptoKeys/key"), f"list keys: {listed!r}"
        denied = [*GCLOUD, *encrypt, f"--ciphertext-file={ciphertext}.denied"]
        expect_denied("ungranted encrypt", denied, DENIED)
    gcloud("kms", "keys", "versions", "destroy", "1", *key)


def run_iamcredentials():
    """Impersonate a new account with --impersonate-service-account and act as it."""
    target = f"gcloud-impersonated@{PROJECT}.iam.gserviceaccount.com"
    gcloud("iam", "service-accounts", "create", "gcloud-impersonated")
    # scripts/run/clients.py enrolls the granted identity as app@PROJECT.
    for member, role in (
        (
            f"serviceAccount:app@{PROJECT}.iam.gserviceaccount.com",
            "roles/iam.serviceAccountTokenCreator",
        ),
        (f"serviceAccount:{target}", "roles/iam.serviceAccountViewer"),
    ):
        gcloud(
            "iam",
            "service-accounts",
            "add-iam-policy-binding",
            target,
            f"--member={member}",
            f"--role={role}",
        )
    describe = [
        "iam",
        "service-accounts",
        "describe",
        target,
        f"--impersonate-service-account={target}",
        "--format=value(email)",
    ]
    assert gcloud(*describe).strip() == target, "describe as the impersonated account"
    # The account may read only itself, so a project-wide list proves the
    # call ran as it rather than as the granted caller.
    listed = subprocess.run(
        [*GCLOUD, "iam", "service-accounts", "list", f"--impersonate-service-account={target}"],
        env=OWNER,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    denial = "Permission 'iam.serviceAccounts.list' denied"
    assert listed.returncode != 0 and denial in listed.stderr, (
        f"impersonated list: {listed.stderr.strip()[-400:]}"
    )
    expect_denied("ungranted impersonation", [*GCLOUD, *describe], DENIED)
    gcloud("iam", "service-accounts", "delete", target)


if __name__ == "__main__":
    main(
        {
            "cloudkms": run_cloudkms,
            "iamcredentials": run_iamcredentials,
            "pubsub": run_pubsub,
            "secretmanager": run_secretmanager,
            "storage": run_storage,
        },
        sys.argv,
    )
