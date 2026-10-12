"""Drive one service workflow with the pinned gcloud CLI against gcp-local.

gcloud reads its endpoints and credentials from CLOUDSDK_* variables, so the
whole configuration is the environment built here.
"""

import os
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
    }


OWNER, DENIED = environment(), environment(denied=True)


def gcloud(*arguments, stdin=None):
    return run([*GCLOUD, *arguments], OWNER, stdin)


def run_storage():
    bucket = f"gs://gcloud-matrix-{PROJECT}"
    gcloud(
        "storage",
        "buckets",
        "create",
        bucket,
        "--location=us-central1",
        "--uniform-bucket-level-access",
        "--public-access-prevention",
        "--soft-delete-duration=0",
    )
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory, "hello.txt")
        source.write_text("hello from gcloud")
        gcloud("storage", "cp", str(source), f"{bucket}/hello.txt")
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


if __name__ == "__main__":
    main(
        {
            "cloudkms": run_cloudkms,
            "pubsub": run_pubsub,
            "secretmanager": run_secretmanager,
            "storage": run_storage,
        },
        sys.argv,
    )
