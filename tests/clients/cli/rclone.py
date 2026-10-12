"""Drive the Storage workflow with the pinned rclone against gcp-local.

rclone reads its remote from RCLONE_CONFIG_* variables and local ADC through
env_auth, so the whole configuration is the environment built here.
"""

import os
import sys
import tempfile
from pathlib import Path

from common import HTTP, PROJECT, TOOLS, expect_denied, main, run

RCLONE = [f"{TOOLS}/rclone/bin/rclone", "--retries=1", "--low-level-retries=1"]


def environment(*, denied=False):
    """Define the `gcs` remote: the emulator's JSON API base and ADC from the environment."""
    env = {
        **os.environ,
        "RCLONE_CONFIG": "/dev/null",
        "RCLONE_CONFIG_GCS_TYPE": "google cloud storage",
        "RCLONE_CONFIG_GCS_ENV_AUTH": "true",
        "RCLONE_CONFIG_GCS_ENDPOINT": f"{HTTP}/storage/v1/",
        "RCLONE_CONFIG_GCS_PROJECT_NUMBER": PROJECT,
        "RCLONE_CONFIG_GCS_LOCATION": "us-central1",
        "RCLONE_CONFIG_GCS_BUCKET_POLICY_ONLY": "true",
    }
    if denied:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GCP_LOCAL_DENIED_ADC"]
    return env


OWNER, DENIED = environment(), environment(denied=True)


def rclone(*arguments):
    return run([*RCLONE, *arguments], OWNER)


def run_storage():
    bucket = f"gcs:rclone-matrix-{PROJECT}"
    rclone("mkdir", bucket)
    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory, "hello.txt")
        source.write_text("hello from rclone")
        rclone("copyto", str(source), f"{bucket}/hello.txt")
    assert rclone("cat", f"{bucket}/hello.txt") == "hello from rclone", "cat"
    rclone("moveto", f"{bucket}/hello.txt", f"{bucket}/moved.txt")
    assert rclone("lsf", bucket).split() == ["moved.txt"], "lsf after moveto"
    expect_denied("ungranted cat", [*RCLONE, "cat", f"{bucket}/moved.txt"], DENIED)
    rclone("deletefile", f"{bucket}/moved.txt")
    rclone("rmdir", bucket)


if __name__ == "__main__":
    main({"storage": run_storage}, sys.argv)
