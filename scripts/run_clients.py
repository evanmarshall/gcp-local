#!/usr/bin/env python3
"""Run the client matrix against bin/gcp-local inside scripts/run_isolated.py.

Starts the production core with local ADC for a granted and an ungranted
identity, runs every declared (service, client) workflow, and writes
.artifacts/client-matrix/results.json. A declared pass that fails, or a
declared expected failure that passes, fails the run.
"""

import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

import client_matrix

ROOT = client_matrix.ROOT
sys.path.insert(0, str(ROOT))
from gcp_local.bootstrap import DEVELOPER_ROLES  # noqa: E402 -- needs the repository root on sys.path

PROJECT = "client-matrix"
HTTP = "http://127.0.0.1:4566"
GRPC = "127.0.0.1:4567"
SERVICES = (
    "bigquery,bigtable,cloudtasks,firestore,iam,kms,logging,pubsub,"
    "resourcemanager,run,secretmanager,spanner,storage"
)
REVISION_IMAGE = "client-matrix/revision:1"
TIMEOUT = 150


def account(name: str) -> str:
    """Name a service account principal in the matrix project."""
    return f"serviceAccount:{name}@{PROJECT}.iam.gserviceaccount.com"


def write_private(path: Path, value: object) -> None:
    """Write an owner-only file the core and clients accept."""
    text = value if isinstance(value, str) else json.dumps(value, indent=2)
    path.touch(mode=0o600)
    path.write_text(text)


def write_identity(directory: Path) -> dict[str, Path]:
    """Enroll the granted app and the ungranted peer; return each one's ADC file."""
    principals, files = [], {}
    for name in ("app", "peer"):
        token = "gls." + secrets.token_urlsafe(32)
        subject = directory / f"{name}-subject.txt"
        write_private(subject, token)
        principals.append({"subject": account(name), "project": PROJECT, "subject_token": token})
        files[name] = directory / f"{name}-adc.json"
        write_private(
            files[name],
            {
                "type": "external_account",
                "audience": "//gcp-local/identity",
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "token_url": HTTP + "/v1/token",
                "credential_source": {"file": str(subject)},
            },
        )
    write_private(directory / "server.json", {"principals": principals})
    project = "//cloudresourcemanager.googleapis.com/projects/" + PROJECT
    write_private(
        directory / "iam.json",
        {
            "resources": [
                {
                    "name": project,
                    "service": "resourcemanager",
                    "kind": "project",
                    "number": "424242424242",
                    "scope": {"project": PROJECT, "location": "global"},
                }
            ],
            "policies": {
                project: {
                    "bindings": [
                        {"role": role, "members": [account("app")]} for role in DEVELOPER_ROLES
                    ]
                }
            },
        },
    )
    return files


def start_core(directory: Path, log: Path) -> subprocess.Popen:
    """Start bin/gcp-local with every matrix service and wait for its health check."""
    java = os.environ["GCP_LOCAL_JAVA"]
    command = [
        str(ROOT / "bin/gcp-local"),
        "-services=" + SERVICES,
        f"-identity-config={directory / 'server.json'}",
        f"-iam-config={directory / 'iam.json'}",
        f"-pubsub-java={java}",
        f"-pubsub-jar={os.environ['GCP_LOCAL_PUBSUB_JAR']}",
        f"-firestore-java={java}",
        f"-firestore-jar={os.environ['GCP_LOCAL_FIRESTORE_JAR']}",
        f"-spanner-emulator={os.environ['GCP_LOCAL_SPANNER_EMULATOR']}",
        f"-spanner-libraries={os.environ['GCP_LOCAL_SPANNER_LIBRARIES']}",
        # Cloud Run revisions run as local processes of this one image.
        f"-run-process={REVISION_IMAGE}={sys.executable} -I {ROOT / 'tests/clients/revision.py'}",
    ]
    core = subprocess.Popen(command, stdout=log.open("w"), stderr=subprocess.STDOUT)
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if core.poll() is not None:
            raise SystemExit(f"gcp-local exited; see {log}")
        try:
            with opener.open(HTTP + "/_local/health", timeout=1) as response:
                if response.status == 200:
                    return core
        except OSError:
            time.sleep(0.2)
    core.kill()
    raise SystemExit("gcp-local did not become healthy")


def client_environment(files: dict[str, Path], home: Path) -> dict[str, str]:
    """Give clients local ADC and the emulator addresses, and nothing ambient."""
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(home),
        "LANG": "C.UTF-8",
        "GOOGLE_APPLICATION_CREDENTIALS": str(files["app"]),
        "GCP_LOCAL_DENIED_ADC": str(files["peer"]),
        "GOOGLE_CLOUD_PROJECT": PROJECT,
        "GCP_LOCAL_HTTP": HTTP,
        "GCP_LOCAL_GRPC": GRPC,
        "CLOUDSDK_CONFIG": str(home / "gcloud"),
        "GOOGLE_API_USE_CLIENT_CERTIFICATE": "false",
        "GOOGLE_API_USE_MTLS_ENDPOINT": "never",
        "GCP_LOCAL_TOOLS": str(ROOT / ".tools/clients"),
    }


def run_combo(combo: dict, command: list[str], env: dict[str, str], logs: Path) -> dict:
    """Run one workflow and classify it against its declared expectation."""
    log = logs / f"{combo['client']}-{combo['service']}.log"
    started = time.monotonic()
    try:
        result = subprocess.run(
            [*command, combo["service"]],
            env=env,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
            check=False,
        )
        output, passed = result.stdout + result.stderr, result.returncode == 0
    except subprocess.TimeoutExpired as error:
        output = (error.stdout or b"").decode(errors="replace") + (error.stderr or b"").decode(
            errors="replace"
        )
        output, passed = output + f"\ntimed out after {TIMEOUT}s", False
    log.write_text(output)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    reason = "" if passed else (lines[-1] if lines else "no output")[:500]
    expected = combo["expect"]
    outcome = {
        (True, "pass"): "pass",
        (False, "pass"): "fail",
        (False, "xfail"): "xfail",
        (True, "xfail"): "xpass",
    }[(passed, expected)]
    # An expected failure must still fail on its linked gap, not on something new.
    if outcome == "xfail" and combo["match"] not in output:
        outcome, reason = "fail", f"failed without {combo['match']!r}: {reason}"
    return {
        "service": combo["service"],
        "client": combo["client"],
        "package": combo["package"],
        "version": combo["version"],
        "transport": combo["transport"],
        "expected": expected,
        "outcome": outcome,
        "issue": combo.get("issue"),
        "reason": reason,
        "seconds": round(time.monotonic() - started, 1),
    }


def main() -> int:
    """Run the selected combinations and report any that disagree with the matrix."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", action="append", help="limit to these clients")
    parser.add_argument("--service", action="append", help="limit to these services")
    parser.add_argument("--output", type=Path, default=ROOT / ".artifacts/client-matrix")
    args = parser.parse_args()
    if os.environ.get("GCP_LOCAL_ISOLATED") != "1":
        raise SystemExit("Run inside scripts/run_isolated.py (scripts/check_clients.sh)")
    matrix = client_matrix.load()
    # Commands run from the repository root; {python} and {node} are the pinned interpreters.
    interpreters = {
        "{python}": os.environ.get("GCP_LOCAL_TEST_PYTHON", sys.executable),
        "{node}": str(ROOT / ".tools/clients/node/bin/node"),
    }
    commands = {
        name: [interpreters.get(part, part) for part in client["command"]]
        for name, client in matrix["clients"].items()
    }
    combos = [
        combo
        for combo in matrix["combos"]
        if (not args.client or combo["client"] in args.client)
        and (not args.service or combo["service"] in args.service)
    ]
    args.output.mkdir(parents=True, exist_ok=True)
    logs = args.output / "logs"
    logs.mkdir(exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="client-matrix-") as temporary:
        directory = Path(temporary)
        directory.chmod(0o700)
        files = write_identity(directory)
        home = directory / "home"
        home.mkdir()
        core = start_core(directory, args.output / "gcp-local.log")
        try:
            env = client_environment(files, home)
            for combo in combos:
                result = run_combo(combo, commands[combo["client"]], env, logs)
                results.append(result)
                print(
                    f"{result['outcome']:6} {result['service']:14} {result['client']:7} "
                    f"{result['seconds']:6.1f}s {result['reason'][:160]}",
                    flush=True,
                )
        finally:
            core.terminate()
            core.wait(timeout=30)
    (args.output / "results.json").write_text(json.dumps({"results": results}, indent=2) + "\n")
    wrong = [r for r in results if r["outcome"] in {"fail", "xpass"}]
    for result in wrong:
        print(
            f"{result['outcome'].upper()}: {result['service']} with {result['client']}: "
            + (result["reason"] or "passed; update tests/clients/matrix.json and docs/coverage.md"),
            file=sys.stderr,
        )
    return 1 if wrong else 0


if __name__ == "__main__":
    sys.exit(main())
