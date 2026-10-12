"""Workflow acceptance: Terraform-provisioned Pub/Sub push -> Cloud Run -> Storage.

The example Terraform (examples/workflow/main.tf) provisions every resource
through the released google provider against a private local core. A
published message must reach the worker, which stores an object under its own
service account. Toggling the worker's Storage grant and the push identity's
invoker grant through Terraform must fail at those boundaries and recover. A
second plan must be empty, and destroy must leave no listed resources.

Revisions run through the process runtime, so the acceptance needs no
container engine. Terraform only ever sees loopback
endpoints, a reserved universe domain, and local external-account ADC.

With --packaged, the same workflow runs against the packaged environment
started with the documented ``scripts/dev/local_env.py up`` (host Docker; the
revision runs as a process inside the core container). Instead of destroy,
``reset`` must leave every workflow resource kind empty and no worker running,
and ``down`` must remove the environment's containers.

With --packaged --cloud-run-docker, the worker is a user-built image
(examples/workflow/Dockerfile under a fresh tag) run as a revision container
on the environment's network. A second Docker-mode environment runs its own
revision; the first environment's reset and down must remove only the first
environment's containers.
"""

import argparse
import json
import os
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, NamedTuple

import google.auth
import grpc
from cloudrun import start_core
from google.auth.transport.grpc import secure_authorized_channel
from google.auth.transport.requests import Request
from google.cloud import pubsub_v1
from google.pubsub_v1.services.publisher.transports.grpc import PublisherGrpcTransport
from support import free_ports

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from gcp_local import bootstrap

REPO = Path(__file__).resolve().parents[2]
SERVE_SOCKET = 9298
PUBSUB_GRPC_PORT = SERVE_SOCKET + 1
BASE = "http://127.0.0.1:" + str(SERVE_SOCKET)
PROJECT = "demo-run"
# A real-shaped project number: the push contract names the Pub/Sub service
# agent by it and requires no leading zeros.
PROJECT_NUMBER = "123456789012"
REGION = "us-central1"
BUCKET = "workflow-" + PROJECT
TOPIC = "projects/" + PROJECT + "/topics/workflow"
WORKER_IMAGE = "gcp-local/workflow:latest"
WORKER_EMAIL = "worker@" + PROJECT + ".iam.gserviceaccount.com"
DEVELOPER = "serviceAccount:developer@" + PROJECT + ".iam.gserviceaccount.com"
WORKER_SOURCE = str(REPO / "examples/workflow/worker.py")
# Revisions get a minimal environment, so the worker needs an interpreter that
# runs without one: setup-python's build needs LD_LIBRARY_PATH and otherwise
# loads the system libpython against its own stdlib. The worker is stdlib-only.
WORKER_PYTHON = "/usr/bin/python3" if Path("/usr/bin/python3").exists() else "python3"
# Matches a revision process, not the core whose flags name the same file.
WORKER_PROCESS = "^" + WORKER_PYTHON + " " + WORKER_SOURCE
PGREP = shutil.which("pgrep") or "/usr/bin/pgrep"
PKILL = shutil.which("pkill") or "/usr/bin/pkill"
TERRAFORM = REPO / ".tools/terraform/bin/terraform"
DOCKER = shutil.which("docker") or "docker"
# The packaged image's bundled worker command (gcp_local/container.py maps it).
# A revision's command line starts with it; the core's own flags only name it.
PACKAGED_WORKER = "/usr/local/bin/python3 -I /opt/gcp-local/workflow/worker.py"
# How long a refused delivery must stay refused before the grant is restored.
REFUSAL_WINDOW = 8.0


def write_iam(directory: Path) -> Path:
    """Grant the developer only the project roles the Terraform apply needs."""
    resource = "//cloudresourcemanager.googleapis.com/projects/" + PROJECT
    roles = ["roles/storage.admin", "roles/pubsub.admin", "roles/run.admin",
             "roles/iam.serviceAccountAdmin", "roles/iam.serviceAccountUser"]
    iam = {
        "resources": [{"name": resource, "service": "resourcemanager", "kind": "project",
                       "number": PROJECT_NUMBER,
                       "scope": {"project": PROJECT, "location": "global"}}],
        "policies": {resource: {"bindings": [
            {"role": role, "members": [DEVELOPER]} for role in roles]}},
    }
    path = directory / "iam.json"
    bootstrap.private_json(path, iam)
    return path


def write_identity(directory: Path) -> tuple[Path, Path, Path]:
    """Enroll the developer and the worker; return server, SDK ADC, and Terraform ADC.

    The worker's enrollment backs its Cloud Run metadata tokens; Terraform
    creates the matching account record.
    """
    identity = directory / "identity"
    bootstrap.write_identity(
        identity, project=PROJECT, principal=DEVELOPER, endpoint=BASE, workloads=[WORKER_EMAIL]
    )
    return identity / "server.json", identity / "adc.json", identity / "terraform-adc.json"


class Local(NamedTuple):
    """One local core: its loopback HTTP base and the developer's access token."""

    base: str
    token: str


def api(local: Local, method: str, path: str) -> tuple[int, Any]:
    """Issue one authenticated request against the local core."""
    head = urllib.request.Request(local.base + path, method=method)  # noqa: S310 -- loopback base
    head.add_header("Authorization", "Bearer " + local.token)
    try:
        with urllib.request.urlopen(head, timeout=30) as response:  # noqa: S310 -- fixed loopback base
            payload = response.read()
            return response.status, payload
    except urllib.error.HTTPError as error:
        return error.code, error.read()


class Terraform:
    """Run the pinned Terraform against a private copy of the example."""

    def __init__(self, workdir: Path, variables: dict[str, str]) -> None:
        """Stage the example and a filesystem-only provider mirror."""
        shutil.copy(REPO / "examples/workflow/main.tf", workdir / "main.tf")
        config = workdir / "terraform.rc"
        config.write_text(
            "disable_checkpoint = true\nprovider_installation {\n filesystem_mirror {\n path = "
            + json.dumps(str(REPO / ".tools/terraform/mirror"))
            + '\n include = ["registry.terraform.io/hashicorp/google"]\n }\n}\n')
        self.workdir = workdir
        # Only what Terraform needs: no ambient credentials, gcloud config, or
        # proxies; variables travel in the environment, never in argv. The
        # dead proxy is an independent guard: any non-loopback HTTP(S) request
        # fails instead of leaving the machine (loopback is never proxied).
        self.env = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
        self.env.update({
            "TF_CLI_CONFIG_FILE": str(config), "TF_IN_AUTOMATION": "1",
            "CHECKPOINT_DISABLE": "1", "HTTPS_PROXY": "http://127.0.0.1:9",
            "HTTP_PROXY": "http://127.0.0.1:9", "CLOUDSDK_CONFIG": str(workdir / "gcloud"),
        })
        self.env.update({"TF_VAR_" + key: value for key, value in variables.items()})

    def run(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Run one Terraform command in the staged directory."""
        result = subprocess.run(
            [str(TERRAFORM), *arguments, "-no-color", "-input=false"],
            cwd=self.workdir, env=self.env, capture_output=True, text=True, check=False)
        if check and result.returncode != 0:
            raise AssertionError("terraform " + arguments[0] + " failed:\n"
                                 + result.stdout[-4000:] + result.stderr[-4000:])
        return result

    def apply(self, **toggles: bool) -> None:
        """Apply the example with the given grant toggles."""
        flags = ["-var=" + key + "=" + str(value).lower() for key, value in toggles.items()]
        self.run("apply", "-auto-approve", *flags)

    def assert_local_only(self, sdk_adc: Path) -> None:
        """Require the variable guards to refuse a non-loopback endpoint and real-shaped ADC."""
        for override, refusal in (
            ("endpoint=https://storage.googleapis.com", "never targets real GCP"),
            ("credentials_file=" + str(sdk_adc), "Use a local gcp-local ADC file"),
        ):
            result = self.run("plan", "-var=" + override, check=False)
            output = " ".join((result.stdout + result.stderr).split())
            assert result.returncode != 0, "terraform accepted " + override
            assert refusal in output, override + " failed for another reason:\n" + output[-2000:]

    def assert_no_changes(self) -> None:
        """Require an empty plan (exit code 0 with -detailed-exitcode)."""
        result = self.run("plan", "-detailed-exitcode", check=False)
        assert result.returncode == 0, "second plan is not clean:\n" + result.stdout[-4000:]


def publish(publisher: pubsub_v1.PublisherClient, payload: str, message_id: str) -> None:
    """Publish one message through the SDK client."""
    future = publisher.publish(TOPIC, payload.encode(), bucket=BUCKET, messageId=message_id)
    assert future.result(timeout=15), "publish failed"


def object_status(local: Local, message_id: str) -> tuple[int, bytes]:
    """Read one stored object as the developer."""
    return api(local, "GET", "/storage/v1/b/" + BUCKET + "/o/" + message_id + ".txt?alt=media")


def expect_object(local: Local, message_id: str, payload: str, deadline: float = 60) -> None:
    """Wait for the worker to store the message payload."""
    end = time.time() + deadline
    while time.time() < end:
        code, body = object_status(local, message_id)
        if code == 200:
            assert body.decode() == payload, body
            return
        time.sleep(1)
    raise AssertionError("The worker did not store the object: " + message_id)


def expect_absent(local: Local, message_id: str) -> None:
    """Require the object to stay absent for the whole refusal window."""
    end = time.time() + REFUSAL_WINDOW
    while time.time() < end:
        code, _ = object_status(local, message_id)
        assert code == 404, "a refused delivery still stored " + message_id
        time.sleep(1)


def storage_denial(terraform: Terraform, publisher: pubsub_v1.PublisherClient,
                   local: Local) -> None:
    """Revoke the worker's object grant, expect no write, restore, expect recovery."""
    terraform.apply(grant_worker_storage=False)
    publish(publisher, "denied-payload", "denied-msg")
    expect_absent(local, "denied-msg")
    terraform.apply(grant_worker_storage=True)
    # The refused message is redelivered and now succeeds.
    expect_object(local, "denied-msg", "denied-payload")
    publish(publisher, "restored-payload", "restored-msg")
    expect_object(local, "restored-msg", "restored-payload")


def invoker_denial(terraform: Terraform, publisher: pubsub_v1.PublisherClient,
                   local: Local) -> None:
    """Revoke run.invoker from the push identity; the push stops at invocation."""
    terraform.apply(grant_push_invoker=False)
    # The worker's Storage grant is intact, so a delivery that reached the
    # worker would store the object: its absence places the refusal at the
    # invocation check, the only boundary this toggle changes.
    publish(publisher, "uninvoked-payload", "uninvoked-msg")
    expect_absent(local, "uninvoked-msg")
    terraform.apply(grant_push_invoker=True)
    expect_object(local, "uninvoked-msg", "uninvoked-payload")


def assert_empty(local: Local, after: str = "destroy") -> None:
    """List every workflow resource kind through the local API; all must be gone."""
    listings = {
        "buckets": "/storage/v1/b?project=" + PROJECT,
        "topics": "/v1/projects/" + PROJECT + "/topics",
        "subscriptions": "/v1/projects/" + PROJECT + "/subscriptions",
        "services": "/v2/projects/" + PROJECT + "/locations/" + REGION + "/services",
        "accounts": "/v1/projects/" + PROJECT + "/serviceAccounts",
    }
    for kind, path in listings.items():
        code, body = api(local, "GET", path)
        assert code == 200, kind + " listing failed: " + str(code) + " " + body.decode()[:200]
        document = json.loads(body or b"{}")
        leftovers = [value for key, value in document.items()
                     if isinstance(value, list) and key != "unreachable" and value]
        assert not leftovers, kind + " survived " + after + ": " + json.dumps(leftovers)[:400]
        print("after " + after + ":", kind, "listing is empty")


def assert_no_revisions() -> None:
    """Require destroy to stop every revision process."""
    running = subprocess.run([PGREP, "-af", WORKER_PROCESS], capture_output=True, check=False,
                             text=True)
    assert running.returncode == 1, "The workflow left a worker process: " + running.stdout
    print("after destroy: no revision process remains")


def install_tools() -> tuple[Path, str]:
    """Build the core and install the pinned Pub/Sub jar and Terraform."""
    for installer in (["scripts/install/go.py"], ["scripts/install/emulator_jar.py", "pubsub"],
                      ["scripts/install/terraform.py"]):
        subprocess.run([sys.executable, str(REPO / "scripts" / installer[0]), *installer[1:]],
                       check=True, capture_output=True, text=True)
    binary = REPO / "bin" / "gcp-local"
    subprocess.run(
        ["/bin/sh", str(REPO / "scripts/go.sh"), "build", "-o", str(binary), "./cmd/gcp-local"],
        check=True, capture_output=True, cwd=REPO)
    java = os.environ.get("GCP_LOCAL_JAVA") or shutil.which("java") or ""
    assert java.startswith("/"), "An absolute Java path is required"
    assert TERRAFORM.is_file(), "The pinned Terraform is missing"
    return binary, java


def exercise(terraform: Terraform, credentials: google.auth.credentials.Credentials,
             local: Local, grpc_target: str) -> None:
    """Deliver one message, then exercise both denial boundaries and re-plan."""
    # Refresh before the channel exists: the on-demand refresh inside the
    # gRPC auth interceptor fails and the SDK retries it forever.
    channel = secure_authorized_channel(
        credentials, Request(), grpc_target, ssl_credentials=grpc.local_channel_credentials())
    publisher = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(channel=channel))
    publish(publisher, "workflow-payload", "wf-msg-1")
    expect_object(local, "wf-msg-1", "workflow-payload")
    print("published message stored by the worker")
    storage_denial(terraform, publisher, local)
    print("storage grant removed: write refused; restored: redelivery stored")
    invoker_denial(terraform, publisher, local)
    print("invoker grant removed: push refused at invocation; restored: delivered")
    terraform.assert_no_changes()
    print("second plan: no changes")
    channel.close()


def provision(terraform: Terraform, sdk_adc: Path) -> None:
    """Initialize, check the local-only guards, and apply the example."""
    terraform.run("init")
    # The SDK ADC file lacks the local universe marker, so it doubles as a
    # credential the configuration must refuse.
    terraform.assert_local_only(sdk_adc)
    print("guards: variable validation refused a non-loopback endpoint and unmarked ADC")
    terraform.apply()


def run_workflow(terraform: Terraform, credentials: google.auth.credentials.Credentials,
                 sdk_adc: Path) -> None:
    """Apply, exercise both denial boundaries, re-plan, destroy, and list."""
    provision(terraform, sdk_adc)
    local = Local(BASE, credentials.token)
    exercise(terraform, credentials, local, "127.0.0.1:" + str(PUBSUB_GRPC_PORT))
    terraform.run("destroy", "-auto-approve")
    assert_empty(local)


def environment(directory: Path, action: str, *options: str) -> str:
    """Run one documented packaged-environment command and return its output."""
    result = subprocess.run(
        [sys.executable, str(REPO / "scripts/dev/local_env.py"), "--directory", str(directory),
         *options, action],
        capture_output=True, text=True, timeout=1800, check=False)
    if result.returncode:
        raise AssertionError("scripts/dev/local_env.py " + action + " failed:\n"
                             + result.stdout[-4000:] + result.stderr[-4000:])
    return result.stdout


def packaged_workers(container: str) -> list[str]:
    """List the bundled worker processes running inside the core container."""
    top = subprocess.run([DOCKER, "top", container, "-o", "pid,args"], capture_output=True,
                         text=True, check=True, timeout=30)
    rows = [line.split(None, 1) for line in top.stdout.splitlines()[1:]]
    return [row[1] for row in rows if len(row) == 2 and row[1].startswith(PACKAGED_WORKER)]


def packaged_credentials(adc: str) -> google.auth.credentials.Credentials:
    """Load and refresh the environment's developer ADC."""
    credentials, _ = google.auth.load_credentials_from_file(
        adc, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    return credentials


def compose_project(directory: Path) -> str:
    """Return the environment's Compose project, which also labels its revisions."""
    return json.loads((directory / "environment.json").read_text())["compose_project"]


def revision_containers(directory: Path) -> list[str]:
    """List the environment's Cloud Run revision containers, running or not."""
    label = "label=gcp-local.environment=" + compose_project(directory)
    return subprocess.check_output([DOCKER, "ps", "-aq", "--filter", label], text=True).split()


def build_worker_image() -> str:
    """Build the example worker as a user image under a fresh local tag."""
    tag = "gcp-local-smoke/worker:" + secrets.token_hex(4)
    subprocess.run([DOCKER, "build", "--quiet", "-t", tag, str(REPO / "examples/workflow")],
                   check=True, capture_output=True, text=True, timeout=600)
    return tag


def assert_revision_container(directory: Path, image: str) -> None:
    """Require one revision container on the environment network with no host access."""
    containers = revision_containers(directory)
    assert len(containers) == 1, "expected one revision container, found " + str(containers)
    state = json.loads(subprocess.check_output([DOCKER, "inspect", containers[0]], text=True))[0]
    assert state["Config"]["Image"] == image
    assert list(state["NetworkSettings"]["Networks"]) == [compose_project(directory) + "_backend"]
    assert not state["HostConfig"].get("Binds"), state["HostConfig"]["Binds"]
    assert not state["Mounts"], state["Mounts"]
    assert not state["HostConfig"]["Privileged"]
    assert not state["HostConfig"].get("PortBindings"), "a revision published a host port"
    names = {entry.split("=", 1)[0] for entry in state["Config"]["Env"]}
    # Declared env, the Cloud Run variables, the metadata host, and the image's own.
    assert names <= {"K_SERVICE", "K_REVISION", "K_CONFIGURATION", "PORT", "GCE_METADATA_HOST",
                     "GCP_LOCAL_STORAGE_ENDPOINT", "PATH", "LANG", "GPG_KEY",
                     "PYTHON_VERSION", "PYTHON_SHA256"}, names
    print("the worker revision runs as a labeled container on the environment network")


def deploy_neighbor(directory: Path, image: str) -> None:
    """Deploy one revision in a second Docker-mode environment through the REST API."""
    env = json.loads(environment(directory, "env"))
    credentials = packaged_credentials(env["GOOGLE_APPLICATION_CREDENTIALS"])
    local = Local(env["GCP_LOCAL_ENDPOINT"], credentials.token)
    scope = "/v2/projects/" + env["GOOGLE_CLOUD_PROJECT"] + "/locations/" + REGION
    body = {"template": {"containers": [{"image": image}]}}
    request = urllib.request.Request(  # noqa: S310 -- the environment's loopback endpoint
        local.base + scope + "/services?serviceId=neighbor", method="POST",
        data=json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + local.token, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310 -- loopback
        operation = json.loads(response.read())
    end = time.time() + 60
    while not operation.get("done"):
        assert time.time() < end, "the neighbor rollout did not finish"
        time.sleep(0.5)
        _, payload = api(local, "GET", "/v2/" + operation["name"])
        operation = json.loads(payload)
    assert "error" not in operation, operation
    assert len(revision_containers(directory)) == 1


def packaged_run(directory: Path, workdir: Path, image: str = WORKER_IMAGE,
                 neighbor: Path | None = None) -> None:
    """Start the packaged environment, run the workflow, reset, and verify it is empty.

    With a neighbor, the environment runs in Cloud Run Docker mode and the
    neighbor's revision container must survive this environment's reset.
    """
    port = free_ports(19)  # HTTP, gRPC, the data ports, and gRPC TLS
    mode = ("--cloud-run-docker",) if neighbor else ()
    print(environment(directory, "up", *mode, "--port", str(port), "--project", PROJECT,
                      "--workload", WORKER_EMAIL.split("@")[0]).strip().splitlines()[-1])
    env = json.loads(environment(directory, "env"))
    base = env["GCP_LOCAL_ENDPOINT"]
    container = compose_project(directory) + "-gcp-local-1"
    credentials = packaged_credentials(env["GOOGLE_APPLICATION_CREDENTIALS"])
    terraform = Terraform(workdir, {
        "endpoint": base, "credentials_file": env["GCP_LOCAL_TERRAFORM_CREDENTIALS"],
        "project": PROJECT, "project_number": env["GCP_LOCAL_PROJECT_NUMBER"],
        "region": REGION, "bucket": BUCKET, "worker_image": image,
        # A process revision runs inside the core container, where the core
        # listens on the same port the host sees; a revision container dials
        # the core by its name on the environment network.
        "worker_storage_endpoint": env.get("GCP_LOCAL_RUN_ENDPOINT", base) + "/storage/v1",
    })
    provision(terraform, Path(env["GOOGLE_APPLICATION_CREDENTIALS"]))
    exercise(terraform, credentials, Local(base, credentials.token),
             env["GCP_LOCAL_GRPC_ENDPOINT"])
    if neighbor:
        assert_revision_container(directory, image)
        assert not packaged_workers(container), "a revision ran as a process in Docker mode"
    else:
        assert packaged_workers(container), "the worker revision is not running in the core"
        print("the worker revision runs as a process inside the packaged core container")
    # No destroy: reset alone must discard everything the workflow created.
    environment(directory, "reset")
    credentials = packaged_credentials(env["GOOGLE_APPLICATION_CREDENTIALS"])
    assert_empty(Local(base, credentials.token), after="reset")
    assert not packaged_workers(container), "a worker revision survived reset"
    assert not revision_containers(directory), "a revision container survived reset"
    print("after reset: no revision process or container remains")
    if neighbor:
        assert len(revision_containers(neighbor)) == 1, (
            "reset removed another environment's revision")
        print("after reset: the other environment's revision container still runs")


def down(directory: Path) -> str | None:
    """Stop one environment and return its Compose project, if it was created."""
    result = subprocess.run(
        [sys.executable, str(REPO / "scripts/dev/local_env.py"), "--directory",
         str(directory), "down"], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    return compose_project(directory) if (directory / "environment.json").exists() else None


def assert_removed(project: str) -> None:
    """Require no Compose container and no revision container of one environment."""
    for label in ("com.docker.compose.project=", "gcp-local.environment="):
        remaining = subprocess.check_output(
            [DOCKER, "ps", "-aq", "--filter", "label=" + label + project], text=True)
        assert not remaining.strip(), "down left a container labeled " + label + project


def packaged(*, cloud_run_docker: bool) -> int:
    """Run the workflow against the packaged environment, then reset and tear it down."""
    subprocess.run([sys.executable, str(REPO / "scripts/install/terraform.py")],
                   check=True, capture_output=True, text=True)
    assert TERRAFORM.is_file(), "The pinned Terraform is missing"
    image = build_worker_image() if cloud_run_docker else WORKER_IMAGE
    with tempfile.TemporaryDirectory(prefix="gcp-local-packaged-") as temporary:
        root = Path(temporary).resolve()
        root.chmod(stat.S_IRWXU)
        directory = root / "environment"
        neighbor = root / "neighbor" if cloud_run_docker else None
        workdir = root / "terraform"
        workdir.mkdir()
        projects = []
        try:
            if neighbor:
                environment(neighbor, "up", "--cloud-run-docker", "--port",
                            str(free_ports(19)))
                deploy_neighbor(neighbor, image)
            packaged_run(directory, workdir, image, neighbor)
        finally:
            projects.append(down(directory))
            if neighbor:
                assert len(revision_containers(neighbor)) == 1, (
                    "down removed another environment's revision")
                projects.append(down(neighbor))
            if cloud_run_docker:
                subprocess.run([DOCKER, "rmi", image], capture_output=True, check=False)
        for project in filter(None, projects):
            assert_removed(project)
    print("after down: no packaged or revision container remains")
    print("packaged workflow acceptance passed")
    return 0


def main() -> int:
    """Run the Terraform-driven workflow acceptance and clean everything up."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packaged", action="store_true",
                        help="run against the packaged Docker environment "
                             "(scripts/dev/local_env.py)")
    parser.add_argument("--cloud-run-docker", action="store_true",
                        help="with --packaged: run a user-built worker image as a container")
    args = parser.parse_args()
    if args.cloud_run_docker and not args.packaged:
        parser.error("--cloud-run-docker needs --packaged")
    if args.packaged:
        return packaged(cloud_run_docker=args.cloud_run_docker)
    binary, java = install_tools()
    secrets_dir = Path(tempfile.mkdtemp(prefix="gcp-local-workflow-"))
    identity_path, adc, terraform_adc = write_identity(secrets_dir)
    # The process revision runtime runs the worker source directly and reaches
    # the core over loopback. Container execution is covered separately by
    # scripts/smoke/cloudrun.py; this acceptance targets the resource chain.
    core = start_core(binary, write_iam(secrets_dir), identity_path, port=SERVE_SOCKET,
                      services="storage,pubsub,run,iam", pubsub_java=java,
                      pubsub_jar=str(REPO / ".tools/pubsub/emulator.jar"),
                      extra_flags=("--run-process",
                                   WORKER_IMAGE + "=" + WORKER_PYTHON + " " + WORKER_SOURCE))
    try:
        credentials, _ = google.auth.load_credentials_from_file(
            str(adc), scopes=["https://www.googleapis.com/auth/cloud-platform"])
        credentials.refresh(Request())
        with tempfile.TemporaryDirectory(prefix="gcp-local-workflow-tf-") as workdir:
            terraform = Terraform(Path(workdir), {
                "endpoint": BASE, "credentials_file": str(terraform_adc), "project": PROJECT,
                "project_number": PROJECT_NUMBER, "region": REGION, "bucket": BUCKET,
                "worker_image": WORKER_IMAGE, "worker_storage_endpoint": BASE + "/storage/v1",
            })
            run_workflow(terraform, credentials, adc)
        assert_no_revisions()
    finally:
        core.terminate()
        core.wait(timeout=10)
        shutil.rmtree(secrets_dir, ignore_errors=True)
        # The process runtime does not stop revisions when the core exits;
        # after a failed run, do not leave the worker running.
        subprocess.run([PKILL, "-f", WORKER_PROCESS], check=False)
    print("workflow acceptance passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
