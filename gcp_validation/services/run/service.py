"""Bounded Cloud Run v2 service recording: deploy, read, page, roll out, invoke, delete."""

import argparse
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from gcp_validation.config import Target
from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive, now
from gcp_validation.live_paths import PrefixClient
from gcp_validation.live_rest import LiveScope, Observations, OwnedRun, Response
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.storage.client import NoRedirects

SERVICE = "run"
LIMITS = {"max_services": 4, "max_run_seconds": 1500}
LIVE_ENDPOINT = "https://run.googleapis.com"
LOCATION = "us-central1"
# Google's sample image; with no traffic the services scale to zero.
IMAGE = "us-docker.pkg.dev/cloudrun/container/hello"
SERVICE_TYPE = "type.googleapis.com/google.cloud.run.v2.Service"


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the owner-approved Cloud Run live scope."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class RunClient(PrefixClient):
    """The Cloud Run Admin API v2 transport."""

    prefix = "/v2"


def revision_id(name: str) -> str:
    """Drop GCP's random revision suffix: svc-00002-abc becomes svc-00002."""
    return re.sub(r"-(\d{5})-[a-z0-9]{3}$", r"-\1", name.rsplit("/", 1)[-1])


class ServicesFixture(OwnedRun):
    """Two services named for the run; every name is recorded before it is created."""

    def __init__(self, client: RunClient, state_dir: Path, *, manifest: str | None = None) -> None:
        """Bind the transport to one owned run."""
        super().__init__(
            client,
            state_dir,
            SERVICE,
            LIMITS["max_run_seconds"],
            manifest=manifest,
            check=lambda v: all(
                name.startswith("gl-" + v["run_id"][:10]) for name in v.get("services", [])
            ),
        )
        self.manifest.setdefault("services", [])
        stem = "gl-" + self.manifest["run_id"][:10]
        self.stem, self.first, self.second = stem, stem + "-b", stem + "-a"
        self.parent = f"/v2/projects/{self.project}/locations/{LOCATION}"

    def service_path(self, service: str, suffix: str = "") -> str:
        """Return the REST path of one owned service."""
        return f"{self.parent}/services/{service}{suffix}"

    def call(
        self,
        method: str,
        path: str,
        *,
        actor: str = "fixture",
        body: dict | None = None,
        query: dict | None = None,
    ) -> Response:
        """Send one request inside the run's deadline."""
        return self.client.request(
            actor, method, path, body=body, query=query, timeout=self.remaining()
        )

    def body(self, env: dict | None = None) -> dict:
        """Return a service body that runs the sample image as the allowed identity."""
        container = {"image": IMAGE}
        if env:
            container["env"] = [{"name": key, "value": value} for key, value in env.items()]
        return {
            "template": {"serviceAccount": self.client.emails["allowed"], "containers": [container]}
        }

    def create(self, service: str, body: dict, *, actor: str = "fixture") -> Response:
        """Record the service name, then create it; a refused creation leaves nothing."""
        if service not in self.manifest["services"]:
            if len(self.manifest["services"]) >= LIMITS["max_services"]:
                raise ValueError("Service limit reached")
            self.manifest["services"].append(service)
            self.save()
        return self.call(
            "POST", f"{self.parent}/services", actor=actor, body=body, query={"serviceId": service}
        )

    def wait(self, operation: str) -> Response:
        """Poll one operation until it is done."""
        deadline = time.monotonic() + (300 if self.client.sandbox else 60)
        while True:
            response = self.call("GET", "/v2/" + operation)
            if (
                response.status != 200
                or response.json().get("done")
                or time.monotonic() >= deadline
            ):
                return response
            time.sleep(2)

    def setup(self) -> Response:
        """Create the first service; the scenario creates the second."""
        self.acquire()
        self.client.verify_project()
        self.manifest["attempted"] = True
        self.save()  # Creation may succeed even if the response is lost.
        return self.create(self.first, self.body())

    def cleanup(self) -> None:
        """Delete every owned service, then confirm each reads 404."""
        if not self.manifest["attempted"]:
            self.release()
            return
        self.acquire()
        try:
            self.client.verify_project()
            for service in self.manifest["services"]:
                self.delete_owned(service)
            self.manifest.update(cleanup="passed", finished_at=now())
            self.save()
        finally:
            self.release()

    def delete_owned(self, service: str) -> None:
        """Delete one owned service and wait until it reads 404."""
        path = self.service_path(service)
        current = self.client.request("fixture", "GET", path)
        if current.status == 200:
            if not current.json().get("name", "").endswith("/services/" + service):
                raise ValueError("Refusing cleanup: ownership does not match the manifest")
            deleted = self.client.request("fixture", "DELETE", path)
            if deleted.status not in {200, 404}:
                raise ValueError(f"Owned service delete returned HTTP {deleted.status}")
        elif current.status != 404:
            raise ValueError(f"Owned service read returned HTTP {current.status}")
        deadline = time.monotonic() + 120
        while self.client.request("fixture", "GET", path).status != 404:
            if time.monotonic() >= deadline:
                raise ValueError("Owned service remains after deletion")
            time.sleep(3)


def invoke(uri: str, token: str | None = None) -> int:
    """GET the service URL directly, with no proxy and no redirects; return the status."""
    if not uri.startswith(("https://", "http://127.0.0.1:")):
        raise ValueError("Invoke only the service's own run.app or loopback URL")
    headers = {"Authorization": "Bearer " + token} if token else {}
    opener = build_opener(ProxyHandler({}), NoRedirects())
    try:
        with opener.open(Request(uri + "/", headers=headers), timeout=30) as response:  # noqa: S310 -- scheme checked above
            return response.status
    except HTTPError as error:
        return error.code
    except (URLError, TimeoutError, OSError):
        raise Inconclusive("Invocation transport failed") from None


def operation_view(response: Response) -> dict:
    """Select an operation's shape: done, and the resource its metadata carries."""
    value = response.json()
    metadata = value.get("metadata", {})
    return {
        "http_status": response.status,
        "fields": sorted(value),
        "done": value.get("done", False),
        "metadata_type": metadata.get("@type"),
        "metadata_generation": metadata.get("generation"),
        "metadata_reconciling": metadata.get("reconciling", False),
        "response_type": value.get("response", {}).get("@type"),
    }


def service_view(observe: Observations, response: Response) -> dict:
    """Select a service's identity, routing, readiness, and template."""
    value = response.json()
    container = value.get("template", {}).get("containers", [{}])[0]
    port = (container.get("ports") or [{}])[0]
    statuses = [
        {
            "type": entry.get("type"),
            "percent": entry.get("percent"),
            "revision": observe.text(revision_id(entry.get("revision", ""))),
        }
        for entry in value.get("trafficStatuses", [])
    ]
    return {
        "http_status": response.status,
        "name": observe.text(value.get("name", "")),
        "generation": value.get("generation"),
        "observed_generation": value.get("observedGeneration"),
        "creator": observe.text(value.get("creator", "")),
        "ingress": value.get("ingress"),
        "launch_stage": value.get("launchStage"),
        "traffic": value.get("traffic"),
        "traffic_statuses": statuses,
        "reconciling": value.get("reconciling", False),
        "latest_ready": observe.text(revision_id(value.get("latestReadyRevision", ""))),
        "latest_ready_is_latest_created": value.get("latestReadyRevision")
        == value.get("latestCreatedRevision"),
        "ready_names_a_revision": value.get("latestReadyRevision", "").startswith(
            value.get("name", "?") + "/revisions/"
        ),
        "terminal_condition": {
            key: value.get("terminalCondition", {}).get(key) for key in ("type", "state")
        },
        "condition_types": sorted(entry.get("type") for entry in value.get("conditions", [])),
        "service_account": observe.text(value.get("template", {}).get("serviceAccount", "")),
        "image": container.get("image"),
        "env": container.get("env", []),
        "container_port": port.get("containerPort"),
        "port_name": port.get("name"),
        "has_uri": bool(value.get("uri")),
    }


def observe_lists(fixture: ServicesFixture, snapshot: object, observe: Observations) -> None:
    """Observe list order and paging over the two owned services, and refused page parameters."""
    path = f"{fixture.parent}/services"

    def listing(**query: str) -> Response:
        return fixture.call("GET", path, query=query or None)

    names = [
        item.get("name", "").rsplit("/", 1)[-1] for item in listing().json().get("services", [])
    ]
    first = listing(pageSize="1")
    token = first.json().get("nextPageToken", "")
    second = listing(pageSize="1", pageToken=token)
    negative = listing(pageSize="-1")
    snapshot.match(
        "list-paged",
        {
            "owned_order": [
                observe.text(name) for name in names if name in (fixture.first, fixture.second)
            ],
            "first_status": first.status,
            "first_count": len(first.json().get("services", [])),
            "first_has_next": bool(token),
            "second_status": second.status,
            "second_count": len(second.json().get("services", [])),
            "negative_size_status": negative.status,
            "negative_size_lists_owned": {fixture.first, fixture.second}
            <= {
                item.get("name", "").rsplit("/", 1)[-1]
                for item in negative.json().get("services", [])
            },
        },
    )
    snapshot.match("list-invalid-token", detailed(observe, listing(pageToken="not-a-page-token")))


def observe_denials(fixture: ServicesFixture, snapshot: object, observe: Observations) -> None:
    """Observe the denied caller's refusals, which carry the permission and resource."""
    snapshot.match(
        "denied-get",
        detailed(observe, fixture.call("GET", fixture.service_path(fixture.first), actor="denied")),
    )
    snapshot.match(
        "denied-get-missing",
        detailed(
            observe,
            fixture.call("GET", fixture.service_path(fixture.stem + "-none"), actor="denied"),
        ),
    )
    snapshot.match(
        "denied-list",
        detailed(observe, fixture.call("GET", f"{fixture.parent}/services", actor="denied")),
    )
    denied = fixture.create(fixture.stem + "-denied", fixture.body(), actor="denied")
    snapshot.match("denied-create", detailed(observe, denied))


def observe_rollout(
    fixture: ServicesFixture, snapshot: object, observe: Observations, created: str
) -> str:
    """Roll out a second revision; observe its operation, the revisions, and the operations list."""
    update = fixture.call(
        "PATCH", fixture.service_path(fixture.first), body=fixture.body({"MODE": "second"})
    )
    operation = update.json().get("name", "")
    observe.aliases[operation.rsplit("/", 1)[-1]] = "<update-operation>"
    snapshot.match(
        "update-operation", {**operation_view(update), "distinct_from_create": operation != created}
    )
    # A wait whose timeout passes before the rollout is done.
    short = fixture.call("POST", f"/v2/{operation}:wait", body={"timeout": "1s"})
    # Its metadata is a partial Service, so only the operation's shape is selected.
    snapshot.match(
        "update-wait-short",
        {
            "http_status": short.status,
            "fields": sorted(short.json()),
            "done": short.json().get("done", False),
        },
    )
    fixture.wait(operation)
    snapshot.match(
        "update-waited",
        operation_view(fixture.call("POST", f"/v2/{operation}:wait", body={"timeout": "20s"})),
    )
    snapshot.match(
        "updated", service_view(observe, fixture.call("GET", fixture.service_path(fixture.first)))
    )
    revisions = fixture.call("GET", fixture.service_path(fixture.first, "/revisions"))
    snapshot.match(
        "revisions",
        {
            "http_status": revisions.status,
            "names": [
                observe.text(revision_id(item.get("name", "")))
                for item in revisions.json().get("revisions", [])
            ],
        },
    )
    return operation


def observe_operations(
    fixture: ServicesFixture, snapshot: object, observe: Observations, owned: dict[str, str]
) -> None:
    """Observe the owned operations' list order and a denied operation read."""
    names, token = [], None
    for _ in range(20):
        page = fixture.call(
            "GET",
            f"{fixture.parent}/operations",
            query={"pageSize": "100", **({"pageToken": token} if token else {})},
        )
        names += [item.get("name", "") for item in page.json().get("operations", [])]
        token = page.json().get("nextPageToken")
        if not token:
            break
    snapshot.match(
        "operations-listed",
        {
            "http_status": page.status,
            "owned_order": [owned[name] for name in names if name in owned],
        },
    )
    update = next(name for name, label in owned.items() if label == "update-first")
    snapshot.match(
        "denied-operation-get",
        detailed(observe, fixture.call("GET", "/v2/" + update, actor="denied")),
    )


def observe_conditions(fixture: ServicesFixture, snapshot: object, observe: Observations) -> None:
    """Grant run.viewer on the first service under a request.time condition, then a resource one.

    The time condition grants. A condition on resource.type, resource.service,
    and resource.name of the same service grants nothing: Cloud Run supplies
    no resource attributes to its service policies' conditions.
    """
    name = f"projects/{fixture.project}/locations/{LOCATION}/services/{fixture.first}"
    path = fixture.service_path(fixture.first)
    snapshot.match(
        "policy-read", observe.select(fixture.call("GET", path + ":getIamPolicy"), ("bindings",))
    )

    def grant(title: str, expression: str) -> Response:
        policy = {
            "version": 3,
            "bindings": [
                {
                    "role": "roles/run.viewer",
                    "members": ["serviceAccount:" + fixture.client.emails["allowed"]],
                    "condition": {"title": title, "expression": expression},
                }
            ],
        }
        return fixture.call("POST", path + ":setIamPolicy", body={"policy": policy})

    written = grant("gl-time", 'request.time < timestamp("2100-01-01T00:00:00Z")')
    snapshot.match(
        "conditional-policy",
        {
            "http_status": written.status,
            "version": written.json().get("version"),
            "bindings": observe.value(written.json().get("bindings")),
        },
    )
    read = lambda: fixture.call("GET", path, actor="allowed")  # noqa: E731 -- one retried read
    allowed = fixture.wait_until(read, {403}, 300)
    snapshot.match(
        "time-condition",
        {"http_status": allowed.status, "name": observe.text(allowed.json().get("name", ""))},
    )
    grant(
        "gl-resource",
        f'resource.type == "run.googleapis.com/Service" && '
        f'resource.service == "run.googleapis.com" && resource.name == "{name}"',
    )
    snapshot.match("resource-condition", detailed(observe, fixture.wait_until(read, {200}, 300)))


def observe_decoding(fixture: ServicesFixture, snapshot: object, observe: Observations) -> None:
    """Observe refused bodies: an unknown name and a mistyped value; neither creates anything."""
    service = fixture.stem + "-x"
    unknown = fixture.create(service, {**fixture.body(), "unknownField": "x"})
    snapshot.match("unknown-field", detailed(observe, unknown))
    mistyped = {
        "template": {"containers": [{"image": IMAGE, "ports": [{"containerPort": "many"}]}]}
    }
    snapshot.match("mistyped-field", detailed(observe, fixture.create(service, mistyped)))


def observe_delete(
    fixture: ServicesFixture, snapshot: object, observe: Observations, operation: str
) -> None:
    """Delete the second service, then delete one finished operation."""
    deleted = fixture.call("DELETE", fixture.service_path(fixture.second))
    snapshot.match(
        "delete-operation",
        {
            **operation_view(deleted),
            "metadata_has_delete_time": "deleteTime" in deleted.json().get("metadata", {}),
        },
    )
    snapshot.match("delete-waited", operation_view(fixture.wait(deleted.json().get("name", ""))))
    snapshot.match(
        "absent", observe.error(fixture.call("GET", fixture.service_path(fixture.second)))
    )
    removed = fixture.call("DELETE", "/v2/" + operation)
    snapshot.match("operation-deleted", {"http_status": removed.status, "body": removed.json()})
    snapshot.match("operation-absent", observe.error(fixture.call("GET", "/v2/" + operation)))


def run_scenario(fixture: ServicesFixture, snapshot: object, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.stem: "<run>",
            fixture.project: "<project>",
            client.number: "<project-number>",
            **{email: "<" + actor + ">" for actor, email in client.emails.items()},
        }
    )
    first_operation = created.json().get("name", "")
    snapshot.match("create-operation", operation_view(created))
    snapshot.match("create-waited", operation_view(fixture.wait(first_operation)))
    second = fixture.create(fixture.second, fixture.body())
    second_operation = second.json().get("name", "")
    fixture.wait(second_operation)
    service = fixture.call("GET", fixture.service_path(fixture.first))
    snapshot.match("service", service_view(observe, service))
    uri = service.json().get("uri", "")
    snapshot.match("invoke-anonymous", {"http_status": invoke(uri)})
    snapshot.match("invoke-invalid-token", {"http_status": invoke(uri, "not-a-token")})
    observe_denials(fixture, snapshot, observe)
    observe_lists(fixture, snapshot, observe)
    update = observe_rollout(fixture, snapshot, observe, first_operation)
    observe_operations(
        fixture,
        snapshot,
        observe,
        {
            first_operation: "create-first",
            second_operation: "create-second",
            update: "update-first",
        },
    )
    observe_conditions(fixture, snapshot, observe)
    observe_decoding(fixture, snapshot, observe)
    observe_delete(fixture, snapshot, observe, second_operation)


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned services after an interrupted run."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("command", choices=["cleanup"])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--allow-live", action="store_true", required=True)
    args = parser.parse_args(argv)
    sandbox = Sandbox.load(args.profile)
    scope = load_scope(args.scope, sandbox, "cleanup")
    target = Target.select(
        "record",
        project=sandbox.project,
        credentials_file=sandbox.identities["allowed"].credentials_file,
        live_scope=sandbox.live_scope,
        allow_live=args.allow_live,
    )
    client = RunClient(target, LIVE_ENDPOINT, sandbox=sandbox, scope=scope)
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    ServicesFixture(client, state, manifest=args.manifest).cleanup()
    sys.stdout.write("Owned service cleanup passed.\n")


if __name__ == "__main__":
    main()
