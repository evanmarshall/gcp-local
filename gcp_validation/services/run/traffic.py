"""Bounded Cloud Run v2 traffic recording: splits, tags, tag URLs, scaling, and ingress.

One owned service runs Google's sample image. Its revisions are split by
percent, tagged, invoked through the service URL and a tag URL with the
allowed identity's ID token, scaled, and put behind internal ingress, which
the recording host reaches as an external caller. ID tokens stay in memory.
"""

import json
import re
import time
from collections.abc import Callable
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from gcp_validation.errors import detailed
from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import Observations, Response
from gcp_validation.services.run.service import ServicesFixture
from gcp_validation.services.storage.client import NoRedirects

CREDENTIALS_ENDPOINT = "https://iamcredentials.googleapis.com"
LATEST = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
REVISION = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"


def fetch(uri: str, token: str | None = None) -> tuple[int, str]:
    """GET one service or tag URL directly, with no proxy and no redirects."""
    if not uri.startswith(("https://", "http://127.0.0.1:")):
        raise ValueError("Invoke only the service's own run.app or loopback URL")
    headers = {"Authorization": "Bearer " + token} if token else {}
    opener = build_opener(ProxyHandler({}), NoRedirects())
    try:
        # The scheme is checked above.
        with opener.open(Request(uri + "/", headers=headers), timeout=30) as response:  # noqa: S310
            return response.status, response.read(1 << 16).decode(errors="replace")
    except HTTPError as error:
        return error.code, ""
    except (URLError, TimeoutError, OSError):
        raise Inconclusive("Invocation transport failed") from None


class TrafficRun:
    """The scenario's state: the fixture, its observations, and minted tokens."""

    def __init__(self, fixture: ServicesFixture, snapshot: object) -> None:
        """Alias the run's identifiers; revisions get aliases as they appear."""
        client = fixture.client
        self.fixture, self.snapshot = fixture, snapshot
        self.service = fixture.first
        self.path = fixture.service_path(self.service)
        self.observe = Observations(
            {
                fixture.stem: "<run>",
                fixture.project: "<project>",
                client.number: "<project-number>",
                **{email: "<" + actor + ">" for actor, email in client.emails.items()},
            }
        )
        self.revisions: list[str] = []
        self.tokens: dict[str, str] = {}

    def match(self, name: str, value: object) -> None:
        """Record one observation."""
        self.snapshot.match(name, value)

    def get(self) -> dict:
        """Read the owned service."""
        return self.fixture.call("GET", self.path).json()

    def learn(self, service: dict) -> None:
        """Alias the newest created revision's full name, GCP suffix included."""
        name = service.get("latestCreatedRevision", "").rsplit("/", 1)[-1]
        if name and name not in self.revisions:
            self.revisions.append(name)
            self.observe.aliases[name] = f"<rev{len(self.revisions)}>"

    def body(self, traffic: list[dict], *, env: dict | None = None, **extra: object) -> dict:
        """Return a full service body: the sample image as the allowed identity."""
        body = self.fixture.body(env)
        body["template"].update(extra.pop("template", {}))
        return {**body, "traffic": traffic, **extra}

    def patch(self, body: dict) -> Response:
        """Replace the owned service."""
        return self.fixture.call("PATCH", self.path, body=body)

    def rollout(self, body: dict) -> dict:
        """Replace the service, wait for its operation, and return the service."""
        response = self.patch(body)
        if response.status != 200:
            raise Inconclusive(f"Rollout returned HTTP {response.status}")
        self.fixture.wait(response.json().get("name", ""))
        service = self.get()
        self.learn(service)
        return service

    def settled(self) -> dict:
        """Read the service once it is no longer reconciling."""
        deadline = time.monotonic() + (300 if self.fixture.client.sandbox else 60)
        while True:
            service = self.get()
            if not service.get("reconciling") or time.monotonic() >= deadline:
                return service
            time.sleep(2)

    def refusal(self, body: dict) -> dict:
        """Observe an update GCP should refuse, synchronously or in its operation."""
        response = self.patch(body)
        if response.status != 200:
            return detailed(self.observe, response)
        done = self.fixture.wait(response.json().get("name", "")).json()
        error = done.get("error", {})
        return {
            "http_status": 200,
            "operation_code": error.get("code"),
            "operation_message": self.observe.text(error.get("message", "")),
        }

    def deletion(self, revision: str) -> dict:
        """Delete one revision; select the refusal, or the operation's outcome."""
        response = self.fixture.call("DELETE", self.path + "/revisions/" + revision)
        if response.status != 200:
            return detailed(self.observe, response)
        done = self.fixture.wait(response.json().get("name", "")).json()
        return {"http_status": 200, "done": done.get("done", False), "error": done.get("error")}

    def traffic_view(self, service: dict) -> dict:
        """Select the traffic spec, its statuses, and which revisions exist."""

        def alias(name: str) -> str:
            return self.observe.text(name.rsplit("/", 1)[-1]) if name else ""

        statuses = []
        for entry in service.get("trafficStatuses", []):
            tag, uri = entry.get("tag", ""), entry.get("uri", "")
            statuses.append(
                {
                    "type": entry.get("type"),
                    "percent": entry.get("percent"),
                    "revision": alias(entry.get("revision", "")),
                    "tag": tag,
                    "uri_is_tag_url": bool(tag)
                    and bool(re.search("/" + re.escape(tag + "---" + self.service), uri)),
                    "fields": sorted(entry),
                }
            )
        return {
            "generation": service.get("generation"),
            "traffic": [
                {**entry, "revision": alias(entry["revision"])} if "revision" in entry else entry
                for entry in service.get("traffic", [])
            ],
            "traffic_statuses": statuses,
            "latest_created": alias(service.get("latestCreatedRevision", "")),
            "latest_ready": alias(service.get("latestReadyRevision", "")),
            "terminal_state": service.get("terminalCondition", {}).get("state"),
            "ingress": service.get("ingress"),
        }

    def token(self, audience: str) -> str:
        """Mint the allowed identity's ID token for one audience, kept in memory."""
        if audience not in self.tokens:
            client = self.fixture.client
            live = client.target.mode != "local"
            endpoint = CREDENTIALS_ENDPOINT if live else client.endpoint
            email = client.emails["allowed"]
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + client.token("allowed"),
            }
            if live:
                headers["x-goog-user-project"] = client.target.project
            # The URL is the fixed https endpoint or the selected loopback endpoint.
            request = Request(  # noqa: S310
                f"{endpoint}/v1/projects/-/serviceAccounts/{email}:generateIdToken",
                data=json.dumps({"audience": audience, "includeEmail": True}).encode(),
                headers=headers,
                method="POST",
            )
            try:
                with client.opener.open(request, timeout=30) as response:
                    self.tokens[audience] = json.loads(response.read(1 << 16))["token"]
            except (HTTPError, URLError, TimeoutError, OSError, KeyError, ValueError):
                raise Inconclusive("Could not mint the allowed identity's ID token") from None
        return self.tokens[audience]

    def until(self, probe: Callable[[], tuple[int, str]], done: Callable[[int], bool]) -> tuple:
        """Repeat a probe until done, for IAM and routing propagation; live only."""
        seconds = 240 if self.fixture.client.sandbox else 0
        deadline = time.monotonic() + seconds
        while True:
            result = probe()
            if done(result[0]) or time.monotonic() >= deadline:
                return result
            time.sleep(10)

    def invoke_view(self, uri: str, audience: str | None, *, retry: bool = True) -> dict:
        """Invoke one URL; select the status and which known revision answered."""
        token = self.token(audience) if audience else None
        probe = lambda: fetch(uri, token)  # noqa: E731 -- one retried call
        status, text = self.until(probe, lambda code: code == 200) if retry else probe()
        answered = [self.observe.aliases[name] for name in self.revisions if name in text]
        return {"http_status": status, "answered_by": answered}


def grant_invoker(run: TrafficRun) -> None:
    """Grant the allowed identity run.invoker on the owned service only."""
    member = "serviceAccount:" + run.fixture.client.emails["allowed"]
    policy = {"bindings": [{"role": "roles/run.invoker", "members": [member]}]}
    written = run.fixture.call("POST", run.path + ":setIamPolicy", body={"policy": policy})
    run.match("invoker-granted", {"http_status": written.status})


def observe_split(run: TrafficRun) -> tuple[str, str]:
    """Split 50/50 between the first revision (tagged) and the latest, then retag."""
    first = run.revisions[0]
    split = run.rollout(
        run.body(
            [
                {"type": REVISION, "revision": first, "percent": 50, "tag": "blue"},
                {"type": LATEST, "percent": 50},
            ],
            env={"MODE": "second"},
        )
    )
    run.match("split", run.traffic_view(split))
    # Not the latest created revision, but serving half the traffic.
    run.match("delete-split-revision", run.deletion(first))
    second = run.revisions[-1]
    # Traffic alone changes: no new revision.
    moved = run.rollout(
        run.body(
            [
                {"type": REVISION, "revision": second, "percent": 100},
                {"type": REVISION, "revision": first, "tag": "blue"},
                {"type": REVISION, "revision": second, "tag": "green"},
            ],
            env={"MODE": "second"},
        )
    )
    run.match("retagged", run.traffic_view(moved))
    return split.get("uri", ""), next(
        entry.get("uri", "") for entry in moved.get("trafficStatuses", []) if entry.get("tag")
    )


def observe_invocation(run: TrafficRun, uri: str, tag_uri: str) -> None:
    """Invoke the service URL and the tag URL with ID tokens for each audience."""
    run.match("invoke-service", run.invoke_view(uri, uri))
    run.match("invoke-tag", run.invoke_view(tag_uri, tag_uri))
    run.match("invoke-tag-service-audience", run.invoke_view(tag_uri, uri, retry=False))
    run.match("invoke-tag-anonymous", run.invoke_view(tag_uri, None, retry=False))


def observe_validation(run: TrafficRun) -> None:
    """Observe refused traffic blocks, scaling values, and serving revision deletes."""
    first, second = run.revisions[0], run.revisions[1]
    env = {"MODE": "second"}
    run.match("delete-latest-revision", run.deletion(second))
    # Tagged, serving no percent, and still named by the service's traffic.
    run.match("delete-tagged-revision", run.deletion(first))
    run.match("after-tagged-delete", run.traffic_view(run.settled()))
    cases = {
        "traffic-sum-90": [
            {"type": REVISION, "revision": first, "percent": 40},
            {"type": LATEST, "percent": 50},
        ],
        "traffic-unknown-revision": [
            {"type": REVISION, "revision": run.service + "-99999-zzz", "percent": 100}
        ],
        "traffic-duplicate-tag": [
            {"type": REVISION, "revision": first, "percent": 50, "tag": "same"},
            {"type": REVISION, "revision": second, "percent": 50, "tag": "same"},
        ],
    }
    for name, traffic in cases.items():
        run.match(name, run.refusal(run.body(traffic, env=env)))
    keep = [{"type": REVISION, "revision": second, "percent": 100}]
    run.match(
        "scaling-min-above-max",
        run.refusal(
            run.body(
                keep, env=env, template={"scaling": {"minInstanceCount": 3, "maxInstanceCount": 2}}
            )
        ),
    )
    run.match(
        "concurrency-too-high",
        run.refusal(run.body(keep, env=env, template={"maxInstanceRequestConcurrency": 1001})),
    )
    run.match("after-refusals", run.traffic_view(run.get()))


def observe_scaling(run: TrafficRun, created: dict) -> None:
    """Observe the template's scaling defaults, then set them and read them back."""

    def scaling(template: dict) -> dict:
        return {
            "scaling": template.get("scaling"),
            "max_instance_request_concurrency": template.get("maxInstanceRequestConcurrency"),
        }

    run.match("scaling-defaults", scaling(created.get("template", {})))
    scaled = run.rollout(
        run.body(
            [{"type": LATEST, "percent": 100}],
            env={"MODE": "third"},
            template={"scaling": {"maxInstanceCount": 2}, "maxInstanceRequestConcurrency": 10},
        )
    )
    revision = run.fixture.call("GET", run.path + "/revisions/" + run.revisions[-1]).json()
    run.match(
        "scaled",
        {
            **run.traffic_view(scaled),
            **scaling(scaled.get("template", {})),
            "revision": scaling(revision),
        },
    )
    listed = run.fixture.call("GET", run.path + "/revisions").json().get("revisions", [])
    names = [item.get("name", "").rsplit("/", 1)[-1] for item in listed]
    # GCP suffixes revision names (-00003-abc); gcp-local doesn't.
    run.match(
        "revisions",
        {
            "aliases": [run.observe.text(name) for name in names],
            "numbers": [re.sub(r"^.*-(\d{5})(-[a-z0-9]{3})?$", r"\1", name) for name in names],
        },
    )


def observe_ingress(run: TrafficRun, uri: str) -> None:
    """Restrict ingress; the recording host is an external caller of the run.app URL."""
    labels = {
        "INGRESS_TRAFFIC_INTERNAL_ONLY": "ingress-internal-only",
        "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER": "ingress-internal-load-balancer",
    }
    template = {"scaling": {"maxInstanceCount": 2}, "maxInstanceRequestConcurrency": 10}
    token = run.token(uri)
    for ingress, label in labels.items():
        body = run.body(
            [{"type": LATEST, "percent": 100}],
            env={"MODE": "third"},
            template=template,
            ingress=ingress,
        )
        service = run.rollout(body)
        status, _ = run.until(lambda: fetch(uri, token), lambda code: code != 200)
        anonymous, _ = fetch(uri)
        run.match(
            label,
            {**run.traffic_view(service), "invoke_status": status, "anonymous_status": anonymous},
        )


def run_scenario(fixture: ServicesFixture, snapshot: object, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    run = TrafficRun(fixture, snapshot)
    fixture.wait(created.json().get("name", ""))
    service = run.get()
    run.learn(service)
    run.match("created", run.traffic_view(service))
    grant_invoker(run)
    uri, tag_uri = observe_split(run)
    observe_invocation(run, uri, tag_uri)
    observe_validation(run)
    observe_scaling(run, service)
    observe_ingress(run, uri)
