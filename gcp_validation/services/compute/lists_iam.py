"""Compute list options, operations methods, and subnetwork IAM policy versions and conditions.

One network, one subnetwork, three firewall rules, and one e2-micro VM without an
external address exist for a few minutes. The subnetwork's own policy gets one
conditional binding for the allowed caller; it is deleted with the subnetwork.
"""

import argparse
import time
from pathlib import Path
from typing import Protocol

from gcp_validation.config import Target
from gcp_validation.evidence import check_observation
from gcp_validation.live_rest import LiveScope, Observations, Response
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.compute.networking import clients
from gcp_validation.services.compute.vm_connectivity import REGION, ZONE, VmFixture

SERVICE = "computelists"
LIMITS = {
    "max_networks": 1,
    "max_subnetworks": 6,
    "max_firewalls": 3,
    "max_instances": 1,
    "max_policy_writes": 6,
    "max_run_seconds": 1500,
}
# Subnetworks with one networkViewer grant each, to tell which IAM Conditions
# resource attributes Compute supplies: "none" is unconditional, "time"
# reads no resource attribute.
GRANTS = ("none", "time", "service", "type", "name")
FIREWALLS = ("fw-b", "fw-c", "fw-a")  # created out of name order
OK = 200


class Snapshot(Protocol):
    """The snapshot fixture's matching interface."""

    def match(self, name: str, value: object) -> None:
        """Record or compare one observation."""


def load_scope(filename: str | None, sandbox: Sandbox | None, mode: str) -> LiveScope:
    """Load the owner-approved scope for this scenario's fixed limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class ListFixture(VmFixture):
    """The owned network resources and VM, created in setup, deleted in reverse."""

    def subnet_path(self) -> str:
        """Return the scenario subnetwork's path."""
        return f"{self.region_path}/subnetworks/{self.name('s')}"

    def setup(self) -> dict:
        """Create the network, subnetwork, firewalls, and VM; return each insert's operation."""
        operations = {"network": super().setup().json()}
        operations["subnetwork"] = self.insert(
            "subnetworks",
            "subnetworks",
            {
                "name": self.name("s"),
                "network": self.network_url("a"),
                "ipCidrRange": "10.30.0.0/24",
                "region": REGION,
            },
        ).json()
        for index, variant in enumerate(GRANTS, start=1):
            self.insert(
                "subnetworks",
                "subnetworks",
                {
                    "name": self.name("g-" + variant),
                    "network": self.network_url("a"),
                    "ipCidrRange": f"10.31.{index}.0/24",
                    "region": REGION,
                },
            )
        for suffix in FIREWALLS:
            self.insert(
                "firewalls",
                "firewalls",
                {
                    "name": self.name(suffix),
                    "network": self.network_url("a"),
                    "allowed": [{"IPProtocol": "tcp", "ports": ["22"]}],
                    "sourceRanges": ["10.0.0.0/8"],
                },
            )
            time.sleep(1 if self.client.sandbox else 0)  # distinct creation timestamps
        self.manifest["created"].append(["instances", self.name("vm")])
        self.save()
        body = {
            "name": self.name("vm"),
            "machineType": f"zones/{ZONE}/machineTypes/e2-micro",
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {
                        "sourceImage": "projects/debian-cloud/global/images/family/debian-12",
                        "diskSizeGb": "10",
                    },
                }
            ],
            "networkInterfaces": [{"subnetwork": self.subnet_path().removeprefix("/compute/v1/")}],
            "serviceAccounts": [],
            "labels": {"env": "gl"},
        }
        operations["instance"] = self.wait(
            self.client.request(
                "fixture",
                "POST",
                f"{self.zone_path}/instances",
                body=body,
                timeout=self.remaining(),
            )
        ).json()
        return operations


def listed(fixture: ListFixture, response: Response) -> dict:
    """Select the run's names in list order, whether a next page exists, or the error."""
    value = response.json()
    if response.status != OK:
        error = value.get("error", {})
        return {
            "http_status": response.status,
            "code": error.get("code"),
            "reasons": [e.get("reason") for e in error.get("errors", [])],
            "message": error.get("message", "").replace(fixture.prefix, "<prefix>"),
        }
    names = [
        item["name"].replace(fixture.prefix, "<prefix>")
        for item in value.get("items", [])
        if item["name"].startswith(fixture.prefix + "-")
    ]
    return {
        "http_status": response.status,
        "kind": value.get("kind"),
        "names": names,
        "next_page": bool(value.get("nextPageToken")),
    }


def lists(fixture: ListFixture, snapshot: Snapshot) -> None:
    """Firewall and instance list filters, order, and paging."""

    def firewalls(**query: str) -> Response:
        return fixture.client.request(
            "fixture",
            "GET",
            f"{fixture.global_path}/firewalls",
            query=query,
            timeout=fixture.remaining(),
        )

    ours = f"name eq {fixture.prefix}-.*"
    for name, query in (
        ("filter-regex", {"filter": ours}),
        ("filter-regex-quoted", {"filter": f'name eq "{fixture.prefix}-fw-.*"'}),
        ("filter-regex-ne", {"filter": "name ne .*-fw-b"}),
        ("filter-equal", {"filter": f'name = "{fixture.prefix}-fw-b"'}),
        ("filter-equal-bare", {"filter": f"name = {fixture.prefix}-fw-b"}),
        ("filter-not-equal", {"filter": f'name != "{fixture.prefix}-fw-b"'}),
        (
            "filter-and",
            {"filter": f'(name != "{fixture.prefix}-fw-a") (name != "{fixture.prefix}-fw-c")'},
        ),
        ("order-name-unfiltered", {"orderBy": "name"}),
        ("order-created-desc-unfiltered", {"orderBy": "creationTimestamp desc"}),
        ("order-created-asc-unfiltered", {"orderBy": "creationTimestamp"}),
        ("order-name-desc-unfiltered", {"orderBy": "name desc"}),
        ("order-description-unfiltered", {"orderBy": "description"}),
        ("order-name", {"filter": ours, "orderBy": "name"}),
        ("order-created-desc", {"filter": ours, "orderBy": "creationTimestamp desc"}),
        ("order-created-asc", {"filter": ours, "orderBy": "creationTimestamp"}),
        ("order-name-desc", {"filter": ours, "orderBy": "name desc"}),
        ("max-results-zero", {"filter": ours, "maxResults": "0"}),
        ("max-results-over", {"filter": ours, "maxResults": "501"}),
        ("max-results-negative", {"filter": ours, "maxResults": "-1"}),
        ("max-results-text", {"filter": ours, "maxResults": "two"}),
        ("page-token-unknown", {"filter": ours, "pageToken": "gcp-local-unknown"}),
    ):
        snapshot.match("firewalls-" + name, listed(fixture, firewalls(**query)))
    first = firewalls(filter=ours, maxResults="2")
    snapshot.match("firewalls-page-1", listed(fixture, first))
    token = first.json().get("nextPageToken", "")
    snapshot.match(
        "firewalls-page-2", listed(fixture, firewalls(filter=ours, maxResults="2", pageToken=token))
    )
    for name, expression in (
        ("label-equal", "labels.env = gl"),
        ("label-other-value", "labels.env = other"),
        ("label-not-equal", "labels.env != gl"),
        ("label-missing-not-equal", "labels.missing != x"),
    ):
        response = fixture.client.request(
            "fixture",
            "GET",
            f"{fixture.zone_path}/instances",
            query={"filter": expression},
            timeout=fixture.remaining(),
        )
        snapshot.match("instances-" + name, listed(fixture, response))


def operation_view(response: Response) -> dict:
    """Select an operation method's status and the operation's kind and state."""
    value = response.json()
    selected = {
        "http_status": response.status,
        "kind": value.get("kind"),
        "status": value.get("status"),
        "operationType": value.get("operationType"),
    }
    check_observation(selected)
    return selected


def operations(fixture: ListFixture, snapshot: Snapshot, created: dict) -> None:
    """List, wait, and delete one operation in each of the global, region, and zone scopes."""
    scopes = (
        ("global", fixture.global_path, created["network"]),
        ("region", fixture.region_path, created["subnetwork"]),
        ("zone", fixture.zone_path, created["instance"]),
    )
    for label, base, operation in scopes:
        name = operation["name"]
        listing = fixture.client.request(
            "fixture",
            "GET",
            f"{base}/operations",
            query={"filter": f'name = "{name}"'},
            timeout=fixture.remaining(),
        )
        items = listing.json().get("items", [])
        snapshot.match(
            f"{label}-operations-list",
            {
                "http_status": listing.status,
                "kind": listing.json().get("kind"),
                "items": [
                    {
                        "operationType": i.get("operationType"),
                        "status": i.get("status"),
                        "same_name": i.get("name") == name,
                    }
                    for i in items
                ],
            },
        )
        path = f"{base}/operations/{name}"
        waited = fixture.client.request(
            "fixture", "POST", f"{path}/wait", timeout=fixture.remaining()
        )
        snapshot.match(f"{label}-operation-wait", operation_view(waited))
        deleted = fixture.client.request("fixture", "DELETE", path, timeout=fixture.remaining())
        snapshot.match(
            f"{label}-operation-delete",
            {"http_status": deleted.status, "body": deleted.json() or None},
        )
        gone = fixture.client.request("fixture", "GET", path, timeout=fixture.remaining())
        snapshot.match(f"{label}-operation-after-delete", {"http_status": gone.status})


def policy_view(observe: Observations, response: Response) -> dict:
    """Select a subnetwork policy's version and bindings; a _withcond_ suffix is opaque."""
    value = response.json()
    if response.status != OK:
        error = value.get("error", {})
        return {
            "http_status": response.status,
            "code": error.get("code"),
            "reasons": [e.get("reason") for e in error.get("errors", [])],
        }
    bindings = []
    for binding in value.get("bindings", []):
        role, marker, suffix = binding["role"].partition("_withcond_")
        bindings.append(
            {
                "role": role,
                "members": [observe.text(m) for m in binding["members"]],
                "withcond_suffix": {
                    "length": len(suffix),
                    "hex": all(c in "0123456789abcdef" for c in suffix),
                }
                if marker
                else None,
                "condition": observe.value(binding.get("condition")),
            }
        )
    selected = {
        "http_status": response.status,
        "fields": sorted(value),
        "version": value.get("version"),
        "bindings": bindings,
    }
    check_observation(selected)
    return selected


def grant_binding(fixture: ListFixture, variant: str) -> dict:
    """Return one attribute grant's networkViewer binding for the allowed caller."""
    name = f"projects/{fixture.project}/regions/{REGION}/subnetworks/{fixture.name('g-' + variant)}"
    expression = {
        "time": 'request.time < timestamp("2100-01-01T00:00:00Z")',
        "service": 'resource.service == "compute.googleapis.com"',
        "type": 'resource.type == "compute.googleapis.com/Subnetwork"',
        "name": f'resource.name == "{name}"',
    }.get(variant)
    binding = {
        "role": "roles/compute.networkViewer",
        "members": ["serviceAccount:" + fixture.client.emails["allowed"]],
    }
    if expression:
        binding["condition"] = {"title": "gcp-local-" + variant, "expression": expression}
    return binding


def attribute_grants(fixture: ListFixture) -> dict:
    """Grant each attribute subnetwork, then poll the allowed caller's reads of all of them."""
    paths = {v: f"{fixture.region_path}/subnetworks/{fixture.name('g-' + v)}" for v in GRANTS}
    for variant, path in paths.items():
        written = fixture.client.request(
            "fixture",
            "POST",
            f"{path}/setIamPolicy",
            timeout=fixture.remaining(),
            body={"policy": {"version": 3, "bindings": [grant_binding(fixture, variant)]}},
        )
        if written.status != OK:
            raise ValueError(f"Grant {variant} returned HTTP {written.status}")
    statuses: dict[str, int] = {}
    deadline = time.monotonic() + (480 if fixture.client.sandbox else 0)
    while True:
        for variant, path in paths.items():
            if statuses.get(variant) != OK:
                read = fixture.client.request("allowed", "GET", path, timeout=fixture.remaining())
                statuses[variant] = read.status
        if all(code == OK for code in statuses.values()) or time.monotonic() >= deadline:
            return {variant: {"http_status": statuses[variant]} for variant in GRANTS}
        time.sleep(15)  # a new grant takes a while to apply


def subnetwork_policy(fixture: ListFixture, snapshot: Snapshot, observe: Observations) -> None:
    """Policy versions on the subnetwork, and what its conditional grant's attributes match."""
    path = fixture.subnet_path()

    def read(version: str | None) -> Response:
        query = {"optionsRequestedPolicyVersion": version} if version is not None else None
        return fixture.client.request(
            "fixture", "GET", f"{path}/getIamPolicy", query=query, timeout=fixture.remaining()
        )

    initial = read(None)
    snapshot.match("subnetwork-policy-initial", policy_view(observe, initial))
    snapshot.match("subnetwork-policy-version-2", policy_view(observe, read("2")))
    name = path.removeprefix("/compute/v1/")
    binding = {
        "role": "roles/compute.networkViewer",
        "members": ["serviceAccount:" + fixture.client.emails["allowed"]],
        "condition": {"title": "gcp-local-conformance", "expression": f'resource.name == "{name}"'},
    }
    written = fixture.client.request(
        "fixture",
        "POST",
        f"{path}/setIamPolicy",
        timeout=fixture.remaining(),
        body={"policy": {"version": 3, "bindings": [binding], "etag": initial.json().get("etag")}},
    )
    snapshot.match("subnetwork-policy-set-conditional", policy_view(observe, written))
    for label, version in (("v3", "3"), ("v1", "1"), ("default", None)):
        snapshot.match(f"subnetwork-policy-read-{label}", policy_view(observe, read(version)))
    snapshot.match("subnetwork-attribute-grants", attribute_grants(fixture))
    network = fixture.client.request(
        "allowed",
        "GET",
        f"{fixture.global_path}/networks/{fixture.name('a')}",
        timeout=fixture.remaining(),
    )
    snapshot.match("network-without-grant", {"http_status": network.status})


def run_scenario(fixture: ListFixture, snapshot: Snapshot, created: dict) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    client = fixture.client
    observe = Observations(
        {
            fixture.prefix: "<prefix>",
            fixture.project: "<project>",
            **{client.emails[a]: "<" + a + ">" for a in client.emails},
        }
    )
    snapshot.match(
        "inserts",
        {
            label: {"status": op.get("status"), "kind": op.get("kind")}
            for label, op in sorted(created.items())
        },
    )
    lists(fixture, snapshot)
    operations(fixture, snapshot, created)
    subnetwork_policy(fixture, snapshot, observe)


def build(
    target: Target,
    sandbox: Sandbox | None,
    scope: LiveScope | None,
    state_dir: Path,
    manifest: str | None = None,
) -> ListFixture:
    """Build the fixture with Compute and Network Management clients."""
    compute, management = clients(target, sandbox=sandbox, scope=scope)
    return ListFixture(compute, management, state_dir, manifest=manifest)


def main(argv: list[str] | None = None) -> None:
    """Delete only the owned resources after an interrupted run."""
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
    build(
        target,
        sandbox,
        scope,
        Path.home() / ".local/state/gcp-local" / sandbox.project / "networking",
        manifest=args.manifest,
    ).cleanup()
    print("Owned compute list cleanup passed.")  # noqa: T201 -- the CLI reports on stdout


if __name__ == "__main__":
    main()
