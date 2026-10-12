"""Service-account and network-tag firewall scoping, judged by Connectivity Tests.

Two e2-micro VMs run for a few minutes; one run-named service account is created and
deleted. The run also records Compute's synchronous refusals of mistyped or
contradictory request bodies and the order Compute lists resources in.
"""

import argparse
import sys
import time
from http import HTTPStatus
from pathlib import Path

from gcp_validation.config import Target
from gcp_validation.evidence import check_observation, now
from gcp_validation.live_rest import LiveScope, Observations, Response, RestClient
from gcp_validation.sandbox import Sandbox
from gcp_validation.services.compute.networking import clients, operation, verdict
from gcp_validation.services.compute.vm_connectivity import REGION, ZONE, VmFixture

SERVICE = "vmserviceaccounts"
LIMITS = {
    "max_networks": 1,
    "max_subnetworks": 2,
    "max_firewalls": 21,
    "max_instances": 6,
    "max_service_accounts": 1,
    "max_connectivity_tests": 10,
    "max_run_seconds": 2400,
}
IAM = "https://iam.googleapis.com"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
IMAGE = "projects/debian-cloud/global/images/family/debian-12"
GONE = {HTTPStatus.OK, HTTPStatus.NOT_FOUND}


def load_scope(filename: str, sandbox: Sandbox, mode: str) -> LiveScope:
    """Load the owner-approved scope for this scenario's fixed limits."""
    return LiveScope.load(SERVICE, LIMITS, filename, sandbox, mode)


class AccountFixture(VmFixture):
    """Adds one run-named service account, deleted before the network resources."""

    def __init__(
        self,
        compute: RestClient,
        management: RestClient,
        accounts: RestClient,
        state_dir: Path,
        *,
        manifest: str | None = None,
    ) -> None:
        """Name the run's account; nothing is created until the scenario asks."""
        super().__init__(compute, management, state_dir, manifest=manifest)
        self.accounts = accounts
        self.account = f"{self.prefix}-sa@{self.project}.iam.gserviceaccount.com"
        self.accounts_path = f"/v1/projects/{self.project}/serviceAccounts"

    def create_account(self) -> Response:
        """Create the run's service account, recording ownership first."""
        self.manifest["created"].append(["serviceAccounts", self.account])
        self.save()
        return self.accounts.request(
            "fixture",
            "POST",
            self.accounts_path,
            timeout=self.remaining(),
            body={
                "accountId": self.prefix + "-sa",
                "serviceAccount": {
                    "displayName": "gcp-local conformance",
                    "description": self.manifest["run_id"],
                },
            },
        )

    def read_account(self) -> Response:
        """Read the run's account; a new account takes a while to appear."""
        return self.accounts.request(
            "fixture", "GET", f"{self.accounts_path}/{self.account}", timeout=self.remaining()
        )

    def grant_act_as(self) -> Response:
        """Let the fixture act as the run's account only; no project-level grant."""
        path = f"{self.accounts_path}/{self.account}"
        policy = self.accounts.request(
            "fixture", "POST", path + ":getIamPolicy", body={}, timeout=self.remaining()
        )
        binding = {
            "role": "roles/iam.serviceAccountUser",
            "members": ["serviceAccount:" + self.client.emails["fixture"]],
        }
        return self.accounts.request(
            "fixture",
            "POST",
            path + ":setIamPolicy",
            timeout=self.remaining(),
            body={"policy": {"etag": policy.json().get("etag", ""), "bindings": [binding]}},
        )

    def vm(
        self,
        suffix: str,
        *,
        account: str | None = None,
        tags: tuple[str, ...] = (),
        labels: object = None,
    ) -> Response:
        """Insert one e2-micro VM without an external address; the operation isn't awaited."""
        self.manifest["created"].append(["instances", self.name(suffix)])
        self.save()
        subnet = f"projects/{self.project}/regions/{REGION}/subnetworks/{self.name('a1')}"
        body = {
            "name": self.name(suffix),
            "machineType": f"zones/{ZONE}/machineTypes/e2-micro",
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {"sourceImage": IMAGE, "diskSizeGb": "10"},
                }
            ],
            "networkInterfaces": [{"subnetwork": subnet}],
            "serviceAccounts": [{"email": account, "scopes": SCOPES}] if account else [],
            "labels": {"managed-by": "gcp-local"} if labels is None else labels,
        }
        if tags:
            body["tags"] = {"items": list(tags)}
        return self.client.request(
            "fixture", "POST", f"{self.zone_path}/instances", body=body, timeout=self.remaining()
        )

    def firewall(self, suffix: str, body: dict) -> Response:
        """Insert a firewall body as given; the caller judges the outcome."""
        self.manifest["created"].append(["firewalls", self.name(suffix)])
        self.save()
        body = {"name": self.name(suffix), "network": self.network_url("a"), **body}
        return self.client.request(
            "fixture", "POST", f"{self.global_path}/firewalls", body=body, timeout=self.remaining()
        )

    def cleanup(self) -> None:
        """Delete the account, then the instances and network resources, even if it fails."""
        failure = None
        if self.manifest["attempted"]:
            self.acquire()
            try:
                self.delete_accounts()
            except ValueError as error:
                failure = error
            finally:
                self.release()
        super().cleanup()
        if failure is not None:
            self.manifest["cleanup"] = "pending"
            self.save()
            raise failure

    def delete_accounts(self) -> None:
        """Delete the owned account and wait until reads stop finding it."""
        for kind, email in self.manifest["created"]:
            if kind != "serviceAccounts":
                continue
            if email != self.account:
                raise ValueError("Refusing service-account cleanup outside the run prefix")
            path = f"{self.accounts_path}/{email}"
            deleted = self.accounts.request("fixture", "DELETE", path)
            if deleted.status not in GONE:
                raise ValueError(f"Owned service-account delete returned HTTP {deleted.status}")
            deadline = time.monotonic() + (120 if self.client.sandbox else 0)
            while self.accounts.request("fixture", "GET", path).status != HTTPStatus.NOT_FOUND:
                if time.monotonic() >= deadline:
                    raise ValueError("Owned service account remains after deletion")
                time.sleep(5)
        created = self.manifest["created"]
        self.manifest["created"] = [e for e in created if e[0] != "serviceAccounts"]
        self.save()


def outcome(fixture: AccountFixture, observe: Observations, response: Response) -> dict:
    """Select a synchronous error, or an accepted request's finished operation, with messages."""
    value = response.json()
    if response.status == HTTPStatus.OK and value.get("kind") == "compute#operation":
        done = fixture.wait(response).json()
        errors = done.get("error", {}).get("errors", [])
        result = {
            "http_status": response.status,
            "operation_status": done.get("status"),
            "operation_errors": [
                {"code": e.get("code"), "message": observe.text(e.get("message", ""))}
                for e in errors
            ],
        }
    else:
        error = value.get("error", {})
        result = {
            "http_status": response.status,
            "code": error.get("code"),
            "status": error.get("status"),
            "message": observe.text(error.get("message", "")),
            "errors": [
                {key: observe.text(str(item.get(key))) for key in sorted(item)}
                for item in error.get("errors", [])
            ],
            "detail_types": [d.get("@type") for d in error.get("details", [])],
        }
    check_observation(result)
    return result


def ours(fixture: AccountFixture, response: Response) -> dict:
    """Select the run's resource names in the order the list returned them."""
    names = [
        item["name"]
        for item in response.json().get("items", [])
        if item["name"].startswith(fixture.prefix + "-")
    ]
    return {
        "http_status": response.status,
        "names": [name.replace(fixture.prefix, "<prefix>") for name in names],
    }


def lists_and_regions(fixture: AccountFixture, snapshot: object, observe: Observations) -> None:
    """Create two subnetworks out of name order, then list and read them across regions."""
    client = fixture.client
    for suffix, cidr in (("a1", "10.10.0.0/24"), ("a0", "10.10.1.0/24")):
        fixture.insert(
            "subnetworks",
            "subnetworks",
            {
                "name": fixture.name(suffix),
                "network": fixture.network_url("a"),
                "ipCidrRange": cidr,
                "region": REGION,
            },
        )
    snapshot.match(
        "subnetwork-list",
        ours(fixture, client.request("fixture", "GET", f"{fixture.region_path}/subnetworks")),
    )
    other_region = fixture.region_path.rsplit("/", 1)[0] + "/us-east1"
    snapshot.match(
        "subnetwork-other-region-get",
        outcome(
            fixture,
            observe,
            client.request("fixture", "GET", f"{other_region}/subnetworks/{fixture.name('a1')}"),
        ),
    )
    snapshot.match(
        "subnetwork-other-region-list",
        ours(fixture, client.request("fixture", "GET", f"{other_region}/subnetworks")),
    )


def refusals(fixture: AccountFixture, snapshot: object, observe: Observations) -> None:
    """Send contradictory and mistyped firewall bodies on a port no verdict uses.

    Compute accepts some of them; each accepted rule is read back, then deleted
    so it can't take part in the verdicts.
    """
    account, ranges = [fixture.account], ["10.10.0.0/24"]
    allow = {"direction": "INGRESS", "allowed": [{"IPProtocol": "tcp", "ports": ["1111"]}]}
    accepted = {**allow, "sourceRanges": ranges}
    for suffix, body in (
        ("p-target-both", {**allow, "targetTags": ["gl-web"], "targetServiceAccounts": account}),
        ("p-source-both", {**allow, "sourceTags": ["gl-web"], "sourceServiceAccounts": account}),
        (
            "p-source-sa-target-tag",
            {**allow, "sourceServiceAccounts": account, "targetTags": ["gl-web"]},
        ),
        (
            "p-source-tag-target-sa",
            {**allow, "sourceTags": ["gl-web"], "targetServiceAccounts": account},
        ),
        (
            "p-egress-source-sa",
            {
                "direction": "EGRESS",
                "allowed": allow["allowed"],
                "destinationRanges": ranges,
                "sourceServiceAccounts": account,
            },
        ),
        (
            "p-egress-source-tag",
            {"direction": "EGRESS", "allowed": allow["allowed"], "sourceTags": ["gl-web"]},
        ),
        ("p-priority-text", {**accepted, "priority": "high"}),
        ("p-priority-numeric-text", {**accepted, "priority": "900"}),
        ("p-disabled-yes", {**accepted, "disabled": "yes"}),
        ("p-disabled-no", {**accepted, "disabled": "no"}),
        ("p-disabled-false-text", {**accepted, "disabled": "false"}),
        ("p-target-sa-text", {**accepted, "targetServiceAccounts": fixture.account}),
        ("p-ranges-object", {**allow, "sourceRanges": {"cidr": "10.10.0.0/24"}}),
        ("p-protocol-number", {**allow, "sourceRanges": ranges, "allowed": [{"IPProtocol": 6}]}),
        ("p-unknown-field", {**accepted, "bogusField": "x"}),
    ):
        response = fixture.firewall(suffix, body)
        result = outcome(fixture, observe, response)
        if response.status == HTTPStatus.OK:
            path = f"{fixture.global_path}/firewalls/{fixture.name(suffix)}"
            result["stored"] = observe.select(
                fixture.client.request("fixture", "GET", path),
                ("allowed", "disabled", "priority", "sourceRanges", "targetServiceAccounts"),
            )
            fixture.wait(
                fixture.client.request("fixture", "DELETE", path, timeout=fixture.remaining())
            )
        snapshot.match("firewall-" + suffix, result)
    snapshot.match(
        "instance-labels-text",
        outcome(fixture, observe, fixture.vm("p-labels", labels="managed-by")),
    )
    snapshot.match(
        "instance-without-act-as",
        outcome(fixture, observe, fixture.vm("p-act-as", account=fixture.account)),
    )


def denied_act_as(response: Response) -> bool:
    """Report whether an instance insert finished refused for want of actAs."""
    errors = response.json().get("error", {}).get("errors", [])
    return any(error.get("code") == "SERVICE_ACCOUNT_ACCESS_DENIED" for error in errors)


def instances(fixture: AccountFixture, snapshot: object, observe: Observations) -> None:
    """Grant actAs, then start vm2 as the account and vm1 with the tag, out of name order."""
    client = fixture.client
    snapshot.match("act-as-granted", {"http_status": fixture.grant_act_as().status})
    # Compute refuses a missing actAs in the finished operation; the grant takes
    # a while to reach it, so a live run retries that refusal.
    deadline = time.monotonic() + (300 if client.sandbox else 0)
    with_account = fixture.wait(fixture.vm("vm2", account=fixture.account))
    while denied_act_as(with_account) and time.monotonic() < deadline:
        time.sleep(15)
        with_account = fixture.wait(fixture.vm("vm2", account=fixture.account))
    snapshot.match("instance-with-account", operation(with_account))
    with_tag = fixture.vm("vm1", tags=("gl-web",))
    snapshot.match("instance-with-tag", operation(fixture.wait(with_tag)))
    snapshot.match(
        "instance-list",
        ours(fixture, client.request("fixture", "GET", f"{fixture.zone_path}/instances")),
    )
    for suffix in ("vm1", "vm2"):
        path = f"{fixture.zone_path}/instances/{fixture.name(suffix)}"
        instance = client.request("fixture", "GET", path).json()
        snapshot.match(
            "instance-" + suffix,
            observe.value(
                {
                    "serviceAccounts": instance.get("serviceAccounts"),
                    "tags": instance.get("tags", {}).get("items"),
                }
            ),
        )


def scoped_verdict(observe: Observations, response: Response) -> dict:
    """Select the verdict and the name of every firewall rule the trace applied."""
    traces = response.json().get("reachabilityDetails", {}).get("traces", [])
    steps = traces[0].get("steps", []) if traces else []
    return {
        **verdict(observe, response),
        "firewalls": [
            observe.text(step["firewall"].get("displayName", ""))
            for step in steps
            if "firewall" in step
        ],
    }


def verdicts(fixture: AccountFixture, snapshot: object, observe: Observations) -> None:
    """Scope one rule per port by account or tag, then test both directions."""
    client, ranges = fixture.client, ["10.10.0.0/24"]
    for suffix, port, scope in (
        (
            "fw-target-sa",
            "8080",
            {"sourceRanges": ranges, "targetServiceAccounts": [fixture.account]},
        ),
        ("fw-source-sa", "9090", {"sourceServiceAccounts": [fixture.account]}),
        ("fw-target-tag", "7070", {"sourceRanges": ranges, "targetTags": ["gl-web"]}),
        ("fw-source-tag", "6060", {"sourceTags": ["gl-web"]}),
        ("fw-allow-5050", "5050", {"sourceRanges": ranges}),
        (
            "fw-egress-deny-sa",
            "5050",
            {
                "direction": "EGRESS",
                "destinationRanges": ranges,
                "targetServiceAccounts": [fixture.account],
            },
        ),
    ):
        verb = "denied" if scope.get("direction") == "EGRESS" else "allowed"
        created = fixture.firewall(
            suffix,
            {
                "direction": "INGRESS",
                "priority": 1000,
                verb: [{"IPProtocol": "tcp", "ports": [port]}],
                **scope,
            },
        )
        snapshot.match(suffix + "-operation", operation(fixture.wait(created)))
    snapshot.match(
        "firewall-target-sa",
        observe.select(
            client.request(
                "fixture", "GET", f"{fixture.global_path}/firewalls/{fixture.name('fw-target-sa')}"
            ),
            ("direction", "sourceRanges", "targetServiceAccounts", "allowed"),
        ),
    )
    snapshot.match(
        "firewall-list",
        ours(fixture, client.request("fixture", "GET", f"{fixture.global_path}/firewalls")),
    )
    for test_id, source, destination, port in (
        ("t-target-sa", "vm1", "vm2", 8080),
        ("t-not-target-sa", "vm2", "vm1", 8080),
        ("t-source-sa", "vm2", "vm1", 9090),
        ("t-not-source-sa", "vm1", "vm2", 9090),
        ("t-target-tag", "vm2", "vm1", 7070),
        ("t-not-target-tag", "vm1", "vm2", 7070),
        ("t-source-tag", "vm1", "vm2", 6060),
        ("t-not-source-tag", "vm2", "vm1", 6060),
        ("t-egress-sa", "vm2", "vm1", 5050),
        ("t-not-egress-sa", "vm1", "vm2", 5050),
    ):
        snapshot.match(
            test_id,
            scoped_verdict(
                observe,
                fixture.connectivity(
                    test_id,
                    {
                        "source": {"instance": fixture.instance_url(source)},
                        "destination": {
                            "instance": fixture.instance_url(destination),
                            "port": port,
                        },
                        "protocol": "TCP",
                    },
                ),
            ),
        )


def run_scenario(fixture: AccountFixture, snapshot: object, created: Response) -> None:
    """Make the same selected observations in local, recording, and revalidation runs."""
    observe = Observations(
        {
            fixture.prefix: "<prefix>",
            fixture.manifest["run_id"]: "<run>",
            fixture.project: "<project>",
            fixture.client.number: "<project-number>",
            fixture.account: "<account>",
            **{email: "<" + actor + ">" for actor, email in fixture.client.emails.items()},
        }
    )
    snapshot.match("network-operation", operation(created))
    lists_and_regions(fixture, snapshot, observe)
    snapshot.match("account-created", {"http_status": fixture.create_account().status})
    fixture.wait_until(fixture.read_account, {HTTPStatus.NOT_FOUND}, 120)
    refusals(fixture, snapshot, observe)
    instances(fixture, snapshot, observe)
    verdicts(fixture, snapshot, observe)


def build(
    target: Target,
    sandbox: Sandbox | None,
    scope: LiveScope | None,
    state_dir: Path,
    manifest: str | None = None,
) -> AccountFixture:
    """Build the fixture with Compute, Network Management, and IAM clients."""
    compute, management = clients(target, sandbox=sandbox, scope=scope)
    accounts = RestClient(target, IAM, sandbox=sandbox, scope=scope)
    return AccountFixture(compute, management, accounts, state_dir, manifest=manifest)


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
    state = Path.home() / ".local/state/gcp-local" / sandbox.project / SERVICE
    build(target, sandbox, scope, state, manifest=args.manifest).cleanup()
    sys.stdout.write(f"Owned service-account VM cleanup passed at {now()}\n")


if __name__ == "__main__":
    main()
