"""Bounded, observable bucket IAM transitions for authorization conformance."""

import time

from gcp_validation.evidence import Inconclusive


class StorageAuthorization:
    def __init__(self, storage):
        self.storage = storage

    def member(self, actor):
        if actor not in {"allowed", "denied", "peer"}:
            raise ValueError("Only scenario callers may receive temporary bucket grants")
        if self.storage.sandbox:
            identity = self.storage.sandbox.identities.get(actor)
            if identity is None:
                raise ValueError("Select an explicit identity for the authorization scenario")
            email = identity.email
        else:
            project = "synthetic-peer" if actor == "peer" else self.storage.project
            email = actor + "@" + project + ".iam.gserviceaccount.com"
        return "serviceAccount:" + email

    def policy(self):
        return self.storage.client.request("fixture", "GET", self.storage.path + "/iam").require(200)

    def set_role(self, actor, role=None):
        """Preserve etag and unrelated bindings; grants die with the owned bucket."""
        if role not in {None, "roles/storage.objectUser", "roles/storage.objectViewer"}:
            raise ValueError("Select a bounded scenario role")
        member = self.member(actor)
        policy = self.policy()
        bindings = []
        for original in policy.get("bindings", []):
            binding = dict(original)
            binding["members"] = [value for value in binding["members"] if value != member]
            if binding["members"]:
                bindings.append(binding)
        if role:
            bindings.append({"role": role, "members": [member]})
        policy["bindings"] = bindings
        self.storage.client.request("fixture", "PUT", self.storage.path + "/iam", body=policy).require(200)
        actual = self.policy()
        attached = [binding["role"] for binding in actual.get("bindings", [])
                    if member in binding["members"]]
        assert attached == ([role] if role else [])
        return {"actor": actor, "attachment": "bucket", "roles": attached}

    def wait_read(self, name, actor, expected):
        # IAM documents two minutes typically, seven minutes or longer in some cases.
        # Exhaustion is inconclusive, never proof of a grant or revocation.
        deadline = time.monotonic() + (420 if self.storage.sandbox else 0)
        while True:
            response = self.storage.read(name, actor=actor)
            if response.status == expected:
                return response
            if response.status not in {200, 403}:
                response.require(expected)
            if time.monotonic() >= deadline:
                raise Inconclusive("IAM read transition did not converge within the bounded deadline")
            time.sleep(5)
