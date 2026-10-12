"""Select public Storage fields and preserve identifier/etag relationships."""

import hashlib

from gcp_validation.evidence import check_observation


class StorageObservations:
    def __init__(self, fixture):
        self.fixture = fixture
        self.aliases = {fixture.bucket: "<bucket>", fixture.project: "<project>",
                        fixture.project_number: "<project-number>"}
        if fixture.sandbox:
            self.aliases.update({identity.email: "<" + alias + ">"
                                for alias, identity in fixture.sandbox.identities.items()})
        if fixture.sandbox and fixture.sandbox.peer_project:
            self.aliases[fixture.sandbox.peer_project] = "<peer-project>"
        if not fixture.sandbox:
            for actor in ("fixture", "allowed", "denied", "peer"):
                project = "synthetic-peer" if actor == "peer" else fixture.project
                self.aliases[actor + "@" + project + ".iam.gserviceaccount.com"] = "<" + actor + ">"
            self.aliases["synthetic-peer"] = "<peer-project>"
        self.references = {}

    def reference(self, kind, value):
        if not isinstance(value, str) or not value:
            raise ValueError("Missing Storage reference field: " + kind)
        values = self.references.setdefault(kind, {})
        return values.setdefault(value, f"<{kind}-{len(values) + 1}>")

    def text(self, value):
        for original, alias in sorted(self.aliases.items(), key=lambda pair: -len(pair[0])):
            value = value.replace(original, alias)
        check_observation(value)
        return value

    def object(self, value):
        selected = {key: value[key] for key in ("name", "size", "contentType", "md5Hash")}
        selected["bucket"] = self.text(value["bucket"])
        for key in ("generation", "metageneration", "etag"):
            selected[key] = self.reference(key, value[key])
        check_observation(selected)
        return selected

    def bucket(self, response):
        value = response.json()
        return {"status": response.status, "name": self.text(value["name"]),
                "location": value["location"], "storageClass": value["storageClass"],
                "uniform_access": value["iamConfiguration"]["uniformBucketLevelAccess"]["enabled"],
                "public_access": value["iamConfiguration"]["publicAccessPrevention"],
                "soft_delete_seconds": int(value.get("softDeletePolicy", {}).get("retentionDurationSeconds", 0))}

    def error(self, response):
        try:
            value = response.json()
        except (ValueError, UnicodeError):
            selected = {"status": response.status, "format": "text",
                        "message": self.text(response.body.decode("utf-8"))}
        else:
            if not isinstance(value, dict) or not isinstance(value.get("error"), dict):
                raise ValueError("Unexpected Storage error structure")
            error = value["error"]
            selected = {"status": response.status, "format": "json", "code": error["code"],
                        "message": self.text(error["message"]),
                        "reasons": [entry["reason"] for entry in error.get("errors", [])]}
        selected["content_type"] = response.content_type
        check_observation(selected)
        return selected

    @staticmethod
    def content(response):
        return {"status": response.status, "bytes": len(response.body),
                "sha256": hashlib.sha256(response.body).hexdigest()}
