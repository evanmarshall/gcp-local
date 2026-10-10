"""Explicit targets, with no ambient credential or project discovery."""

import ipaddress
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


def _require_loopback_endpoint(endpoint: str | None) -> None:
    """Reject any endpoint that is not a loopback URL with an explicit port."""
    url = urlsplit(endpoint or "")
    try:
        loopback = ipaddress.ip_address(url.hostname or "").is_loopback
        port = url.port
    except ValueError:
        loopback, port = False, None
    if not (
        loopback
        and port
        and url.scheme in {"http", "https"}
        and not url.username
        and not url.password
        and url.path in {"", "/"}
        and not url.query
        and not url.fragment
    ):
        raise ValueError("Local mode requires a loopback IP endpoint with an explicit port")


@dataclass(frozen=True)
class Target:
    """The validation mode plus the exact project and endpoint it talks to."""

    mode: str = "offline"
    project: str | None = None
    endpoint: str | None = None
    credentials_file: Path | None = None
    live_scope: str | None = None

    @classmethod
    def select(
        cls,
        mode: str = "offline",
        *,
        project: str | None = None,
        endpoint: str | None = None,
        credentials_file: Path | None = None,
        live_scope: str | None = None,
        allow_live: bool = False,
    ) -> "Target":
        """Validate the option combination for the requested mode."""
        if mode == "offline":
            if any((project, endpoint, credentials_file, live_scope, allow_live)):
                raise ValueError("Offline mode does not accept target or credential options")
            return cls()
        if mode == "local":
            return cls._local(
                project,
                endpoint,
                credentials_file,
                live_scope,
                allow_live=allow_live,
            )
        if mode not in {"record", "revalidate"}:
            raise ValueError("Unknown validation mode")
        if not all((project, credentials_file, live_scope, allow_live)):
            raise ValueError(
                "Live mode requires project, credential file, live scope, and --gcp-allow-live"
            )
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
            raise ValueError("Supply the dedicated sandbox project ID, not its number")
        if endpoint:
            raise ValueError(
                "Live API endpoints are selected by the service adapter, not an override"
            )
        path = Path(credentials_file).expanduser().resolve()
        if not path.is_file():
            raise ValueError("The explicitly selected credential file does not exist")
        if not live_scope.strip():
            raise ValueError("Describe the authorized live resource/cost scope")
        return cls(mode, project, credentials_file=path, live_scope=live_scope)

    @classmethod
    def _local(
        cls,
        project: str | None,
        endpoint: str | None,
        credentials_file: Path | None,
        live_scope: str | None,
        *,
        allow_live: bool,
    ) -> "Target":
        """Build the loopback-only local target, refusing real-credential options."""
        if credentials_file or live_scope or allow_live:
            raise ValueError("Local mode cannot use real credentials or live-cloud options")
        _require_loopback_endpoint(endpoint)
        if not project:
            raise ValueError("Local mode requires an explicit synthetic project ID")
        return cls("local", project, endpoint.rstrip("/"))
