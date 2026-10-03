"""Explicit targets, with no ambient credential or project discovery."""

from dataclasses import dataclass
import ipaddress
from pathlib import Path
import re
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Target:
    mode: str = "offline"
    project: str | None = None
    endpoint: str | None = None
    credentials_file: Path | None = None
    live_scope: str | None = None

    @classmethod
    def select(cls, mode="offline", *, project=None, endpoint=None,
               credentials_file=None, live_scope=None, allow_live=False):
        if mode == "offline":
            if any((project, endpoint, credentials_file, live_scope, allow_live)):
                raise ValueError("Offline mode does not accept target or credential options")
            return cls()
        if mode == "local":
            if credentials_file or live_scope or allow_live:
                raise ValueError("Local mode cannot use real credentials or live-cloud options")
            url = urlsplit(endpoint or "")
            try:
                loopback = ipaddress.ip_address(url.hostname or "").is_loopback
                port = url.port
            except ValueError:
                loopback, port = False, None
            if not (loopback and port and url.scheme in {"http", "https"}
                    and not url.username and not url.password
                    and url.path in {"", "/"} and not url.query and not url.fragment):
                raise ValueError("Local mode requires a loopback IP endpoint with an explicit port")
            if not project:
                raise ValueError("Local mode requires an explicit synthetic project ID")
            return cls(mode, project, endpoint.rstrip("/"))
        if mode not in {"record", "revalidate"}:
            raise ValueError("Unknown validation mode")
        if not all((project, credentials_file, live_scope, allow_live)):
            raise ValueError("Live mode requires project, credential file, live scope, and --gcp-allow-live")
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
            raise ValueError("Supply the dedicated sandbox project ID, not its number")
        if endpoint:
            raise ValueError("Live API endpoints are selected by the service adapter, not an override")
        path = Path(credentials_file).expanduser().resolve()
        if not path.is_file():
            raise ValueError("The explicitly selected credential file does not exist")
        if not live_scope.strip():
            raise ValueError("Describe the authorized live resource/cost scope")
        return cls(mode, project, credentials_file=path, live_scope=live_scope)
