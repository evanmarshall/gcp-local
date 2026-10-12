"""Helpers shared by the smoke scripts. Not an entry point."""

import json
import socket
import urllib.parse
from urllib.request import ProxyHandler, Request, build_opener


def exchange(endpoint: str, subject_token: str) -> str:
    """Exchange one enrolled subject token at the local STS for an access token."""
    form = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "audience": "//gcp-local/identity",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": subject_token,
            "scope": "https://www.googleapis.com/auth/cloud-platform",
        }
    ).encode()
    request = Request(  # noqa: S310 -- the caller's loopback STS endpoint
        endpoint + "/v1/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with build_opener(ProxyHandler({})).open(request, timeout=10) as response:
        return json.loads(response.read())["access_token"]


def free_ports(count: int) -> int:
    """Return a base port whose `count` consecutive loopback ports are all free."""
    for _ in range(100):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            base = sock.getsockname()[1]
        if base + count - 1 > 65535:  # the highest TCP port
            continue
        try:
            for port in range(base, base + count):
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        return base
    raise AssertionError(f"No free block of {count} loopback ports")
