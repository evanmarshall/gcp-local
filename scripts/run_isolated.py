#!/usr/bin/env python3
"""Run a local provider and its test command in one Linux network namespace."""

import argparse
import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener

UNSHARE = shutil.which("unshare") or "unshare"
IP = shutil.which("ip") or "ip"


def assert_isolated() -> None:
    """Refuse to run on a machine with real GCP credentials present."""
    if sys.platform != "linux" or {name for _, name in __import__("socket").if_nameindex()} != {
        "lo"
    }:
        raise RuntimeError(
            "Local upstream tests require a Linux network namespace containing only loopback"
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Validate the runner options and the embedded server command.

    The raw argv is kept on the namespace: the re-exec must forward the
    provider options, not only the test command.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server-json", help="JSON argv for a provider started inside the same namespace"
    )
    parser.add_argument("--ready-url")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    args.raw_argv = list(argv)
    args.test_command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not args.test_command or not 1 <= args.timeout <= 1800:
        parser.error("Supply a command and timeout between 1 and 1800 seconds")
    if args.server_json:
        check_server(parser, args)
    elif args.ready_url:
        parser.error("A readiness URL requires a server command")
    if not args.inside and sys.platform != "linux":
        parser.error("Run in moo or Linux CI; no weaker host fallback is available")
    return args


def check_server(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Require a JSON string array and a numeric loopback readiness URL."""
    server_command = json.loads(args.server_json)
    if (
        not isinstance(server_command, list)
        or not server_command
        or not all(isinstance(value, str) for value in server_command)
    ):
        parser.error("Server command must be a nonempty JSON string array")
    url = urlsplit(args.ready_url or "")
    if (
        url.scheme != "http"
        or url.hostname not in {"127.0.0.1", "::1"}
        or not url.port
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        parser.error("Select a numeric loopback HTTP readiness URL with an explicit port")


def reenter_isolated(args: argparse.Namespace) -> None:
    """Re-exec the runner inside a private network, mount, and PID namespace."""
    # Terraform and other helpers may create their own process groups. A
    # private PID namespace also removes detached descendants when this
    # runner exits, including after timeout or a killed test process.
    original = list(args.raw_argv)
    os.execvp(  # noqa: S606 -- the argv list form, not a shell command string
        UNSHARE,
        [
            UNSHARE,
            "--net",
            "--mount",
            "--pid",
            "--mount-proc",
            "--fork",
            "--kill-child",
            sys.executable,
            str(Path(__file__).resolve()),
            "--inside",
            *original,
        ],
    )


def build_environment() -> dict[str, str]:
    """Strip cloud credentials and proxies, then mark the isolated run."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GOOGLE_", "GCLOUD_", "CLOUDSDK_", "GCP_TEST_", "ACTIONS_ID_TOKEN_"))
        and key.lower() not in {"http_proxy", "https_proxy", "all_proxy"}
    }
    env["GCP_LOCAL_ISOLATED"] = "1"
    return env


def await_provider(server: subprocess.Popen, args: argparse.Namespace) -> None:
    """Wait for the provider's loopback readiness URL."""
    deadline = time.monotonic() + 20
    opener = build_opener(ProxyHandler({}))
    while True:
        if server.poll() is not None:
            raise RuntimeError("Provider exited before readiness")
        try:
            with opener.open(args.ready_url, timeout=1) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("Provider readiness deadline exceeded")
        time.sleep(0.1)


def main(argv: list[str] | None = None) -> int:
    """Run the requested validation in a clean isolated environment."""
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if not args.inside:
        reenter_isolated(args)
    subprocess.run([IP, "link", "set", "lo", "up"], check=True)
    assert_isolated()
    env = build_environment()
    children: list[subprocess.Popen] = []

    def stop_all() -> None:
        """Stop every process this script started."""
        for child in reversed(children):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGTERM)
        for child in reversed(children):
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()

    def interrupted(signum: int, frame: object) -> None:  # noqa: ARG001 -- signal contract
        """Handle a termination signal by stopping the isolation."""
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        if args.server_json:
            server = subprocess.Popen(json.loads(args.server_json), env=env, start_new_session=True)
            children.append(server)
            await_provider(server, args)
        test = subprocess.Popen(args.test_command, env=env, start_new_session=True)
        children.append(test)
        return test.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        print("Isolated runner exceeded its time limit.", file=sys.stderr)
        return 124
    except KeyboardInterrupt:
        return 130
    finally:
        stop_all()


if __name__ == "__main__":
    sys.exit(main())
