#!/usr/bin/env python3
"""Prove loopback works and a child process has no external IPv4/IPv6 route."""

import argparse
import errno
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen


def probe() -> None:
    """Prove loopback works and external routes are absent."""
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    for family, address in (
        (socket.AF_INET, ("192.0.2.1", 443)),
        (socket.AF_INET6, ("2001:db8::1", 443)),
    ):
        with socket.socket(family, socket.SOCK_STREAM) as connection:
            connection.settimeout(1)
            assert connection.connect_ex(address) in {errno.ENETUNREACH, errno.EHOSTUNREACH}, (
                "External routing is available"
            )
    assert not any(
        key in os.environ
        for key in ("GOOGLE_APPLICATION_CREDENTIALS", "GCP_TEST_CREDENTIALS_FILE", "HTTPS_PROXY")
    )


def main() -> None:
    """Run the isolation probe inside the isolated child process."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--detached-probe")
    args = parser.parse_args()
    if args.detached_probe:
        subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        Path(args.detached_probe).write_text(str(Path("/proc/self/ns/pid").readlink()))
        time.sleep(60)
        return
    if args.probe:
        probe()
        return
    if args.child:
        probe()
        with urlopen("http://127.0.0.1:18463/", timeout=2) as response:
            assert response.status == 200
        subprocess.run([sys.executable, __file__, "--probe"], check=True, timeout=5)
        print(
            "Isolation smoke passed: loopback, IPv4/IPv6 egress denial, "
            "and inherited subprocess isolation."
        )
        return
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory() as directory:
        env = dict(
            os.environ,
            GOOGLE_APPLICATION_CREDENTIALS="/unselected/secret.json",
            GCP_TEST_CREDENTIALS_FILE="/unselected/test.json",
            HTTPS_PROXY="http://127.0.0.1:1",
        )
        server = [
            sys.executable,
            "-m",
            "http.server",
            "18463",
            "--bind",
            "127.0.0.1",
            "--directory",
            directory,
        ]
        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/run/isolated.py"),
                "--server-json",
                json.dumps(server),
                "--ready-url",
                "http://127.0.0.1:18463/",
                "--timeout",
                "20",
                "--",
                sys.executable,
                __file__,
                "--child",
            ],
            env=env,
            check=True,
            timeout=35,
        )
        # The runner must bound a hanging test process too.
        namespace_file = Path(directory) / "namespace"
        result = subprocess.run(
            [
                sys.executable,
                str(root / "scripts/run/isolated.py"),
                "--timeout",
                "1",
                "--",
                sys.executable,
                __file__,
                "--detached-probe",
                str(namespace_file),
            ],
            timeout=10,
            check=False,
        )
        assert result.returncode == 124
        namespace = namespace_file.read_text()
        for path in Path("/proc").glob("[0-9]*/ns/pid"):
            try:
                current = path.readlink()
            except (FileNotFoundError, PermissionError):
                continue
            assert current != namespace, "A detached test descendant survived its runner"
        print("Isolation smoke passed: timeout removed hung and detached descendants.")


if __name__ == "__main__":
    main()
