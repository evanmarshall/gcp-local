#!/usr/bin/env python3
"""Prove loopback works and a child process has no external IPv4/IPv6 route."""

import argparse
import errno
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
from urllib.request import urlopen


def probe():
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    for family, address in ((socket.AF_INET, ("192.0.2.1", 443)),
                            (socket.AF_INET6, ("2001:db8::1", 443))):
        with socket.socket(family, socket.SOCK_STREAM) as connection:
            connection.settimeout(1)
            assert connection.connect_ex(address) in {errno.ENETUNREACH, errno.EHOSTUNREACH}, "External routing is available"
    assert not any(key in os.environ for key in ("GOOGLE_APPLICATION_CREDENTIALS", "GCP_TEST_CREDENTIALS_FILE", "HTTPS_PROXY"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()
    if args.probe:
        probe()
        return
    if args.child:
        probe()
        with urlopen("http://127.0.0.1:18463/", timeout=2) as response:
            assert response.status == 200
        subprocess.run([sys.executable, __file__, "--probe"], check=True, timeout=5)
        print("Isolation smoke passed: loopback, IPv4/IPv6 egress denial, and inherited subprocess isolation.")
        return
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as directory:
        env = dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS="/unselected/secret.json",
                   GCP_TEST_CREDENTIALS_FILE="/unselected/test.json", HTTPS_PROXY="http://127.0.0.1:1")
        server = [sys.executable, "-m", "http.server", "18463", "--bind", "127.0.0.1", "--directory", directory]
        subprocess.run([sys.executable, str(root / "scripts/run_isolated.py"), "--server-json", json.dumps(server),
            "--ready-url", "http://127.0.0.1:18463/", "--timeout", "20", "--", sys.executable, __file__, "--child"],
            env=env, check=True, timeout=35)
        # The runner must bound a hanging test process too.
        result = subprocess.run([sys.executable, str(root / "scripts/run_isolated.py"), "--timeout", "1", "--",
            sys.executable, "-c", "import time; time.sleep(60)"], timeout=10)
        assert result.returncode == 124
        print("Isolation smoke passed: hung child terminated within the runner deadline.")


if __name__ == "__main__":
    main()
