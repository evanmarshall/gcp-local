#!/usr/bin/env python3
"""Measure the official emulator and verify its local protocol/lifecycle limits."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def main():
    if (sys.platform != 'linux' or os.environ.get('GCP_LOCAL_ISOLATED') != '1'
            or {name for _, name in socket.if_nameindex()} != {'lo'}):
        raise SystemExit('Use scripts/run_isolated.py in moo/Linux')
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / 'toolchains/pubsub.json').read_text())
    jar = root / '.tools/pubsub/emulator.jar'
    if hashlib.sha256(jar.read_bytes()).hexdigest() != manifest['jar_sha256']:
        raise SystemExit('Official emulator JAR does not match its pin')
    java = os.environ.get('GCP_LOCAL_JAVA') or shutil.which('java')
    if not java or not Path(java).is_absolute():
        raise SystemExit('Install Java 17 or select its absolute path with GCP_LOCAL_JAVA')
    java_version = subprocess.check_output([java, '-version'], stderr=subprocess.STDOUT, text=True).splitlines()[0]
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    command = [java, '-Xms32m', '-Xmx256m', '-XX:MaxDirectMemorySize=128m', '-XX:ActiveProcessorCount=2',
               '-jar', str(jar), '--host=127.0.0.1', '--port=' + str(port), '--projects=backend-probe,backend-peer']
    checks, samples = [], []
    with tempfile.TemporaryDirectory(prefix='gcp-local-pubsub-') as directory:
        for restart in (False, True):
            started = time.monotonic()
            log = Path(directory) / ('restart.log' if restart else 'initial.log')
            with log.open('w') as output:
                process = subprocess.Popen(command, cwd=directory, env={'PATH': '/usr/bin:/bin', 'TMPDIR': directory},
                                           stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 20
                while True:
                    if process.poll() is not None:
                        raise RuntimeError('Official emulator exited before readiness: ' + log.read_text()[-4000:])
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise RuntimeError('Official emulator did not become ready') from None
                        time.sleep(0.05)
                startup = time.monotonic() - started
                args = [str(root / 'bin/pubsub-probe'), '-endpoint', f'127.0.0.1:{port}']
                if restart:
                    args.append('-expect-empty')
                result = subprocess.run(args, capture_output=True, text=True, timeout=30)
                if result.returncode:
                    raise RuntimeError(result.stdout + result.stderr + log.read_text()[-4000:])
                checks.append(json.loads(result.stdout))
                status = Path(f'/proc/{process.pid}/status').read_text().splitlines()
                rss = next(int(line.split()[1]) for line in status if line.startswith('VmRSS:'))
                samples.append({'startup_seconds': round(startup, 3), 'rss_kib_after_probe': rss})
            finally:
                stopped = time.monotonic()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                        raise RuntimeError('Official emulator did not shut down gracefully') from None
                if samples:
                    samples[-1]['shutdown_seconds'] = round(time.monotonic() - stopped, 3)
                with socket.socket() as closed:
                    closed.settimeout(1)
                    if closed.connect_ex(('127.0.0.1', port)) == 0:
                        raise RuntimeError('Official emulator left its listener open')
    artifacts = root / '.artifacts/pubsub-backend'
    artifacts.mkdir(parents=True, exist_ok=True)
    report = {'kind': 'backend-only', 'emulator': manifest['version'], 'jar_sha256': manifest['jar_sha256'],
              'java': java_version, 'checks': checks, 'samples': samples,
              'network': 'loopback-only', 'gcp_execution': False, 'provider_iam': 'not-tested-by-backend-probe'}
    (artifacts / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
