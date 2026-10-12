#!/usr/bin/env python3
"""Install the built wheel into a fresh venv and drive its gcp-local CLI on Docker.

The CLI runs outside the checkout, so it cannot fall back to a source build: it
starts the image selected with --image (a local tag here; a published reference
for users). Nothing is pulled from or pushed to a registry beyond the pinned
HAProxy entry point.
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKER = shutil.which("docker") or "docker"


def run(*command: str, cwd: Path | None = None, timeout: int = 900) -> str:
    """Run one command, failing with its output."""
    result = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False
    )
    if result.returncode:
        raise AssertionError(" ".join(command[:3]) + " failed:\n" + result.stdout + result.stderr)
    return result.stdout


def build_wheel(python: Path, output: Path) -> Path:
    """Build the wheel from this checkout with the pinned build backend."""
    run(str(python), "-m", "pip", "wheel", "--no-deps", "--wheel-dir", str(output), str(ROOT))
    wheels = list(output.glob("gcp_local-*.whl"))
    assert len(wheels) == 1, wheels
    return wheels[0]


def main() -> int:
    """Build or take a wheel and image, then exercise the installed console script."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, help="prebuilt wheel; default: build one")
    parser.add_argument("--image", help="runtime image; default: build gcp-local:package-smoke")
    parser.add_argument("--port", type=int, default=15466)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="gcp-local-wheel-") as temporary:
        root = Path(temporary).resolve()
        venv.create(root / "venv", with_pip=True)
        python = root / "venv/bin/python"
        wheel = args.wheel.resolve() if args.wheel else build_wheel(python, root / "dist")
        image = args.image
        if image is None:
            image = "gcp-local:package-smoke"
            run(DOCKER, "build", "--build-arg", "GCP_LOCAL_VERSION=package-smoke", "--tag",
                image, str(ROOT), timeout=1800)  # fmt: skip
        run(str(python), "-m", "pip", "install", "--no-deps", "--quiet", str(wheel))
        cli = str(root / "venv/bin/gcp-local")
        work = root / "application"
        work.mkdir()
        directory = work / ".gcp-local/environment"
        base = (cli, "--directory", str(directory))
        assert run(cli, "--version", cwd=work).strip().startswith("gcp-local ")
        try:
            started = json.loads(
                run(*base, "--port", str(args.port), "--image", image, "up", cwd=work)
                .strip()
                .splitlines()[-1]
            )
            assert started["ready"], started
            assert started["image"] == image, "the installed CLI did not use the selected image"
            status = json.loads(run(*base, "status", cwd=work))
            assert status["ready"], status
            assert len(status["providers"]) == 14, status
            env = json.loads(run(*base, "env", cwd=work))
            assert env["GCP_LOCAL_ENDPOINT"] == f"http://127.0.0.1:{args.port}"
            for example, greeting in (
                ("storage.py", "Hello from local GCP."),
                ("pubsub.py", "Hello from local Pub/Sub."),
            ):
                output = run(
                    sys.executable, str(ROOT / "examples" / example), "--directory", str(directory)
                )
                assert greeting in output, output
            run(*base, "reset", cwd=work)
            assert json.loads(run(*base, "status", cwd=work))["ready"]
        finally:
            subprocess.run([*base, "down"], cwd=work, capture_output=True, check=False)
        project = json.loads((directory / "environment.json").read_text())["compose_project"]
        remaining = run(
            DOCKER, "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project
        )
        assert not remaining.strip(), "down left a container"
    print("Installed wheel CLI passed: up, status, env, SDK examples, reset, and down.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
