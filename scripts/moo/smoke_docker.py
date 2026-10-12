#!/usr/bin/env python3
"""Probe native arm64 core/Storage inside a network-none task-guest container."""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]

PROBE = r"""
import json,socket,urllib.request,urllib.parse
assert {name for _,name in socket.if_nameindex()}=={'lo'}
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
base='http://127.0.0.1:4566'
form=urllib.parse.urlencode({'grant_type':'urn:ietf:params:oauth:grant-type:token-exchange','audience':'//gcp-local/identity','subject_token_type':'urn:ietf:params:oauth:token-type:access_token','requested_token_type':'urn:ietf:params:oauth:token-type:access_token','subject_token':'gls.synthetic-moo-container-probe-subject','scope':'https://www.googleapis.com/auth/cloud-platform'})
token=json.load(opener.open(urllib.request.Request(base+'/v1/token',data=form.encode(),headers={'Content-Type':'application/x-www-form-urlencoded'}),timeout=5))['access_token']
def call(method,path,body=None,media=False):
 data=body if media else json.dumps(body).encode() if body is not None else None
 headers={'Authorization':'Bearer '+token,
                  'Content-Type':'application/octet-stream' if media else 'application/json'}
 request=urllib.request.Request(base+path,data=data,headers=headers,method=method)
 with opener.open(request,timeout=5) as response:
  return response.read()
assert not json.loads(call('GET','/storage/v1/b?project=moo-probe')).get('items')
bucket={'name':'moo-container-bucket','location':'US-CENTRAL1','iamConfiguration':{'uniformBucketLevelAccess':{'enabled':True},'publicAccessPrevention':'enforced'},'softDeletePolicy':{'retentionDurationSeconds':'0'}}
call('POST','/storage/v1/b?project=moo-probe',bucket)
call('POST','/upload/storage/v1/b/moo-container-bucket/o?uploadType=media&name=probe&ifGenerationMatch=0',b'container-storage',True)
assert call('GET','/storage/v1/b/moo-container-bucket/o/probe?alt=media')==b'container-storage'
print(json.dumps({'local_token_exchange':True,'storage_write_read':True,'network_none':True}))
"""


def main() -> None:
    """Probe the native arm64 core and Storage in a task-guest container."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--docker-host",
        required=True,
        help="Explicit task-guest Unix socket; never a host/remote engine",
    )
    args = parser.parse_args()
    if (
        platform.system() != "Linux"
        or platform.machine() != "aarch64"
        or not args.docker_host.startswith("unix:///")
    ):
        parser.error("Run in an arm64 Moo task guest with an explicit Unix Docker socket")
    # The first stage's base image, skipping flags such as --platform.
    image = next(
        word
        for line in (ROOT / "packaging/Dockerfile").read_text().splitlines()
        if line.startswith("FROM ")
        for word in line.split()[1:]
        if not word.startswith("--")
    )
    if "@sha256:" not in image:
        raise ValueError("The diagnostic base image must be digest pinned")
    run_id = uuid4().hex
    project, tag = "gcp-local-moo-" + run_id, "gcp-local-moo-probe:" + run_id
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME"}}
    env.update(DOCKER_HOST=args.docker_host, CGO_ENABLED="0")

    def command(args: list[str], *, check: bool = True, timeout: int = 60) -> object:
        result = subprocess.run(
            args, env=env, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False
        )
        if check and result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        return result

    # Image pulling is a separate preparatory step, outside isolated execution.
    command(["docker", "image", "inspect", image])
    versions = json.loads(command(["docker", "version", "--format", "{{json .}}"]).stdout)
    compose_version = command(["docker", "compose", "version", "--short"]).stdout.strip()
    command(
        ["sh", "scripts/go.sh", "build", "-o", "bin/moo-container-core", "./cmd/gcp-local"],
        timeout=180,
    )
    with tempfile.TemporaryDirectory(prefix="gcp-local-moo-docker-") as temporary:
        directory = Path(temporary)
        env["DOCKER_CONFIG"] = str(directory / "empty-docker-config")
        binary = directory / "gcp-local"
        shutil.copyfile(ROOT / "bin/moo-container-core", binary)
        binary.chmod(0o755)
        (directory / "Dockerfile").write_text(
            f'FROM {image}\nCOPY gcp-local /gcp-local\nENTRYPOINT ["/gcp-local"]\n'
        )
        (directory / ".dockerignore").write_text("*\n!Dockerfile\n!gcp-local\n")
        config = directory / "config"
        config.mkdir(mode=0o700)
        principal = "serviceAccount:fixture@moo-probe.iam.gserviceaccount.com"
        project_resource = "//cloudresourcemanager.googleapis.com/projects/moo-probe"
        configs = {
            "identity.json": {
                "principals": [
                    {
                        "subject": principal,
                        "project": "moo-probe",
                        "subject_token": "gls.synthetic-moo-container-probe-subject",
                    }
                ]
            },
            "iam.json": {
                "resources": [
                    {
                        "name": project_resource,
                        "service": "resourcemanager",
                        "kind": "project",
                        "number": "123456",
                        "scope": {"project": "moo-probe", "location": "global"},
                    }
                ],
                "policies": {
                    project_resource: {
                        "bindings": [{"role": "roles/storage.admin", "members": [principal]}]
                    }
                },
            },
        }
        for name, value in configs.items():
            path = config / name
            path.write_text(json.dumps(value))
            path.chmod(0o600)
        compose = directory / "compose.json"
        compose.write_text(
            json.dumps(
                {
                    "services": {
                        "core": {
                            "image": tag,
                            "network_mode": "none",
                            "read_only": True,
                            "cap_drop": ["ALL"],
                            "security_opt": ["no-new-privileges:true"],
                            "mem_limit": "256m",
                            "pids_limit": 64,
                            "tmpfs": [
                                # in-container tmpfs mount, not a host path
                                "/tmp:rw,noexec,nosuid,size=67108864,mode=1777",  # noqa: S108
                            ],
                            "volumes": [str(config) + ":/config:ro"],
                            "command": [
                                "-services",
                                "storage",
                                "-identity-config",
                                "/config/identity.json",
                                "-iam-config",
                                "/config/iam.json",
                            ],
                            "healthcheck": {
                                "test": [
                                    "CMD",
                                    "python3",
                                    "-c",
                                    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:4566/_local/health',timeout=1)",
                                ],
                                "interval": "1s",
                                "timeout": "2s",
                                "retries": 15,
                            },
                        }
                    }
                }
            )
        )
        cli = [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "--project-name",
            project,
            "--file",
            str(compose),
        ]
        observations = []
        try:
            command(
                ["docker", "build", "--network=none", "--pull=false", "--tag", tag, str(directory)],
                timeout=120,
            )
            command([*cli, "up", "--detach", "--wait", "--wait-timeout", "25", "--pull", "never"])
            observations.append(
                json.loads(command([*cli, "exec", "-T", "core", "python3", "-c", PROBE]).stdout)
            )
            # Replacement must discard the process-local backend data.
            command(
                [
                    *cli,
                    "up",
                    "--detach",
                    "--force-recreate",
                    "--wait",
                    "--wait-timeout",
                    "25",
                    "--pull",
                    "never",
                ]
            )
            observations.append(
                json.loads(command([*cli, "exec", "-T", "core", "python3", "-c", PROBE]).stdout)
            )
        finally:
            command([*cli, "down", "--timeout", "10"])
            remaining = command(
                ["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project]
            ).stdout
            if remaining.strip():
                raise RuntimeError("Owned Compose probe containers remain")
            command(["docker", "image", "rm", tag], check=False)
    report = {
        "kind": "moo-container-probe",
        "gcp_execution": False,
        "cloud_fidelity": "unverified",
        "docker_server": versions["Server"]["Version"],
        "compose": compose_version,
        "architecture": platform.machine(),
        "base_image": image,
        "source_sha256": hashlib.sha256((ROOT / "bin/moo-container-core").read_bytes()).hexdigest(),
        "network": "none",
        "observations": observations,
        "replacement_reset": True,
        "cleanup": "passed",
        "packaged_bridge_networks": "not-verified",
    }
    path = ROOT / ".artifacts/moo-runtime/container.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
