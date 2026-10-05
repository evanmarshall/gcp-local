# Moo runtime capability checks

The probes for [#25](https://github.com/evanmarshall/gcp-local/issues/25) distinguish guest-isolated API tests, host-forwarded transport, and Docker execution. They do not establish GCP fidelity or make Moo a CI/runtime dependency.

## Observed on 2026-10-05

The task guest used Linux 6.12.91/aarch64, Debian 12, two CPUs, and 2 GiB memory. The retained host Moo binary SHA-256 was `16a21f6734bad9f5a9fdfe4327030b6b199d3272aba163072db5ac6dd39c925b`; doctor does not attest its release version.

| Capability | Observed result |
| --- | --- |
| Guest-isolated official Pub/Sub | Publish/pull/ack, streaming, send half-close, cancellation, shutdown, restart state loss passed with loopback-only egress |
| Host-forwarded official Pub/Sub | Same gRPC operations and cancellation passed through host loopback → guest port 8080; restart lost the expected temporary resources |
| TCP send half-close | The receiver read EOF and returned its acknowledgement successfully; the upstream-documented forwarding limitation did not reproduce for this binary and test |
| Backend samples | Two warm starts: 0.252/0.252 s; RSS after probe: 118476/108432 KiB; shutdown: 0.007/0.017 s. Individual observations, not performance guarantees |
| Docker default bridge | Failed: the guest kernel lacks the needed NAT/MASQUERADE support. Legacy iptables NAT was unavailable too; `/lib/modules` was absent |
| Docker with `network_mode: none` | Passed native arm64 core/Storage execution, local token exchange, read/write, replacement/reset, and cleanup under Compose |
| Packaged multi-network workflow inside Moo | Unsupported by this verified setup. The Docker networking-disabled diagnostic does not establish private bridge/network isolation |

The Docker diagnostic used Debian's Docker 20.10.24+dfsg1, Compose 5.1.3, and the exact digest-pinned Python base image selected in the repository Dockerfile. A temporary diagnostic image embeds the compiled core, with synthetic identity/IAM mounted read-only. It executes actual Storage in a read-only, capability-dropped container with no network interfaces except loopback, then recreates it to verify state loss. The test removes its own containers and image. These are measured compatibility versions, not a recommended production Docker baseline; the pinned Compose asset is published in [Docker's release](https://github.com/docker/compose/releases/tag/v5.1.3).

## Reproduce protocols

Use a separate task worktree/machine with the pinned host Go toolchain, and Java 17 plus the verified Pub/Sub JAR in the guest. Guest port 8080 must be free:

```sh
python3 scripts/install_go.py
python3 scripts/smoke_moo_protocols.py --machine TASK_NAME
```

The host runner identifies the selected machine's forwarded port, starts only disposable owned guest listeners, invokes the actual Pub/Sub probe, checks restart and TCP half-close, and stops the processes. It then runs the stronger isolated guest test separately. Results are written to `.artifacts/moo-runtime/protocol.json` on the host. The `-moo-forwarded` flag is explicit, numeric-loopback-only, disables proxies, and reports its forwarding mode; the normal backend runner still requires Linux network isolation. Neither mode selects a cloud endpoint or credentials.

## Reproduce the limited container diagnostic

Install Docker and the checksum-pinned Compose plugin only in a disposable task guest:

```sh
moo run TASK_NAME -- 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io'
moo run TASK_NAME -- 'cd /srv/app && python3 scripts/install_moo_compose.py'
```

The default Docker daemon failed at bridge NAT initialization on the measured guest. For the **network-none diagnostic only**, start a private daemon with an explicit Unix socket, separate data/exec roots and PID file, `--bridge=none --iptables=false --ip6tables=false`. Keep its logs and stop that owned daemon before saving the machine. Do not point the probe at a host or remote Docker engine, and do not use this configuration to claim bridge-network isolation.

After pulling the Dockerfile's pinned base image into that private daemon, execute:

```sh
moo run TASK_NAME -- 'cd /srv/app && python3 scripts/smoke_moo_docker.py --docker-host unix:///run/gcp-local-docker.sock'
```

The probe requires the image to be present, builds with `--network=none`, selects a unique Compose project, and writes `.artifacts/moo-runtime/container.json` inside the guest. It verifies only the declared diagnostic scope. The shared Moo baseline is unchanged and contains no Docker daemon or real credentials.

## Consequence for Cloud Run development

Use Moo for Go/provider/unit tests and isolated emulator scenarios. Use the already tested host Docker engine or ordinary Linux Docker CI for Cloud Run container/network acceptance until a Moo kernel configuration supplies and passes the required bridge isolation. Do not weaken a provider's network or identity checks to accommodate this developer-machine limitation. A newer Moo binary/kernel can be evaluated by rerunning these probes and the actual packaged workflow.
