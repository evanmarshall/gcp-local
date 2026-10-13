---
name: gcp-local-setup
description: Set up an existing GCP application repository for local GCP testing with gcp-local, including SDK inspection, startup, readiness, credentials, and cleanup. Use for application onboarding, not emulator provider implementation.
---

# GCP application setup

Connect this application to gcp-local while preserving its tools and instructions. The packaged environment selects every implemented provider with local ADC and explicit local IAM grants for its developer identity. Evidence differs by service: Storage has the most cloud-recorded scenarios, other services one to four (Cloud Run one, over REST), and Pub/Sub push is locally modeled only; the checkout's `docs/coverage.md` lists what each service supports. Packaged Cloud Run runs only the bundled workflow worker image unless the environment starts with `gcp-local --cloud-run-docker up`, which runs application images as containers and mounts the Docker socket (root-equivalent on the host); ask before enabling it. Inspect the application's actual operations before claiming compatibility.

## Inspect the application

Run the portable inspector from its installed location:

```sh
python3 .agents/skills/gcp-local-setup/scripts/inspect_repo.py --repo .
```

It recognizes common SDK references, Terraform resource types, candidate test commands, and endpoint variables. It never executes application code or reads credential files. Treat discovery as a starting point; inspect unfamiliar dependencies or omitted files if the report is truncated. Add `--output .gcp-local/compatibility.json` to save a report.

Inspection alone never contacts a service. Add `--doctor --endpoint http://127.0.0.1:4566` to explicitly check one numeric loopback endpoint. Exit code 2 means the runtime is unavailable or a detected service is unsupported. Readiness does not verify credentials, SDK compatibility, or application behavior.

## Start and connect the runtime

Use the `gcp-local` CLI when it is installed (`pip install gcp-local`, available after the first release; it takes the same arguments), or an available committed gcp-local checkout matching this bundle's recorded source revision. Startup needs Python 3.11+ and a running Docker engine with Compose; no GCP account, global Go installation, or real ADC is needed. From the application, with a checkout:

```sh
python3 /path/to/gcp-local/scripts/dev/local_env.py --directory "$PWD/.gcp-local/environment" up
python3 /path/to/gcp-local/scripts/dev/local_env.py --directory "$PWD/.gcp-local/environment" status
python3 /path/to/gcp-local/scripts/dev/local_env.py --directory "$PWD/.gcp-local/environment" env
```

Replace the checkout path with its actual location. Initial `up` accepts `--port`, `--project`, and `--workload ACCOUNT_ID` (Cloud Run workload identities) before the action; separate applications need separate directories and ports. `env` emits JSON with the generated ADC path, synthetic project, and endpoint. Apply these values only to the application's local test process, preserving credentials in its SDK. Override the Storage API endpoint explicitly; avoid switches that replace credentials with anonymous ones. The verified Python lane uses standard local ADC and `client_options={"api_endpoint": endpoint}`. For Terraform, use the `GCP_LOCAL_TERRAFORM_CREDENTIALS` file from `env` with the provider settings in the checkout's `docs/guides/workflow.md`; that configuration is verified only by the emulator's workflow acceptance.

Buckets require an explicit location and uniform access; public access prevention and soft delete may stay at GCP's defaults. The generated developer identity has explicit local project grants for each packaged service. Read the checkout's `docs/services/storage.md` for operation limits and `docs/guides/quickstart.md` for an SDK example. Keep `.gcp-local/` ignored; never copy generated credentials or server configuration into this skill.

For applications using Pub/Sub, read that checkout's `docs/services/messaging/pubsub.md` before setup. The packaged environment starts it with a pinned Java 17 runtime and the emulator JAR, and publishes its gRPC gateway on loopback. The verified Python transport preserves ADC using loopback gRPC call credentials; keep `PUBSUB_EMULATOR_HOST` unset. A detected publish/subscribe dependency does not prove push compatibility: current push supports authenticated, same-project, numeric loopback HTTP receivers only.

Use `logs` for startup failures. `reset` recreates only this environment and discards its in-memory resources and resource-level grants; use it when the task calls for a clean environment. `down` removes its containers/networks while retaining private local credentials for reuse. Do not reset another application's directory.

## Select the validation lane

Read [the profile guide](references/profiles.md) for local versus explicitly authorized live execution. Preserve existing offline tests and the application's AGENTS.md. Inspect discovered commands before execution; the runtime's isolated backend network does not isolate host application subprocesses. Live work needs a designated sandbox and allowed resource scope; reuse existing authorization and request only missing inputs. A local feature gap must stay unsupported or unverified instead of falling back to GCP.

Report detected services, selected directory/endpoint, readiness, tests actually run, and remaining unsupported operations. A running provider does not establish application or IAM compatibility.

## Portability and updates

Inspection scripts and references travel with this folder. Runtime execution separately needs Docker and the CLI or a checkout. Agents without automatic discovery can read this SKILL.md directly. `.install.json` records the source revision and managed hashes; the source installer previews updates and refuses to overwrite local modifications. Reconcile edited files explicitly before installing an update.
