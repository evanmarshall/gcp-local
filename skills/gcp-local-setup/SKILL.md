---
name: gcp-local-setup
description: Set up an existing GCP application repository for local Storage testing with gcp-local, including SDK inspection, startup, readiness, credentials, and cleanup. Use for application onboarding, not emulator provider implementation.
---

# GCP application setup

Connect this application to gcp-local while preserving its tools and instructions. The current runtime supports a recorded slice of private Cloud Storage, local ADC, and basic IAM. Pub/Sub, Cloud Run, other service providers, and broader Storage operations remain unsupported; inspect the application's actual operations before claiming compatibility.

## Inspect the application

Run the portable inspector from its installed location:

```sh
python3 .agents/skills/gcp-local-setup/scripts/inspect_repo.py --repo .
```

It recognizes common SDK references, Terraform resource types, candidate test commands, and endpoint variables. It never executes application code or reads credential files. Treat discovery as a starting point; inspect unfamiliar dependencies or omitted files if the report is truncated. Add `--output .gcp-local/compatibility.json` to save a report.

Inspection alone never contacts a service. Add `--doctor --endpoint http://127.0.0.1:4566` to explicitly check one numeric loopback endpoint. Exit code 2 means the runtime is unavailable or a detected service is unsupported. Readiness does not verify credentials, SDK compatibility, or application behavior.

## Start and connect the runtime

Use an available committed gcp-local checkout matching this bundle's recorded source revision, or obtain it when needed. Startup needs Python 3.11+ and a running Docker engine with Compose; no GCP account, global Go installation, or real ADC is needed. From the application:

```sh
python3 /path/to/gcp-local/scripts/local_env.py --directory "$PWD/.gcp-local/environment" up
python3 /path/to/gcp-local/scripts/local_env.py --directory "$PWD/.gcp-local/environment" status
python3 /path/to/gcp-local/scripts/local_env.py --directory "$PWD/.gcp-local/environment" env
```

Replace the checkout path with its actual location. Initial `up` accepts `--port` and `--project` before the action; separate applications need separate directories and ports. `env` emits JSON with the generated ADC path, synthetic project, and endpoint. Apply these values only to the application's local test process, preserving credentials in its SDK. Override the Storage API endpoint explicitly; avoid switches that replace credentials with anonymous ones. The verified Python lane uses standard local ADC and `client_options={"api_endpoint": endpoint}`. Native Terraform ADC remains unverified despite the passing test-only acceptance slice.

Buckets require an explicit location, uniform access, enforced public access prevention, and zero soft-delete duration. The generated developer identity has an explicit local Storage Admin grant. Read the checkout's `docs/storage.md` for operation limits and `docs/quickstart.md` for an SDK example. Keep `.gcp-local/` ignored; never copy generated credentials or server configuration into this skill.

Use `logs` for startup failures. `reset` recreates only this environment and discards its in-memory resources and bucket grants; use it when the task calls for a clean environment. `down` removes its containers/networks while retaining private local credentials for reuse. Do not reset another application's directory.

## Select the validation lane

Read [the profile guide](references/profiles.md) for local versus explicitly authorized live execution. Preserve existing offline tests and the application's AGENTS.md. Inspect discovered commands before execution; the runtime's isolated backend network does not isolate host application subprocesses. Live work needs a designated sandbox and allowed resource scope; reuse existing authorization and request only missing inputs. A local feature gap must stay unsupported or unverified instead of falling back to GCP.

Report detected services, selected directory/endpoint, readiness, tests actually run, and remaining unsupported operations. A running provider does not establish application or IAM compatibility.

## Portability and updates

Inspection scripts and references travel with this folder. Runtime execution separately needs its checkout and Docker. Agents without automatic discovery can read this SKILL.md directly. `.install.json` records the source revision and managed hashes; the source installer previews updates and refuses to overwrite local modifications. Reconcile edited files explicitly before installing an update.
