# Agent setup for GCP application repositories

The portable [gcp-local-setup skill](../skills/gcp-local-setup/SKILL.md) is for application onboarding. It is separate from the contributor skills used to implement gcp-local providers. The bundle inspects an application, checks explicit local readiness, and guides agents through the [packaged environment](quickstart.md). Runtime execution uses Docker with either the `gcp-local` CLI (after the first release) or a separate gcp-local checkout.

From a clean, committed gcp-local checkout, preview the installation into an existing application repository:

```sh
python3 scripts/install_agent_skill.py --repo /absolute/path/to/application
```

Apply the reviewed file plan:

```sh
python3 scripts/install_agent_skill.py --repo /absolute/path/to/application --apply
```

The default destination is `.agents/skills/gcp-local-setup/`. The installer copies only committed bundle files, records their source revision and SHA-256 hashes in `.install.json`, and appends one marked discovery entry to the application's `AGENTS.md`. Existing instructions and unrelated skills remain intact. Repeating the same command is idempotent; installing from an updated source previews the changed managed files. Edited files, unmanaged installations, changed discovery entries, and symlink/path escapes fail before writes.

The source bundle must be committed before installation. This makes the recorded revision reproducible and excludes ignored credentials or untracked files from the copy. Do not force an update over application-specific skill edits; review and reconcile those edits first. No user-wide skill installation or agent configuration is changed.

In the application repository, ask an agent to use `$gcp-local-setup`. An agent without automatic discovery can read `.agents/skills/gcp-local-setup/SKILL.md` directly. Every inspection helper, reference, and profile template travels with the installed folder. Inspection still works if the original checkout is removed; startup separately needs a committed runtime checkout.

The bundled inspector needs only Python 3.11 or newer:

```sh
python3 .agents/skills/gcp-local-setup/scripts/inspect_repo.py --repo . \
  --output .gcp-local/compatibility.json --doctor --endpoint http://127.0.0.1:4566
```

The report lists detected SDK/language references, Terraform resource types, candidate test commands, endpoint variable references, and tool availability. It never executes the application or discovered commands and does not read `.env` or credential files. Only `--doctor --endpoint` performs a bounded health request, with proxies and redirects disabled. Inspection is heuristic, limited to 2,000 selected files of at most 1 MiB each; a truncated report calls for additional agent inspection.

Doctor exit code **2** means the explicit endpoint is unavailable or a detected service is missing. With no endpoint, doctor reports unavailable without network access. A runtime serving every detected service gives a ready report; services it does not serve remain listed. This checks readiness and service availability, not SDK operations or IAM fidelity.

The bundle includes separate local and live profile templates. The live template contains only empty fields and credential-path inputs, uses the [sandbox profile contract](gcp-sandbox.md), and grants no cloud authorization. An application adapter must explicitly choose its endpoint and identity; a missing local service must never trigger a real-GCP fallback.
