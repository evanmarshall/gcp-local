# Agent setup for application repositories

The [gcp-local-setup skill](../../skills/gcp-local-setup/SKILL.md) teaches a coding agent to connect your application to the [packaged environment](quickstart.md). Install it from a clean, committed gcp-local checkout:

```sh
python3 scripts/dev/install_agent_skill.py --repo /absolute/path/to/application          # preview
python3 scripts/dev/install_agent_skill.py --repo /absolute/path/to/application --apply  # install
```

It copies the committed bundle to `.agents/skills/gcp-local-setup/`, records the source revision and file hashes in `.install.json`, and appends one entry to the application's `AGENTS.md`. Rerunning is idempotent; installing from a newer checkout previews the changed files.

Then ask the agent to use `$gcp-local-setup`. Agents without skill discovery can read `.agents/skills/gcp-local-setup/SKILL.md` directly.

## Check readiness

The inspector needs only Python 3.11+:

```sh
python3 .agents/skills/gcp-local-setup/scripts/inspect_repo.py --repo . \
  --output .gcp-local/compatibility.json --doctor --endpoint http://127.0.0.1:4566
```

It reports detected SDKs, Terraform resource types, test commands, endpoint variables, and tool availability. Exit code 2 means the endpoint is down or a detected service isn't served.

## Limits

- The installer refuses edited managed files, unmanaged installations, changed `AGENTS.md` entries, and paths that escape the repository. It writes nothing in that case.
- The inspector never runs your code and doesn't read `.env` or credential files. Only `--doctor --endpoint` makes a network request.
- Inspection is heuristic: at most 2,000 files of up to 1 MiB each.
- A ready report means the services are served, not that your SDK calls or IAM setup work.
- The bundle's live profile template has only empty fields ([sandbox profile](../contributing/gcp-sandbox.md)) and grants no cloud access. A missing local service must never fall back to real GCP.
- Starting the environment needs Docker and either the `gcp-local` CLI or a gcp-local checkout.
