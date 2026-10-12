---
name: gcp-docs
description: Use when a gcp-local change adds or removes a service, provider method, data-plane capability, conformance recording, permission probe, Terraform case, or developer role, or when support claims in the docs may be stale.
---

# gcp-docs

`docs/coverage.md` is the source of truth for what each service supports. Update it first, then update every place that summarizes it. **REQUIRED:** write in the concise-and-blunt style.

## What to update

| Change | Files |
| --- | --- |
| Any change below | `docs/coverage.md` summary row: methods served, data plane, recordings, Terraform |
| Method or capability | The service doc (`docs/<service>.md`): served methods and explicit gaps. Also the README "Application use" column if what an application can do changed |
| Conformance recording | The service doc's recording list (ID, date, what it observes). `docs/coverage.md` recordings column |
| Permission probe | The `docs/coverage.md` cross-service table and `docs/permission-discovery.md` |
| Terraform case | `docs/coverage.md` Terraform column and `docs/terraform-compatibility.md` |
| Evidence level changes (first recording, or modeled to recorded) | `LOCAL_SUPPORT` in `skills/gcp-local-setup/scripts/inspect_repo.py` |
| Developer role | `gcp_local/bootstrap.py` `DEVELOPER_ROLES` and the `docs/quickstart.md` roles table |
| New service | All of the above, plus the README table, `docs/quickstart.md`, a row in the `CONTRIBUTING.md` checks table, and the `SERVICES` list |

Then find stale copies:

```sh
rg -n -i '<service name>|recorded scenario|one recorded|seven|eight' README.md AGENTS.md docs skills .agents
```

Replace any list of services or recording counts outside `docs/coverage.md` with a link to it. Don't add another copy.

## Claims

- **Recorded** means a committed recording under `tests/conformance/recordings/<id>/` with `"source": "gcp"` and `"reviewed": true`, and the local check passed against it. Anything else is "modeled locally" or "not recorded".
- **Methods served** counts methods that run. Routes that return `UNIMPLEMENTED` don't count. Recount from the provider's dispatch or permission plans, not from `api/generated/*_routes.go`, which lists every route.
- Don't write "matches" or "passes" unless you ran the check in this change.

## Verify

```sh
uv run --with 'PyYAML==6.0.3' python scripts/check_repo.py
```

This fails when a packaged service, recording, or Terraform case is missing from `docs/coverage.md`, when the README doesn't link to it, or when any relative link is broken. It doesn't check the method counts.
