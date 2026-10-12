# Permission discovery and drift

The probe runner finds a minimal permission set for one method by granting candidates in a real sandbox and removing them one at a time. It has run once, against Secret Manager `secretmanager.versions.access` ([#15](https://github.com/evanmarshall/gcp-local/issues/15)). The drift tool compares a result with the permission plans the providers register.

## Recorded result

`tests/permission/recordings/secretmanager-versions-access/`, run 2026-10-12 UTC with a secret-level grant:

- **Conclusive.** Minimal set `{secretmanager.versions.access}` over 11 trials.
- Proven unnecessary: `secretmanager.secrets.get`, `secrets.getIamPolicy`, `secrets.listEffectiveTags`, `secrets.listTagBindings`, `versions.get`, `versions.list`.
- Excluded as `NOT_APPLICABLE` at a secret: five candidates.
- Cleanup passed. Drift against the local Secret Manager plan: none.

`queryTestablePermissions` rejects a secret named by project ID with 400; the runner sends the project-number form.

## How a run works

1. **Ownership.** An owner-only manifest under `~/.local/state/gcp-local/PROJECT/permissiondiscovery/` names every resource before it is created, under a project lock.
2. **Fixture.** The `fixture` identity creates one labeled secret with one version and checks it has no bindings.
3. **Seed.** Candidates are the union of the committed `roles/secretmanager.secretAccessor` and `roles/secretmanager.viewer` snapshots. `queryTestablePermissions` excludes those custom roles can't hold; excluded permissions are reported, never minimized.
4. **Control.** The probe identity (the sandbox `allowed` caller) must hold no candidate and must be denied, or the run stops before any grant.
5. **Baseline.** One project-level custom role with every candidate is bound to the probe identity at the secret (default) or the project.
6. **Minimize.** Remove one candidate. A denial marks it required, and a restoration trial must succeed before the next removal. Success over `removal_confirmations` polls marks it unneeded.
7. **Confirm.** The final set must keep succeeding for the full propagation bound.
8. **Cleanup.** Always runs: remove the probe binding, delete the custom role, delete the secret, each after checking ownership.

Reports alias project, number, run ID, and identities, and never record error messages or payloads.

## Decision rules

- Only an IAM `PERMISSION_DENIED` or a completed success is conclusive. `ALREADY_EXISTS`, `INVALID_ARGUMENT`, quota, `NOT_FOUND`, transport errors, and 403s with a non-IAM reason (`SERVICE_DISABLED`, `USER_PROJECT_DENIED`, `BILLING_DISABLED`, scope, VPC-SC) are inconclusive.
- `testIamPermissions` is a readiness hint; the real call decides. A propagation timeout is inconclusive.
- A long-running operation succeeds only when `done` without error.
- Fixture state is snapshotted before and after every trial. A mismatch stops the run as inconclusive.
- Only a conclusive run reports `minimal_permissions`. One minimal set is not proof of every authorization path.

## Limits

The scope file must match `LIMITS` in `gcp_validation/permission_secretmanager.py`. Hitting a limit stops the run as inconclusive; cleanup still runs.

| Limit | Value |
| --- | --- |
| `max_trials` | 16 |
| `max_role_updates` | 12 |
| `max_custom_roles` | 1 |
| `max_secrets` | 1 |
| `max_requests` | 800 |
| `max_run_seconds` | 7200 |
| `max_propagation_seconds` | 420 |
| `max_operation_seconds` | 60 |
| `poll_interval_seconds` | 15 |
| `removal_confirmations` | 3 |

## Run a probe

Needs `requirements-cloud.txt` and an authorized sandbox. The fixture identity needs temporary `roles/secretmanager.admin` and `roles/iam.roleAdmin` on the project; `--attachment project` also needs `roles/resourcemanager.projectIamAdmin`. Each run creates one secret, one version, one custom role, and one binding. A deleted custom role counts toward the 300-per-project quota until purged.

```sh
# 1. Offline: print the plan and gcloud grant commands; write an unapproved scope file.
#    Review it and set "approved": true yourself.
.venv-tools/bin/python -m gcp_validation.permission_cli plan \
  --profile /absolute/path/to/.env --write-scope /absolute/path/to/.gcp-local/permission-scope.json

# 2. Owner applies the printed temporary fixture grants.

# 3. Live probe; writes an unreviewed report.
.venv-tools/bin/python -m gcp_validation.permission_cli run \
  --profile /absolute/path/to/.env --scope /absolute/path/to/.gcp-local/permission-scope.json \
  --attachment secret --out .artifacts/permission-probe/$(date +%Y%m%d) --allow-live

# 4. Only if step 3 printed a manifest path:
.venv-tools/bin/python -m gcp_validation.permission_cli cleanup \
  --profile /absolute/path/to/.env --scope /absolute/path/to/.gcp-local/permission-scope.json \
  --manifest ~/.local/state/gcp-local/PROJECT/permissiondiscovery/RUN_ID.json --allow-live

# 5. Owner revokes the temporary grants.
```

An inconclusive first run is a safe outcome. Don't widen limits to hide it.

## Drift

`tools/permission-plans` exports registered permission plans offline. `drift` compares a conclusive report with that export and the role snapshots, and writes proposals to `drift.json` and `drift.md` with `applied: false`. It never edits providers, plans, or snapshots. An inconclusive report produces no proposal.

```sh
sh scripts/go.sh run ./tools/permission-plans -service secretmanager > /tmp/plans.json
.venv-tools/bin/python -m gcp_validation.permission_cli drift \
  --report .artifacts/permission-probe/RUN/report.json --plans /tmp/plans.json \
  --out .artifacts/permission-drift/RUN
```
