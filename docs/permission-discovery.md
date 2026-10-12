# Permission discovery and drift

Status: orchestration and cleanup are tested offline against controlled fake responses
(`tests/validation/test_permission_probe.py`). **No live probe has run.** No permission set
in this repository has been established by this tooling yet, and the live plan below is
prepared, not executed. Issue: [#15](https://github.com/evanmarshall/gcp-local/issues/15).

The runner implements the probe and minimization steps of [the design](design.md#probing-algorithm)
for one method at a time. The first target is Secret Manager `secretmanager.versions.access`.

## How a run works

1. **Ownership first.** An owner-only manifest under
   `~/.local/state/gcp-local/PROJECT/permissiondiscovery/` names the secret, custom role, and
   attachment point before anything is created, together with a project lock (`OwnedRun` from
   `gcp_validation/live_rest.py`). Each creation is flagged in the manifest before its request,
   so a lost response is still cleaned up.
2. **Fixture.** The `fixture` identity creates one labeled secret with one enabled version and
   checks that the secret has no IAM bindings.
3. **Seed.** Candidates are the union of the committed, checksummed snapshots of
   `roles/secretmanager.secretAccessor` and `roles/secretmanager.viewer`. The digests are kept in
   the report. `queryTestablePermissions` on the attachment point separates candidates into
   grantable ones (custom-role support `SUPPORTED` or `TESTING`) and excluded ones
   (`NOT_SUPPORTED`, or `NOT_APPLICABLE` when the permission cannot be tested there). Excluded
   permissions are reported, never minimized.
4. **Control.** The probe identity (the sandbox `allowed` caller) must hold none of the
   candidates (`testIamPermissions`) and must be denied. Otherwise the run stops before any grant.
5. **Baseline.** One project-level custom role holding every candidate is bound to the probe
   identity at the explicit attachment point: the secret (default) or the project.
6. **Minimize.** Remove one candidate at a time. A denial marks it required, then a restoration
   trial must succeed again before the next removal. Success that persists for
   `removal_confirmations` polls after the hint converged marks it as unneeded for now. An
   empty set is applied by removing the binding, never by sending an empty role.
7. **Confirm.** The final set must keep succeeding for the full propagation bound after the last
   grant change. This soak catches a revocation that lagged its `testIamPermissions` hint, which
   would otherwise make a required permission look removable.
8. **Cleanup.** Always attempted, without the run budget: remove only the probe member from the
   probe-role binding, delete the custom role after checking its run-ID description, and delete
   the secret after checking its name and labels.

Every trial records its kind, removed permission, granted set, acting identity, request
features, check target, grant location, preconditions, propagation (hint state, attempts,
waited seconds, operation polling), outcome (HTTP status, `status`, ErrorInfo `reason`,
inconclusive detail), and whether fixture state matched the initial snapshot before and after.
Reports alias the project, project number, run ID, and identities, and never record error
messages or payloads.

## Decision rules

- Only an IAM `PERMISSION_DENIED` or a completed success is conclusive. `ALREADY_EXISTS`,
  `INVALID_ARGUMENT`, quota, `NOT_FOUND`, transport failures, and 403s that carry a non-IAM
  ErrorInfo reason (`SERVICE_DISABLED`, `USER_PROJECT_DENIED`, `BILLING_DISABLED`, scope, or
  VPC-SC) are inconclusive. An inconclusive removal leaves the permission undetermined and makes
  the run inconclusive.
- Propagation waits are bounded by `max_propagation_seconds` with a fixed poll interval.
  `testIamPermissions` is used only as a readiness hint; the real call decides. A timeout is
  inconclusive.
- A long-running operation counts as successful only after it reports `done` without an error.
  A completed `PERMISSION_DENIED` operation is a denial, other errors are inconclusive, and an
  unfinished operation within `max_operation_seconds` is inconclusive.
- The fixture state (secret labels and replication, version states, non-probe bindings) is
  snapshotted before and after every trial. A mismatch stops the run as inconclusive.
- Only a conclusive run reports `minimal_permissions`. Required findings stay listed in an
  inconclusive run. Removals that looked unnecessary move to `unconfirmed_not_required`.
- A minimal set found by greedy removal is one sufficient set for this scenario. It does not prove
  every authorization path.

## Limits

The scope file must match `LIMITS` in `gcp_validation/permission_secretmanager.py` exactly
(`LiveScope` from `live_rest.py`, service `permissiondiscovery`). Hitting any limit stops the
run as inconclusive. Cleanup still runs.

| Limit | Value | Bounds |
| --- | --- | --- |
| `max_trials` | 16 | control + baseline + one per candidate + restorations + confirmation |
| `max_role_updates` | 12 | custom-role `PATCH` calls |
| `max_custom_roles` | 1 | role creations |
| `max_secrets` | 1 | secret creations (one version) |
| `max_requests` | 800 | every probe-phase request, including polls |
| `max_run_seconds` | 7200 | the whole probe phase |
| `max_propagation_seconds` | 420 | per-trial wait, and the confirmation soak |
| `max_operation_seconds` | 60 | operation polling (unused by this synchronous target) |
| `poll_interval_seconds` | 15 | hint, call, and operation polls |
| `removal_confirmations` | 3 | consecutive successes before a removal looks unneeded |

## Drift

`tools/permission-plans` exports the permission plans a provider registers with the IAM engine,
without starting a backend or using the network. `drift` compares a conclusive report with that
export for the scenario's request features and with the committed role snapshots. It writes
`drift.json` and `drift.md` to a new directory with `applied: false`. It proposes reviewed
actions: plan additions or extra checks, unresolved features, snapshot re-recording, changed seed
snapshots, and unprobed exclusions. An inconclusive report produces no proposal. It never edits
providers, plans, or snapshots.

```sh
sh scripts/go.sh run ./tools/permission-plans -service secretmanager > /tmp/plans.json
.venv-tools/bin/python -m gcp_validation.permission_cli drift \
  --report .artifacts/permission-probe/RUN/report.json --plans /tmp/plans.json \
  --out .artifacts/permission-drift/RUN
```

## First live result

Executed against the sandbox on 2026-10-12 (UTC) with the plan below and the secret attachment.
Result: **conclusive**, minimal set `{secretmanager.versions.access}` over 11 trials (control,
baseline, six removals, a restoration after the required permission was removed, and the final
confirmation); `secretmanager.secrets.get`, `secrets.getIamPolicy`, `secrets.listEffectiveTags`,
`secrets.listTagBindings`, `versions.get`, and `versions.list` were proven unnecessary for this
request. Five candidates were excluded as `NOT_APPLICABLE` at a secret attachment. Cleanup passed,
the temporary fixture grants were revoked, and the drift report shows **no drift** against the
local Secret Manager plan. Evidence: `tests/permission/recordings/secretmanager-versions-access/`.

The first attempt stopped inconclusive before any trial: `queryTestablePermissions` returns 400
INVALID_ARGUMENT for a secret named with the project ID and only accepts the project-number form
(project resources accept the ID). The runner now sends the number form.

## Live plan

Expected result, a hypothesis rather than evidence: `{secretmanager.versions.access}`. This
follows Google's Secret Manager access-control documentation and the recorded
`secretmanager-access-lifecycle` scenario, where a secret-level `secretAccessor` grant was
enough.

Grants and prerequisites for the default secret attachment:

| Who | Grant | Scope | Why |
| --- | --- | --- | --- |
| fixture SA | `roles/secretmanager.admin` (temporary) | project | create/read/delete the one labeled secret, add its version, set its IAM policy |
| fixture SA | `roles/iam.roleAdmin` (temporary) | project | create, update, and delete the one project-level custom role |
| allowed SA (probe) | none new | none | must hold no candidate permission (the control trial checks), keeps Service Usage Consumer for the `x-goog-user-project` header |
| denied SA | none | none | not used |

The `--attachment project` variant also needs `roles/resourcemanager.projectIamAdmin` on the
fixture identity to add and remove the probe binding on the project policy. It is not part of
the first live run. The APIs `secretmanager.googleapis.com`, `iam.googleapis.com`, and
`cloudresourcemanager.googleapis.com` must be enabled. Resources created: one secret, one
version, one custom role, one binding. Secret Manager charges for one active version for under
two hours, plus a few hundred access operations. IAM calls are free. A deleted custom role
counts toward the 300-roles-per-project quota until it is purged, and its ID cannot be reused.
Each run uses a new run-ID role.

Steps for the owner's operator, with `requirements-cloud.txt` installed:

```sh
# 1. Offline: print the plan (with the exact gcloud grant/revoke commands) and write the
#    scope file unapproved. Review it, then set "approved": true yourself.
.venv-tools/bin/python -m gcp_validation.permission_cli plan \
  --profile /absolute/path/to/.env --write-scope /absolute/path/to/.gcp-local/permission-scope.json

# 2. Owner: apply the two temporary fixture grants printed under owner_commands (add-iam-policy-binding).

# 3. Live probe; writes an unreviewed report candidate.
.venv-tools/bin/python -m gcp_validation.permission_cli run \
  --profile /absolute/path/to/.env --scope /absolute/path/to/.gcp-local/permission-scope.json \
  --attachment secret --out .artifacts/permission-probe/$(date +%Y%m%d) --allow-live

# 4. Only if step 3 printed a manifest path (cleanup failed or the run was killed):
.venv-tools/bin/python -m gcp_validation.permission_cli cleanup \
  --profile /absolute/path/to/.env --scope /absolute/path/to/.gcp-local/permission-scope.json \
  --manifest ~/.local/state/gcp-local/PROJECT/permissiondiscovery/RUN_ID.json --allow-live

# 5. Owner: revoke both temporary fixture grants (remove-iam-policy-binding).
# 6. Offline: drift as above; review before changing anything.
```

Verified by the first run: `queryTestablePermissions` worked with the fixture identity; it needs
the project-number resource form for secrets. Still unverified: the permissions it needs (assumed none beyond
authentication) and the support levels it returns for these candidates; whether every
secret-testable candidate is accepted by the secret's `testIamPermissions`; real
grant/revocation propagation times against the 420-second bound; and the response shape of
custom-role `PATCH`/`DELETE`. A first live run that stops inconclusive for any of these reasons
is the expected safe outcome. It is not a defect to hide by widening limits.
