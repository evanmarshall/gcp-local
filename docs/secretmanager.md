# Secret Manager

Secret Manager v1 over REST: secrets, versions, payload access, per-secret IAM, and location listing. Modeled locally in provider state. The scenario `secretmanager-access-lifecycle` is recorded against GCP.

## Use

Select `-services ...,secretmanager`. Routes are `/v1/projects/{project}/secrets/...` on port 4566. Names use the project number (`projects/NUMBER/secrets/...`); callers may send either form.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Secrets | `create`, `get`, `list`, `patch`, `delete` | Automatic replication |
| Secrets | `getIamPolicy`, `setIamPolicy` | |
| Versions | `addVersion`, `list`, `get` | Payloads up to 64 KiB; metadata reads never return the payload |
| Versions | `access` | Returns `dataCrc32c`; `latest` is the newest enabled version |
| Versions | `disable`, `enable`, `destroy` | Access to a disabled or destroyed version is 400 `FAILED_PRECONDITION` |
| Locations | `list` | |

## IAM

- `secretmanager.versions.access` on the secret reads payloads (`roles/secretmanager.secretAccessor`).
- An accessor cannot read version metadata or add versions.
- Pinned roles: `roles/secretmanager.admin`, `.secretAccessor`, `.viewer`.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `secretmanager-access-lifecycle` | 2026-10-11 | Create and addVersion fields, number-form names, denied then granted access, `latest`, 64 KiB limit, disabled and destroyed access, double destroy, delete then 404 |

```sh
sh scripts/check_rest_recordings.sh
```

To re-record, get an approved `.gcp-local/secretmanager-scope.json` (see `LiveScope` in `gcp_validation/secretmanager.py`) and a temporary `roles/secretmanager.admin` on the fixture identity:

```sh
.venv-tools/bin/python -m pytest tests/conformance/secretmanager -q \
  --gcp-mode record --gcp-profile /absolute/path/to/.env \
  --gcp-secretmanager-scope /absolute/path/to/.gcp-local/secretmanager-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/secretmanager-record
```

After an interrupted run, `python -m gcp_validation.secretmanager cleanup --profile ... --scope ... --manifest ... --allow-live` deletes only the secret in the manifest.

## Gaps

- `list`, `update`, `enable`, IAM policy reads, and locations are not recorded.
- No user-managed replication, CMEK, rotation, expiration, topics, or version aliases.
- Error messages are not compared. State is in memory.
