# Local ADC and workload identity

The runtime now supports local external-account ADC exchange and a metadata listener bound to one enrolled service account. Tokens are opaque, process-local handles; they are never real GCP credentials. Service APIs still require the IAM/backend implementation before application conformance can pass.

## Developer ADC setup

Build the core as described in [runtime.md](runtime.md), then create a new private bundle:

```sh
python3 scripts/create_local_identity.py \
  --directory .gcp-local/identity \
  --project synthetic-project \
  --principal serviceAccount:app@synthetic-project.iam.gserviceaccount.com \
  --endpoint http://127.0.0.1:4566
bin/gcp-local -identity-config .gcp-local/identity/server.json
```

The directory is mode `0700`, and `server.json`, `subject.txt`, and `adc.json` are mode `0600`. The creator refuses an existing destination and accepts only an explicit numeric loopback HTTP endpoint. `.gcp-local/` is ignored by Git. The command prints file paths, never credential contents. Supply the server file only to the core; the application needs only its own ADC/subject files.

Set these variables in the application's local test environment:

```sh
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.gcp-local/identity/adc.json"
export GOOGLE_CLOUD_PROJECT=synthetic-project
```

Preserve the application's normal SDK credentials and override the API endpoint to `http://127.0.0.1:4566`. Do not enable an emulator switch that disables credentials. The real-GCP `.env`, gcloud configuration, and sandbox ADC files are separate and are not read by the runtime or this generator. The [portable application skill](agent-setup.md) connects this credential flow to the [packaged Storage workflow](quickstart.md).

The ADC profile uses `external_account`, a private file-sourced local subject access token, the local audience `//gcp-local/identity`, and `http://127.0.0.1:4566/v1/token`. This audience deliberately has no GCP project number, so the pinned auth library does not perform cloud project discovery. The project is supplied explicitly. A principal name alone is insufficient to mint a token: the subject credential must match its enrollment. The standard Google auth library performs the token exchange; this local issuer does not validate upstream GCP federation JWT/SAML assertions.

Access tokens live for one hour by default. Set `token_lifetime_seconds` in the private server configuration to a value from 1 to 3600 for refresh testing. Principals default to cloud-platform and the Storage SDK's standard scopes; `scopes` on an enrollment can restrict that set. Requested scopes must be a subset. Scope values are carried in the principal context, and IAM/API authorization remains a separate check. Tokens die when the core restarts. The resolver distinguishes absent, unknown, recently expired, and authenticated credentials. Expired handles are retained for at most an additional hour for diagnostics, with a 10,000-entry limit; pruned handles are unknown.

`GET /_local/identity` reports the requesting caller's state, subject, project, and scopes without exposing the token or enrollment secret. Unknown/expired tokens return 401; an absent header reports an explicit anonymous state. General diagnostics do not list credentials. Unsupported grant types, subject token types, audiences, scopes, or exchange options fail explicitly.

## Workload metadata

Start the core with an optional, separate loopback metadata listener:

```sh
bin/gcp-local -identity-config .gcp-local/identity/server.json \
  -metadata-listen 127.0.0.1:4568 \
  -metadata-principal serviceAccount:app@synthetic-project.iam.gserviceaccount.com
```

For the workload's metadata-based ADC lane, leave `GOOGLE_APPLICATION_CREDENTIALS` unset, use an empty `CLOUDSDK_CONFIG` directory, and set `GOOGLE_CLOUD_PROJECT=synthetic-project`, `GCE_METADATA_HOST=127.0.0.1:4568`, and `GCE_METADATA_IP=127.0.0.1:4568`. Both metadata overrides are needed for the pinned Python library's discovery and retrieval paths. These settings belong only in that workload's environment; do not change the developer's global gcloud configuration.

The handler exposes project ID, service-account email/scopes, account info, and access tokens. It requires `Metadata-Flavor: Google` and rejects forwarded requests. `default` and the configured account's own email resolve to the same account; other emails and attempts to choose another account through query parameters fail. Every handler is bound at creation, never from a caller-provided identity. The current CLI exposes one such listener; #13 must attach separate handlers to isolated workload networks. Account binding alone is not container network isolation.

Metadata ID tokens, general service-account impersonation/signing APIs, service-account JSON keys, and federated JWT/SAML validation remain unsupported. The metadata identity-token path explicitly returns 501. The [Pub/Sub push provider](pubsub.md) can mint short-lived, locally signed ID tokens internally after checking the acting service agent's permission on an explicitly declared local account. It exposes only public certificates at `/_local/identity/certs`; there is no public ID-token mint endpoint. These tokens use a separate local issuer and cannot authenticate to Google. Other service contracts remain gated by their provider coverage.

## Verified SDK matrix

| Client/configuration | Token source and transport | Verification |
| --- | --- | --- |
| Python 3.11/3.12, google-auth 2.56.2, external-account ADC | File-sourced local subject access token → local STS; REST credentials preserved | Discovery, initial token, controlled server/client expiry, automatic refresh, old-token rejection |
| Python 3.11/3.12, google-auth 2.56.2, metadata ADC | Both metadata host/IP overrides; one configured account | Discovery, account binding, controlled expiry, automatic refresh |
| google-cloud-storage 3.10.1 with either credential flow | Explicit API endpoint and credentials; no anonymous emulator switch | Authenticated list calls to a controlled provider fixture before and after refresh |

These are credential/transport checks, not Storage service conformance or real-GCP token-issuer parity. No Go/Java/Node SDK or Pub/Sub emulator-mode identity certification is claimed yet. The google 8.5.0 Terraform provider accepts this ADC shape only with an added `"universe_domain": "gcp-local.invalid"` and `scopes = [cloud-platform]`; the [workflow acceptance](workflow.md#terraform-authentication) is the only evidence. API unknown/expired/anonymous decisions pass to the shared IAM layer in #7; none is promoted to an administrator.

Ordinary Go tests and the built-process smoke check require no Python SDK packages. The integration test additionally needs `requirements-sdk.txt` and runs only in the Linux isolation launcher, which gives the core and SDK subprocess loopback connectivity and blocks cloud egress. Its clock-control route exists only inside the test, never in the built core:

```sh
moo run task-name -- 'cd /srv/app && GCP_LOCAL_TEST_PYTHON=/srv/app/.venv-tools/bin/python .venv-tools/bin/python scripts/run_isolated.py -- sh scripts/go.sh test -tags=sdkintegration -race -count=1 -timeout 60s ./internal/identity'
```

CI runs the same integration after warming the pinned module cache outside the isolated network. The normal build check also generates a private bundle, boots the actual binary, exchanges a token, verifies metadata account binding, and checks shutdown.
