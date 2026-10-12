# Local identity

gcp-local issues its own credentials: an external-account ADC file that exchanges a local subject token at the core's `/v1/token`, and an optional metadata server bound to one service account. Tokens are opaque local handles and never work against Google. The [packaged environment](quickstart.md) creates all of this for you; this page is for running `bin/gcp-local` directly ([runtime](../architecture/runtime.md)).

## Developer ADC

Create a private identity bundle and start the core with it:

```sh
python3 scripts/dev/create_local_identity.py \
  --directory .gcp-local/identity \
  --project synthetic-project \
  --principal serviceAccount:app@synthetic-project.iam.gserviceaccount.com \
  --endpoint http://127.0.0.1:4566
bin/gcp-local -identity-config .gcp-local/identity/server.json
```

Then, in the application's test environment:

```sh
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.gcp-local/identity/adc.json"
export GOOGLE_CLOUD_PROJECT=synthetic-project
```

Set each client's API endpoint to `http://127.0.0.1:4566` and keep the SDK's normal credential loading. Don't use emulator switches that disable credentials.

| File (mode 0600, directory 0700) | Used by |
| --- | --- |
| `server.json` | The core only |
| `subject.txt`, `adc.json` | The application |
| `terraform-adc.json` | Terraform; `adc.json` plus `"universe_domain": "gcp-local.invalid"` ([Terraform setup](workflow.md#terraform-authentication)) |

- The creator refuses an existing directory and accepts only a numeric loopback HTTP endpoint.
- Access tokens last one hour. Set `token_lifetime_seconds` (1-3600) in `server.json` to test refresh.
- Tokens carry the requested [OAuth scopes](#oauth-scopes). An exchange with no or an empty `scope` is 400 `invalid_request`, "Scope(s) must be provided.", as real STS answers an external-credential exchange (probed 2026-10-12 with a workload identity provider; not a recorded scenario). Client libraries send their default scopes; code that loads ADC itself must pass scopes, for example `google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])`.
- The exchange accepts OAuth client authentication: one `Authorization: Basic` header (gcloud sends its client ID this way) or `client_id` and optional `client_secret` form fields, never both. The values grant nothing; the subject token alone picks the principal. Any other `Authorization` header is 401 `invalid_client`.
- `POST /v1/introspect` answers RFC 7662 introspection with client authentication. gcloud calls it to name the account: `username` is the enrolled subject's email. ADC files written by gcp-local set `token_info_url` to it. Real STS introspection is not recorded.
- Tokens die when the core restarts.
- `GET /_local/identity` reports the caller's state, subject, project, and scopes. Unknown or expired tokens get 401.

## Groups and domains

Group and domain membership is declared in the `-iam-config` file's `directory`, not in `server.json`, and is never read from Google: [groups and domains](../architecture/iam.md#groups-and-domains). An enrolled principal needs no change to be a group member; the directory names it.

## OAuth scopes

The token service issues only scopes from an allowlist taken from [Google's OAuth 2.0 scope reference](https://developers.google.com/identity/protocols/oauth2/scopes): `cloud-platform` and scopes narrower than it, plus identity scopes that grant no API call. An exchange or metadata token request naming any other scope is refused with `invalid_scope` and the scope's name. An enrollment's `scopes` must come from the list and restrict its tokens to those scopes.

IAM checks the token's scopes before policy, as GCP does: when no scope covers every permission the method's plan resolves to, the call is 403 `PERMISSION_DENIED`, "Request had insufficient authentication scopes.", reason `ACCESS_TOKEN_SCOPE_INSUFFICIENT`. Coverage is modeled from the scopes each API's discovery document accepts and is not recorded.

| Scope (`https://www.googleapis.com/auth/` unless shown) | Covers |
| --- | --- |
| `cloud-platform` | Everything |
| `cloud-platform.read-only` | `get` and `list` permissions of every service |
| `devstorage.full_control` | Storage |
| `devstorage.read_write` | Storage except `*.update`, IAM policy, and HMAC keys (storage v1 discovery: those take `full_control`) |
| `devstorage.read_only` | Storage `get` and `list` |
| `pubsub`, `datastore`, `cloudkms`, `iam`, `bigquery`, `compute`, `logging.admin`, `sqlservice.admin` (`cloudsql.*`), `appengine.admin` | That service's permissions |
| `bigquery.readonly`, `compute.readonly`, `logging.read` | That service's `get` and `list` |
| `bigquery.insertdata` | `bigquery.tables.updateData` |
| `logging.write` | `logging.logEntries.create` |
| `spanner.admin` | Spanner except the data API |
| `spanner.data` | Spanner sessions and database reads, writes, queries, and transactions |
| `bigtable.admin`, `cloud-bigtable.admin` | Bigtable except row reads and mutations |
| `bigtable.admin.table`, `cloud-bigtable.admin.table` | Bigtable tables and backups, except row reads and mutations |
| `bigtable.admin.cluster`, `bigtable.admin.instance`, `cloud-bigtable.admin.cluster` | Bigtable instances, clusters, and app profiles |
| `bigtable.data`, `cloud-bigtable.data` | Bigtable row reads, sample keys, and mutations |
| `bigtable.data.readonly`, `cloud-bigtable.data.readonly` | Bigtable row reads and sample keys |
| `openid`, `email`, `userinfo.email`, `userinfo.profile`, `sqlservice.login` | Nothing (identity and Cloud SQL database login) |

The read-only scopes are approximated by permission verb, so a read the API accepts under them but whose permission is not `get` or `list` (a BigQuery query, for example) is refused. Secret Manager, Cloud Tasks, Cloud Scheduler, Cloud Run, Artifact Registry, Memorystore, Vertex AI, and Resource Manager accept only `cloud-platform` (and, for reads, `cloud-platform.read-only`).

## Workload metadata

Start the core with a loopback metadata listener bound to one enrolled service account:

```sh
bin/gcp-local -identity-config .gcp-local/identity/server.json \
  -metadata-listen 127.0.0.1:4568 \
  -metadata-principal serviceAccount:app@synthetic-project.iam.gserviceaccount.com
```

In the workload's environment only, unset `GOOGLE_APPLICATION_CREDENTIALS`, point `CLOUDSDK_CONFIG` at an empty directory, and set:

```sh
export GOOGLE_CLOUD_PROJECT=synthetic-project
export GCE_METADATA_HOST=127.0.0.1:4568
export GCE_METADATA_IP=127.0.0.1:4568
```

The Python auth library needs both metadata variables. The server returns the project ID, the account's email and scopes, and access tokens. It requires `Metadata-Flavor: Google`, rejects forwarded requests, and resolves only `default` and the bound account's email.

## Verified clients

| Client | Verified |
| --- | --- |
| google-auth 2.56.2 (Python 3.11, 3.12), external-account ADC | Token exchange, expiry, automatic refresh, old-token rejection |
| google-auth 2.56.2, metadata ADC | Account binding, expiry, automatic refresh |
| google-cloud-storage 3.10.1, either flow | Authenticated calls before and after refresh |
| Terraform google provider 8.5.0 | Only through the [workflow](workflow.md#terraform-authentication), with `terraform-adc.json` and the `cloud-platform` scope |
| gcloud 588.0.0, `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` | Exchange with Basic client authentication and gcloud's default scopes, introspection |

Go, Node.js, Python, and gcloud clients are verified per service in the [client matrix](../contributing/clients.md), with each library's default scopes on REST. Java is not verified.

## Limits

- Metadata ID tokens return 501.
- Service-account keys and `signBlob` sign ([service accounts](../services/security/serviceaccounts.md)) but mint no tokens. No impersonation, `signJwt`, or JWT/SAML federation validation.
- One metadata listener per core; account binding is not network isolation.
- [Pub/Sub push](../services/messaging/pubsub.md) mints locally signed ID tokens internally. Their public certificates are at `/_local/identity/certs`; there is no public ID-token endpoint.
