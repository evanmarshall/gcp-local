# Local identity

gcp-local issues its own credentials: an external-account ADC file that exchanges a local subject token at the core's `/v1/token`, and an optional metadata server bound to one service account. Tokens are opaque local handles and never work against Google. The [packaged environment](quickstart.md) creates all of this for you; this page is for running `bin/gcp-local` directly ([runtime](runtime.md)).

## Developer ADC

Create a private identity bundle and start the core with it:

```sh
python3 scripts/create_local_identity.py \
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
- Tokens are scoped to `cloud-platform` and the Storage SDK's default scopes; an enrollment's `scopes` can narrow that. IAM is checked separately.
- A token exchange without `scope` gets the enrollment's scopes, as the metadata server does. Real STS acceptance of a scope-less exchange is not recorded.
- Tokens die when the core restarts.
- `GET /_local/identity` reports the caller's state, subject, project, and scopes. Unknown or expired tokens get 401.

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

Go, Java, and Node SDKs are not verified.

## Limits

- Metadata ID tokens return 501.
- No service-account keys, impersonation or signing APIs, or JWT/SAML federation validation.
- One metadata listener per core; account binding is not network isolation.
- [Pub/Sub push](pubsub.md) mints locally signed ID tokens internally. Their public certificates are at `/_local/identity/certs`; there is no public ID-token endpoint.
