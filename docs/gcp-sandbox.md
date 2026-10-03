# Real-GCP sandbox inputs

The offline runner does not need a Google account. For project creation and later live recordings, fill in the ignored `.env` copied from [.env.example](../.env.example). Keep tokens, passwords, and JSON key contents out of that file; credentials remain in a separate local ADC file.

## Fill in now

| Variable | What to enter |
| --- | --- |
| `GCP_PROJECT_ID` | A new globally unique project ID, such as `gcp-local-sandbox-your-suffix`. It must be 6–30 lowercase letters/digits/hyphens, beginning with a letter. |
| `GCP_BILLING_ACCOUNT_ID` | The billing account to charge, in `XXXXXX-XXXXXX-XXXXXX` form. Find it under Billing → Account management in the Cloud Console. |
| `GCP_SETUP_ACCOUNT` | Your Google account email, with permission to create a project and link the selected billing account. This is the setup identity, not the denied-test caller. |
| `GCP_TEST_BUDGET_USD` | Optional monthly budget-alert threshold; leave blank to skip. This input does not configure a budget yet and is not a hard spending cap. |
| `GCP_TEST_LIVE_SCOPE` | Permitted services and resource limits. For the first suite, suggest Storage only, one temporary bucket, small test objects, and cleanup after each run. |

For an organization-managed project, fill in `GCP_ORGANIZATION_ID` **or** `GCP_FOLDER_ID`. For a personal project without a parent, leave both empty. `GCP_REGION=us-central1` is the proposed default.

Google documents the [project creation options](https://docs.cloud.google.com/sdk/gcloud/reference/projects/create) and [billing linkage](https://docs.cloud.google.com/sdk/gcloud/reference/billing/projects/link). Successful linking needs access to the billing account as well as the project; merely knowing the billing ID does not grant that access.

## Authentication and setup

Use browser sign-in through `gcloud auth login` for project creation. The Cloud CLI must first be installed on the setup machine. If authentication is needed, it requires your interactive sign-in; do not paste passwords, tokens, or keys into chat. See [gcloud login](https://docs.cloud.google.com/sdk/gcloud/reference/auth/login).

After the sandbox and test service account exist, use `gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL` to create test ADC. The signing-in identity needs Service Account Token Creator on that account. Fill `GCP_TEST_SERVICE_ACCOUNT` with its email and `GCP_TEST_CREDENTIALS_FILE` with the absolute or `~/...` path to the resulting ADC file. Leave these blank until setup produces them. See [Google's local ADC instructions](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment).

The test service account needs only the scenario's permissions. A separate setup/cleanup identity manages fixtures, and separate callers exercise allowed/denied behavior. Do not give the denied caller broad inherited roles. An ordinary API key does not replace this identity flow.

Project creation, billing linkage, APIs, roles, budget alerts, and live verification are tracked in [#21](https://github.com/evanmarshall/gcp-local/issues/21). Filling the file prepares that work; it does not itself create resources or start tests. Never copy real credentials into moo's shared baseline. The ignored `.env` and credential files are not synchronized into moo; use a separately scoped live execution environment.


## Explicit local credential profile

The sandbox `.env` is an explicitly selected input file, never sourced as shell code or loaded from the working directory automatically. It supports `NAME=value` and quoted values; shell commands and environment substitutions are not evaluated. Use absolute credential paths or `~/...`. Duplicate fields and mixed sandbox identities are rejected.

After project creation, obtain its numeric identifier with `gcloud projects describe PROJECT_ID --format='value(projectNumber)'` and fill `GCP_PROJECT_NUMBER`. Provide three distinct service accounts from that project using the fields in `.env.example`: the primary `GCP_TEST_*` caller, `GCP_TEST_FIXTURE_*` setup/cleanup caller, and `GCP_TEST_DENIED_*` caller. Each has a separately selected credential file.

The initial local profile accepts keyless, impersonated ADC with an `authorized_user` source, the standard Google IAM/OAuth endpoints, and `quota_project_id` equal to the sandbox. Credential files must have mode `0600`. Store them outside Git and moo. The fixture identity needs Storage setup/cleanup permissions; test callers receive only scenario-specific grants, and the denied identity receives no Storage grants. Grant the setup user Service Account Token Creator on the individual accounts. Service Usage Consumer supports charging quota to the sandbox without granting Storage access.

An offline doctor inspects the profile and credential structure without refreshing tokens or contacting GCP:

```sh
python -m gcp_validation.sandbox doctor --profile /absolute/path/to/.env
```

For an explicitly authorized authentication check, install `requirements-cloud.txt` and add `--allow-live`. This refreshes all three selected identities, prints no tokens, and creates no resources. Neither mode claims that an emulator exists. Browser sign-in can expire; repeat the documented ADC login when needed. The doctor deliberately rejects JSON service-account keys and external-account configurations; GitHub WIF support is still tracked in #21.

Resource bounds default to one bucket, ten objects, and 1 MiB per object and can only be lowered for this initial Storage slice. These are runner constraints, not a billing-account spending cap. Budgets remain optional alerts. The free-text `GCP_TEST_LIVE_SCOPE` records the authorization supplied by the owner; filling it is not itself authorization.
