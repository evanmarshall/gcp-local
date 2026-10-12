# Application validation profiles

Use the application's own test tooling. This bundle does not contain an emulator, a proxy, a cloud recorder, or a live API client.

## Offline and local

Existing unit tests can remain offline. Inspect their setup and subprocesses before running them; a test command's name does not establish that it avoids GCP.

Start a private environment with `gcp-local up` or the runtime checkout's `scripts/local_env.py up`, then obtain its ADC/project/endpoint values with `env`. The [local template](local.env.example) shows those fields; do not copy its placeholder credential path verbatim. Keep generated local settings separate from existing application and live-cloud configuration. Recorded and locally modeled coverage differs by service; see the runtime's `docs/quickstart.md`, and `docs/pubsub.md` for Pub/Sub setup. Do not repoint missing local APIs at production or use real credentials as a workaround.

## Explicit live sandbox

Use the [live template](live.env.example) only for authorized real-GCP work. Keep it separate from local configuration and excluded from Git. Credential files stay outside the repository; the profile contains only their absolute paths (or `~/...`). Never copy another repository's credentials or use ambient ADC to fill missing fields.

The current profile contract needs a dedicated project ID and numeric project number, region, owner-approved scope, and three distinct identities from that project: fixture setup/cleanup, allowed test caller, and denied test caller. The fixture identity manages Storage resources; allowed permissions are granted on each test bucket, and the denied caller has no Storage grants. The initial Storage runner supports at most one temporary bucket, ten objects, and 1 MiB per object with cleanup. These are resource constraints, not a spending cap or a grant of authorization.

Keyless credentials for the live sandbox use service-account impersonation with an authorized-user source and the sandbox as `quota_project_id`. Credential files must have owner-only permissions (`0600`). The Google Cloud CLI and interactive Google sign-in are needed to configure these credentials. Do not create service-account keys or copy credential contents into reports. GitHub WIF setup is separate and is not supplied by this bundle.

Application tests still need their own explicit endpoint/authentication adapter. Some SDK emulator switches use anonymous credentials, so an endpoint override alone cannot establish IAM fidelity. Report unverified behavior clearly. Reuse any existing sandbox scope supplied by the owner; request only missing inputs or authorization for additional live work.
