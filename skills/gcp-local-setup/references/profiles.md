# Application validation profiles

Use the application's own test tooling. This bundle does not contain an emulator, a proxy, a cloud recorder, or a live API client.

## Offline and local

Existing unit tests can remain offline. Inspect their setup and subprocesses before running them; a test command's name does not establish that it avoids GCP.

The [local template](local.env.example) contains only a synthetic project and explicit loopback endpoint. Copy it only when useful, preserving existing configuration. The endpoint is a placeholder for a future emulator, not a listening service. Local API execution remains unavailable until the application's required services and identity flow are implemented. Do not repoint missing local APIs at production or use real credentials as a workaround.

## Explicit live sandbox

Use the [live template](live.env.example) only for authorized real-GCP work. Keep it separate from local configuration and excluded from Git. Credential files stay outside the repository; the profile contains only their absolute paths (or `~/...`). Never copy another repository's credentials or use ambient ADC to fill missing fields.

The current profile contract needs a dedicated project ID and numeric project number, region, owner-approved scope, and three distinct identities from that project: fixture setup/cleanup, allowed test caller, and denied test caller. The fixture identity manages Storage resources; allowed permissions are granted on each test bucket, and the denied caller has no Storage grants. The initial Storage runner supports at most one temporary bucket, ten objects, and 1 MiB per object with cleanup. These are resource constraints, not a spending cap or a grant of authorization.

Keyless local ADC uses service-account impersonation with an authorized-user source and the sandbox as `quota_project_id`. Credential files must have owner-only permissions (`0600`). The Google Cloud CLI and interactive Google sign-in are needed to configure these credentials. Do not create service-account keys or copy credential contents into reports. GitHub WIF setup is separate and is not supplied by this bundle.

Application tests still need their own explicit endpoint/authentication adapter. Some SDK emulator switches use anonymous credentials, so an endpoint override alone cannot establish IAM fidelity. Report unverified behavior clearly. Reuse any existing sandbox scope supplied by the owner; request only missing inputs or authorization for additional live work.
