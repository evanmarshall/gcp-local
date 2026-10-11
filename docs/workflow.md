# The packaged workflow: Pub/Sub push -> Cloud Run worker -> Storage

The acceptance test is `scripts/smoke_workflow.py` (CI job `workflow-acceptance`,
ordinary Linux Docker). The example sources live in `examples/workflow/`.

## Chain

1. The example's Terraform creates the bucket; the smoke uses the same API.
2. A topic and a push subscription are created with the real Pub/Sub SDK over
   the local gateway (the example's Terraform covers the same resources over
   the JSON REST surface). The push delivery mints an ID token for the
   subscription's configured OIDC account, audience-bound to the worker URI.
3. A Cloud Run worker is deployed through the v2 control plane with the worker
   service account configured. Its revision gets a private metadata listener
   bound on the Docker bridge address, injected as `GCE_METADATA_HOST`.
4. Publishing a message triggers the push: invocation verifies the audience and
   the run.invoker grant, then the worker authenticates through its metadata
   endpoint and stores the object under its own service account.
5. Revoking the worker's Storage grant fails the write (the push sees the
   failure); restoring the grant recovers. Delete cleans every container.

## Recorded coverage and approximations

- Storage bucket/object semantics: cloud-recorded (#8 slices).
- Pub/Sub pull authorization: cloud-recorded (#29). Push/invocation delivery is
  local-modeled; real-GCP push evidence still needs an authorized recording plan.
- Cloud Run services/revisions/operations/invocation: local-modeled (#13);
  no cloud recording yet.
- The control-plane calls in the smoke use an allUsers fixture project grant;
  the boundaries under test (invocation, the worker's write) use real
  per-identity grants.
- The example's Terraform now provisions the bucket, topic, and push
  subscription; the resource contracts are pinned by the acceptance runner
  (TestAccPubSubTopicAndSubscription, TestAccStorageBucket_basic). The Cloud
  Run resource stays parked (#13): the provider's start-up sequence hangs in
  the loopback-only runner before reaching the local endpoint, so recording
  its call sequence from real GCP is the unblock path.
