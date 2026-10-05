# Pub/Sub recordings

[Issue #29](https://github.com/evanmarshall/gcp-local/issues/29) adds a prepared recorder for the initial pull and authorization scenario. **No Pub/Sub GCP recording exists yet.** The isolated local smoke test executes the same scenario through production local ADC and the official emulator, but does not create cloud evidence. Existing [Storage recordings](storage.md) remain unchanged.

The runner uses the existing pytest/snapshot lifecycle and pinned `requirements-pubsub.txt`. It creates one topic and one pull subscription, attaches Publisher to the allowed caller on the topic and Subscriber on the subscription, observes denied publication/consumption, checks publication-to-pull message identity and content, acknowledges, and deletes both resources. The initial ack deadline is ten seconds. The subscription uses ordinary GCP retention defaults and is deleted immediately after the run; no retention, exactly-once, push, streaming, or IAM-minimization fidelity is claimed.

Permission choices follow Google's [Pub/Sub access-control table](https://docs.cloud.google.com/pubsub/docs/access-control). A successful fixed grant set does not establish all possible authorization paths. Permission propagation is bounded; timeout, quota, authentication, and transport errors cannot substitute for an observed permission denial. The scenario only records gRPC error codes, selected synthetic payload fields, normalized message relationships, and cleanup outcomes. Raw error text, resource IDs, credentials, acknowledgement IDs, and tokens stay out of recordings.

## Prepare the explicit live plan

This command only reads the selected profile and prints proposed commands. It neither contacts GCP nor authorizes execution:

```sh
python3 scripts/plan_pubsub.py \
  --profile /absolute/path/to/gcp-local/.env \
  --setup-account evan@stitchwink.com \
  --scope-output .gcp-local/pubsub-scope.json
```

The scope file starts with `approved: false`. Obtain the owner's approval for the concrete plan before changing that field or executing any setup command. Existing Storage authorization does not cover Pub/Sub. The selected project and numeric ID must agree with the sandbox profile and the actual Resource Manager response.

The proposed first session enables `pubsub.googleapis.com`, temporarily grants Pub/Sub Admin to the existing **fixture** account, and performs one recording and one revalidation serially. The allowed/denied accounts keep their existing quota/impersonation setup; neither receives a project-level Pub/Sub role. The runner creates only topic/subscription-scoped allow grants and deletes them with those resources. Record pre-existing fixture bindings and remove only a binding newly added for this session. API enablement persists. No new project, billing change, service-account key, public receiver, or scheduled job is needed.

Per-run bounds are one topic, one pull subscription, at most ten publication attempts including denied/propagation trials, at most 1 KiB per message, and 600 seconds of scenario work. RPCs and cleanup have separate deadlines; automatic SDK retries are disabled. These bounds are not a monetary spending cap. Run only one recording process for this project across machines; the local project lock cannot coordinate separate computers.

## Record and compare

After approval and setup, on the host with the explicit private credential profiles:

```sh
.venv-tools/bin/python -m pip install -r requirements-pubsub.txt
.venv-tools/bin/python -m pytest tests/conformance/pubsub -q \
  --gcp-mode record --gcp-profile /absolute/path/to/gcp-local/.env \
  --gcp-pubsub-scope /absolute/path/to/.gcp-local/pubsub-scope.json \
  --gcp-allow-live --gcp-artifacts .artifacts/pubsub-record
```

Review the sanitized candidate, promote it using the [existing evidence procedure](validation.md), and repeat with `--gcp-mode revalidate` and a separate artifact directory. Keep snapshots as observed; a mismatch is a divergence to investigate. The scenario's source and adapter hashes are retained in provenance. No candidate is published after failed test teardown.

Ordinary development remains offline. `sh scripts/check_pubsub.sh` in Moo/Linux runs the recorder smoke test inside a loopback-only network/PID namespace. Once a reviewed `pubsub-pull-authorization` recording is committed, that same integration test also runs pytest in local comparison mode and writes `.artifacts/pubsub-conformance/{coverage.json,junit.xml}`. Without a baseline it reports only local smoke coverage. Explicit local pytest comparison fails as unverified when a baseline is missing; it never records emulator responses as GCP expectations.

The runner selects an explicit synthetic bundle with `--gcp-pubsub-identities`. Each of its three entries supplies a distinct same-project email and an owner-only local ADC path. Only the local STS profile shape is accepted; cloud ADC, ambient credentials, proxies, and anonymous `PUBSUB_EMULATOR_HOST` mode are rejected. The actual integration test creates and destroys this private bundle automatically. Live runs use the three explicitly selected keyless sandbox profiles and the fixed Google endpoint.

## Interrupted runs

Each live run writes an owner-only intent manifest **before creation** under `~/.local/state/gcp-local/PROJECT/pubsub/`. Cleanup checks the numeric project, exact generated names, ownership labels, and subscription-topic relationship before deleting. It never sweeps unrelated resources. An occupied project stops setup, even when the existing resources are unrelated.

After an interruption, retain fixture permissions until owned cleanup succeeds:

```sh
.venv-tools/bin/python -m gcp_validation.pubsub cleanup \
  --profile /absolute/path/to/gcp-local/.env \
  --scope /absolute/path/to/.gcp-local/pubsub-scope.json \
  --manifest /absolute/path/to/RUN_ID.json --allow-live
```

The cleanup command requires the same explicitly approved scope. Ownership mismatches stop cleanup and leave the manifest pending. Cleanup can retry a partially deleted subscription/topic pair; it does not repair foreign ownership or recreate resources. Restore newly added fixture permissions after all manifests report successful cleanup.
