# Pub/Sub push to Cloud Run worker to Storage workflow

`main.tf` provisions the whole chain against a local gcp-local endpoint: the
worker and push service accounts, the bucket and the worker's object grant,
the topic, the Cloud Run v2 worker, its invoker grant, the Pub/Sub service
agent's token-creator grant, and the push subscription. Publishing a message
makes the worker (`worker.py`) store an object under its own service account.

The configuration only accepts a numeric loopback endpoint and a local
external-account ADC file marked with `universe_domain = "gcp-local.invalid"`;
it cannot be pointed at real GCP. `scripts/smoke_workflow.py` runs it end to
end against a private core, and `--packaged` runs it against the packaged
environment, where `env` provides the endpoint, project number, and Terraform
credential file. See docs/workflow.md for the runbook, the authentication details, and
the recorded-coverage notes.
