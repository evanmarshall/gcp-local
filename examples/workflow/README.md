# Pub/Sub push to Cloud Run worker to Storage workflow

Terraform creates the bucket, the topic, and the push subscription; the local
APIs create the Cloud Run worker. Publishing a message makes the worker
store an object under its own service account. See docs/workflow.md for the
runbook and the recorded-coverage notes.
