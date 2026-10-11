# BigQuery (local slice)

BigQuery v2 control plane: datasets, tables with typed schemas, insertAll row storage, and tabledata reads with per-dataset/table IAM (`roles/bigquery.dataEditor` and `roles/bigquery.dataViewer` snapshots pin the baselines).

## Local-modeled surfaces

- Datasets: create (id from `datasetReference.datasetId`), get, list, update (labels, friendly name), delete — delete is refused while the dataset still contains tables, matching the real surface.
- Tables: create with a schema (fields required), get, list, update (labels, friendly name), delete with rows.
- Rows: `insertAll` stores posted `json` rows keyed to the table; `tabledata.list` returns the stored rows newest-last with `totalRows`.

The Terraform acceptance case `TestAccBigQueryDatasetAndTable` drives google_bigquery_dataset + google_bigquery_table with the pinned google 8.5.0 provider and passes locally.

## Query jobs

`POST /bigquery/v2/projects/{project}/queries` (jobs.query) and
`GET .../queries/{jobId}` (jobs.getQueryResults) run a bounded local SELECT
against stored rows: column projection and equality filters, results in the
real jobReference/schema/rows cell shape, stored per job id for result reads.
Legacy SQL, joins, aggregations, functions, and DML/DQL beyond SELECT are
explicit unimplemented gaps, not silent passes.

## Explicit gaps

- No cloud recording backs these methods; issue #41 tracks the recording. No supported claim is made.
- jobs.query, dry-run, materialized views, partitions/clustering, streaming quotas, and the typed-value row encoding beyond passthrough are unimplemented.
- Rows are in-memory per scope; persistence is unsupported (the backend declares it).
