# BigQuery (local slice)

BigQuery v2 control plane: datasets, tables with typed schemas, insertAll row storage, and tabledata reads with per-dataset/table IAM (recorded `roles/bigquery.*` snapshots pin the baselines; dataset listing checks `bigquery.datasets.get` and insertAll checks `bigquery.tables.updateData`, as in GCP).

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

## Recorded scenario

`bigquery-dataset-rows-query` was recorded and revalidated twice against real
GCP on 2026-10-11, and the local provider matches it in comparison mode
(`sh scripts/check_rest_recordings.sh`). One labeled dataset in `US` with one
table. Observed and compared:

- dataset and table documents: `kind`, `id` as `project:dataset[.table]`,
  `etag`, `selfLink`, `location`, `type`, the dataset `access` list, and the
  table's storage counters (presence only)
- insertAll returns only `kind`; tabledata.list returns string-encoded cells
  (`"1"`, `"true"`); `totalRows` is not compared because it lags while streamed
  rows sit in the buffer
- jobs.query with a backtick-quoted table, projection, and equality filter:
  result columns are all NULLABLE and `totalRows` is a string; dry runs return
  the plan fields without rows
- **dataset IAM methods return 400 "This feature requires allowlisting"**;
  access is granted per table with `tables.setIamPolicy` (POST
  `tables.getIamPolicy`, empty policy reads as only `etag`)
- a table-level `roles/bigquery.dataViewer` lets the allowed identity read rows
  but not insert or run queries (no `bigquery.jobs.create`); the denied
  identity gets 403
- deleting a non-empty dataset is 400; `deleteContents=true` deletes it

The recording fixed many local divergences: sparse dataset/table documents,
untyped row cells, the query response shape, missing dry-run support,
backtick table references, the dataset IAM surface (which does not exist
without allowlisting), GET-only table `getIamPolicy`, `deleteContents`, and a
bug that removed a non-empty dataset from state while refusing the delete.
Statistic values (`totalBytesProcessed`, `totalSlotMs`, counters) and the
default `access` entries are local stand-ins; only field presence is recorded.

## Explicit gaps

- Aggregations, joins, functions, legacy SQL, DML, and getQueryResults paging
  remain explicit `UNIMPLEMENTED` gaps and are unrecorded.
- Materialized views, partitions/clustering, streaming quotas, and the dataset
  `access` list's update semantics are not modeled.
