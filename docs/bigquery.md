# BigQuery

BigQuery v2 datasets, tables with typed schemas, `insertAll` rows, `tabledata.list`, and a bounded SELECT through `jobs.query` or query jobs, with dataset and table IAM. Modeled locally in provider state. Two scenarios are recorded against GCP.

## Use

Select `-services ...,bigquery`. Routes are `/bigquery/v2/projects/{project}/...` on port 4566.

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Datasets | `insert`, `get`, `list`, `patch`, `delete` | `patch` changes labels and friendly name. Delete of a non-empty dataset is 400 unless `deleteContents=true` |
| Datasets | `getIamPolicy`, `setIamPolicy` | Return 400 "This feature requires allowlisting", as recorded in GCP |
| Tables | `insert`, `get`, `list`, `patch`, `delete` | Schema required; `patch` changes labels and friendly name |
| Tables | `getIamPolicy`, `setIamPolicy` | Per-table grants; an empty policy returns only `etag` |
| Rows | `tabledata.insertAll`, `tabledata.list` | JSON rows; cells returned as strings |
| Jobs | `jobs.query`, `jobs.insert`, `jobs.get`, `jobs.getQueryResults` | SELECT with column projection and equality filters on one table in the job's project; `dryRun` supported. `jobs.insert` takes Standard SQL query jobs only and finishes them before returning (`DONE`). `getQueryResults` pages with `maxResults`, `startIndex`, and `pageToken`, and honors top-level `fields` |

## IAM

Baselines come from recorded `roles/bigquery.*` snapshots (`admin`, `dataEditor`, `dataOwner`, `dataViewer`, `jobUser`, `resourceViewer`).

- Listing datasets checks `bigquery.datasets.get`. `insertAll` checks `bigquery.tables.updateData`.
- Queries need `bigquery.jobs.create` on the project and `bigquery.tables.getData` on the table. A table-level `roles/bigquery.dataViewer` can read rows but cannot insert or query.
- `jobs.get` and `getQueryResults` need `bigquery.jobs.get`, except for the job's creator; a job user without it gets 404 for a missing job (recorded). The creator check is modeled as `bigquery.jobs.create` on the project.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigquery-dataset-rows-query` | 2026-10-11 | Dataset and table documents, insertAll and string-encoded reads, query and dry run shapes, dataset IAM 400, table-level grant, denied identity 403, non-empty dataset delete |
| `bigquery-jobs-insert-query` | 2026-10-12 | The Python client's `query().result()` calls: `jobs.insert`, `jobs.get` polling, `getQueryResults` with `maxResults=0`, a page, and a `fields` page; duplicate job id 409; creator reads its job without `bigquery.jobs.get`; ungranted reads and insert 403; missing job 404 |

Statistics such as `totalBytesProcessed` and the default `access` entries are stand-ins; only their presence is recorded.

```sh
sh scripts/check_rest_recordings.sh
```

`TestAccBigQueryDatasetAndTable` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- `UNIMPLEMENTED`: aggregations, joins, functions, legacy SQL (the `jobs.insert` default when `useLegacySql` is unset), DML, cross-project table references, `BATCH` priority, query parameters, destination tables, every Job field outside `jobReference` and `configuration.{jobType, dryRun, labels, query.{query, useLegacySql, priority, useQueryCache}}`, nested `fields` selectors, and result format options on `TIMESTAMP` columns.
- Jobs report no `destinationTable`; anonymous result tables are not modeled.
- Not modeled: load, extract, and copy jobs, `jobs.list`, `jobs.cancel`, views, materialized views, partitioning, clustering, streaming quotas, `access` list updates.
- State is in memory; restart discards it.
