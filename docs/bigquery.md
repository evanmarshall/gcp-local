# BigQuery

BigQuery v2 datasets, tables with typed schemas, `insertAll` rows, `tabledata.list`, and a bounded SELECT, with dataset and table IAM. Modeled locally in provider state. The scenario `bigquery-dataset-rows-query` is recorded against GCP.

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
| Jobs | `jobs.query`, `jobs.getQueryResults` | SELECT with column projection and equality filters on one backtick-quoted table; `dryRun` supported |

## IAM

Baselines come from recorded `roles/bigquery.*` snapshots (`admin`, `dataEditor`, `dataOwner`, `dataViewer`, `jobUser`, `resourceViewer`).

- Listing datasets checks `bigquery.datasets.get`. `insertAll` checks `bigquery.tables.updateData`.
- Queries need `bigquery.jobs.create`. A table-level `roles/bigquery.dataViewer` can read rows but cannot insert or query.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigquery-dataset-rows-query` | 2026-10-11 | Dataset and table documents, insertAll and string-encoded reads, query and dry run shapes, dataset IAM 400, table-level grant, denied identity 403, non-empty dataset delete |

Statistics such as `totalBytesProcessed` and the default `access` entries are stand-ins; only their presence is recorded.

```sh
sh scripts/check_rest_recordings.sh
```

`TestAccBigQueryDatasetAndTable` passes ([Terraform](terraform-compatibility.md)).

## Gaps

- `UNIMPLEMENTED`: aggregations, joins, functions, legacy SQL, DML, and `getQueryResults` paging.
- Not modeled: load and extract jobs, views, materialized views, partitioning, clustering, streaming quotas, `access` list updates.
- State is in memory; restart discards it.
