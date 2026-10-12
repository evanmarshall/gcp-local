# BigQuery

BigQuery v2 runs GoogleSQL on an embedded engine: queries with joins, aggregates, CTEs, window functions, and parameters; DML; DDL for tables and views; and scripts. Every table a query names is authorized before it runs. Datasets, tables, streaming inserts, and `tabledata.list` work as before. Three scenarios are recorded against GCP.

## Use

Select `-services ...,bigquery`. Routes are `/bigquery/v2/projects/{project}/...` on port 4566.

```python
client.query("SELECT name, COUNT(*) FROM shop.orders o JOIN shop.people p ON o.person = p.id "
             "WHERE p.id > @min GROUP BY name",
             job_config=bigquery.QueryJobConfig(query_parameters=[
                 bigquery.ScalarQueryParameter("min", "INT64", 1)])).result()
```

## Methods

| Resource | Methods | Notes |
| --- | --- | --- |
| Datasets | `insert`, `get`, `list`, `patch`, `delete` | `patch` changes labels and friendly name. Delete of a non-empty dataset is 400 unless `deleteContents=true` |
| Datasets | `getIamPolicy`, `setIamPolicy` | Return 400 "This feature requires allowlisting", as recorded in GCP |
| Tables | `insert`, `get`, `list`, `patch`, `delete` | `insert` takes a schema; views come from `CREATE VIEW`. Tables created by DDL report the engine's schema |
| Tables | `getIamPolicy`, `setIamPolicy` | The table's own policy; it inherits the dataset's and never changes it |
| Rows | `tabledata.insertAll`, `tabledata.list` | An invalid row fails the request: it is `invalid` in `insertErrors`, the rest `stopped`, nothing lands. `ignoreUnknownValues`, `maxResults`, `pageToken`, `startIndex`, and `formatOptions.useInt64Timestamp` are served |
| Jobs | `jobs.query`, `jobs.insert`, `jobs.get`, `jobs.getQueryResults` | GoogleSQL only. `jobs.insert` takes query jobs and finishes them before returning (`DONE`). Named and positional parameters, `defaultDataset`, `dryRun`, `maxResults`, page tokens, top-level `fields`, and `formatOptions` are served |

Statements: `SELECT` (joins, subqueries, CTEs, `UNNEST`, window functions, `QUALIFY`, pipe syntax), `INSERT`, `UPDATE`, `DELETE`, `MERGE` from a table, `TRUNCATE TABLE`, `CREATE [OR REPLACE] TABLE [AS SELECT]`, `CREATE [OR REPLACE] VIEW`, `DROP TABLE`, `DROP VIEW`, and scripts with `DECLARE`, `SET`, `IF`, loops, and `BEGIN`…`END`. A script returns its last result. DML returns `numDmlAffectedRows` and `dmlStats`; DML and `CREATE` return the target table's schema.

## IAM

Baselines come from recorded `roles/bigquery.*` snapshots (`admin`, `dataEditor`, `dataOwner`, `dataViewer`, `jobUser`, `resourceViewer`).

- A query needs `bigquery.jobs.create` on the project and, for each table it names, before anything runs:

| Statement | Permission | Resource |
| --- | --- | --- |
| Any read, including a `MERGE` source or a table argument | `bigquery.tables.getData` | Table, or its dataset if the table doesn't exist yet |
| `INSERT`, `TRUNCATE` | `bigquery.tables.updateData` | Target |
| `UPDATE`, `DELETE`, `MERGE` | `updateData` and `getData` | Target |
| `CREATE TABLE`, `CREATE VIEW` | `bigquery.tables.create` | Dataset |
| `CREATE OR REPLACE` over an existing table | also `bigquery.tables.update`, plus `updateData` for a table | Table |
| `DROP` | `bigquery.tables.delete` | Table |

- Reading a view needs `getData` on the view and on every table it reads (views guide). Authorized views are not modeled.
- A script is checked whole before its first statement runs; a later statement's denial runs nothing.
- `insertAll` checks `bigquery.tables.updateData`; `tabledata.list` checks `bigquery.tables.getData` on the table.
- `jobs.get` and `getQueryResults` need `bigquery.jobs.get`, except for the job's creator; a job user without it gets 404 for a missing job (recorded).

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigquery-dataset-rows-query` | 2026-10-12 | Dataset and table documents, insertAll and string-encoded reads, query and dry run shapes, dataset IAM 400, table-level grant, denied identity 403, non-empty dataset delete |
| `bigquery-jobs-insert-query` | 2026-10-12 | The Python client's `query().result()` calls: `jobs.insert`, `jobs.get` polling, `getQueryResults` pages; duplicate job id 409; creator reads its job without `bigquery.jobs.get`; ungranted reads 403; missing job 404 |
| `bigquery-sql-engine` | 2026-10-12 | `CREATE TABLE`, `INSERT`, `CREATE TABLE AS SELECT`, a DDL table's schema, a join with `GROUP BY`/`HAVING`, a CTE with a subquery and functions, named and positional parameters, a default dataset, a script, `UPDATE`, `DELETE`, `MERGE`, a view, `tabledata.list`, a table-level reader that can read its table but gets 403 joining a second table and 403 on DML, missing table 404, unknown column 400, `DROP VIEW`, `DROP TABLE` |

Statistics such as `totalBytesProcessed` are stand-ins (`"0"`); only their presence is recorded.

```sh
sh scripts/check/recordings.sh
```

`TestLocalBigQueryDatasetAndTable` passes ([Terraform](../../contributing/terraform-compatibility.md)).

## Gaps

These return `UNIMPLEMENTED` ("BigQuery coverage gap: …"):

- Statements: `EXECUTE IMMEDIATE`, `CALL`, `CREATE FUNCTION`/`PROCEDURE`, `ALTER`, `CREATE SCHEMA`/`DROP SCHEMA`, temporary tables and views, `CREATE TABLE … LIKE`/`COPY`/`CLONE`, transactions (`BEGIN TRANSACTION`), system variables (`SET @@…`), `EXPORT DATA`, `LOAD DATA`, ML, and graph queries.
- References: other projects' tables, wildcard tables, `INFORMATION_SCHEMA`.
- Parameters: `STRUCT`, `RANGE`, and arrays of types other than `INT64`, `FLOAT64`, `BOOL`, and `STRING`.
- Engine limits, for example `MERGE` from a subquery; the message names the engine's refusal.
- Legacy SQL, `BATCH` priority, destination tables, and any other Job or `jobs.query` field not listed above.
- `tables.insert` with a `view`; `insertAll` into `RECORD` columns or with `skipInvalidRows` or `templateSuffix`; `tabledata.list` `selectedFields`; `timestampOutputFormat=ISO8601_STRING`; nested `fields` selectors.
- Names the local engine can't keep apart: datasets or tables whose ids differ only in case, and tables whose `dataset_table` storage names collide (`ds.t_x` and `ds_t.x`).

Not modeled:

- `MERGE` reports no `numDmlAffectedRows`. Tables created by DDL report no `REQUIRED` modes or descriptions. `insertId` deduplication, streaming buffer limits, partitioning, clustering, load, extract, and copy jobs, `jobs.list`, `jobs.cancel`, materialized views, authorized views, and `access` list updates.
- State is in memory; restart discards it.

Why this engine: [decision 0002](../../decisions/0002-bigquery-engine.md).
