# BigQuery

BigQuery v2 runs GoogleSQL on an embedded engine: queries with joins, aggregates, CTEs, window functions, and parameters; DML; DDL for tables and views; and scripts. Every table a query names is authorized before it runs. Datasets with access lists, authorized views, partitioned and clustered tables, expiration, streaming inserts, `tabledata.list`, and `jobs.list` are served. Errors carry GCP's v2 body with `errors[].reason`. Eight scenarios are recorded against GCP; one replays dbt-bigquery's requests.

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
| Datasets | `insert`, `get`, `list`, `patch`, `delete` | `patch` merges labels (a `null` value removes one) and changes `friendlyName`, `description`, `access`, `defaultTableExpirationMs`, and `defaultPartitionExpirationMs` only when sent; `null` clears a default. `access` replaces the list ([access](#dataset-access)). `list` is by ID, paged by `maxResults`/`pageToken`; `_` datasets only with `all=true`. `accessPolicyVersion` (sent by the google Terraform provider) takes any int32, as GCP does (recorded); access lists hold no conditions, so every version reads the same. Delete of a non-empty dataset is 400 `resourceInUse` unless `deleteContents=true` |
| Datasets | `getIamPolicy`, `setIamPolicy` | Return 400 "This feature requires allowlisting", as recorded in GCP, also after access edits (recorded) |
| Tables | `insert`, `get`, `list`, `patch`, `delete` | `insert` takes a schema, labels, `friendlyName`, `description`, `timePartitioning`, `rangePartitioning`, `clustering`, `requirePartitionFilter`, and `expirationTime` ([partitioning](#partitioning-and-clustering)); views come from `CREATE VIEW`. `patch` merges like datasets; a schema is accepted only unchanged. `list` is by ID with `totalItems`, paged, and carries each table's partitioning, clustering, and filter requirement (recorded). Tables created by DDL report the engine's schema |
| Tables | `get` row counts and results | `numRows` counts rows DDL and DML wrote, not streamed rows, which GCP holds in the streaming buffer (recorded for DDL). A `SELECT` job's anonymous result table (`_…` dataset, `anon…` table) reads to the job's creator with the result's schema and row count and expires 24 hours after the job (recorded); anyone else is refused at the project |
| Routines | `list` | An empty object: no statement or method creates a routine (recorded: dbt-bigquery lists routines on every run). `maxResults` and `pageToken` are accepted |
| Tables | `getIamPolicy`, `setIamPolicy` | The table's own policy; it inherits the dataset's and never changes it. Version 3 and conditions are refused, as GCP refuses them (recorded) |
| Rows | `tabledata.insertAll`, `tabledata.list` | An invalid row fails the request: it is `invalid` in `insertErrors`, the rest `stopped`, nothing lands. `ignoreUnknownValues`, `maxResults`, `pageToken`, `startIndex`, and `formatOptions.useInt64Timestamp` are served |
| Jobs | `jobs.query`, `jobs.insert`, `jobs.get`, `jobs.getQueryResults` | GoogleSQL only. `jobs.insert` takes query jobs and finishes them before returning (`DONE`); GCP may answer `RUNNING`. Named and positional parameters, `defaultDataset`, `dryRun`, `maxResults`, page tokens, top-level `fields`, and `formatOptions` are served. `jobTimeoutMs` is accepted and echoed, and never reached, since a job finishes inside the call (recorded). Finished jobs report `configuration.query.destinationTable` for `CREATE TABLE AS SELECT` and `MERGE` (the target) and for `SELECT` (the anonymous result table, with `writeDisposition: WRITE_TRUNCATE`), and a single DDL statement's `ddlOperationPerformed` (`CREATE`, `REPLACE`, `SKIP`, `DROP`) with `ddlTargetTable` or `ddlTargetDataset` (recorded: `CREATE`, `REPLACE`, `SKIP`; `DROP` per the reference). Other statements report no destination. A `jobs.query` retry with the same `requestId` within 15 minutes returns the first run's job. Boolean query parameters ignore case: the Python client sends `formatOptions.useInt64Timestamp=True` |
| Jobs | `jobs.list` | The caller's jobs, newest first; `allUsers=true` needs `bigquery.jobs.listAll`. `projection`, `stateFilter`, `minCreationTime`, `maxCreationTime`, `maxResults`, `pageToken` |

Statements: `SELECT` (joins, subqueries, CTEs, `UNNEST`, window functions, `QUALIFY`, pipe syntax), `INSERT`, `UPDATE`, `DELETE`, `MERGE` from a table or a subquery, `TRUNCATE TABLE`, `CREATE [OR REPLACE] TABLE [AS SELECT]`, `CREATE [OR REPLACE] VIEW`, the `expiration_timestamp` option on both (evaluated once, when the statement runs; recorded), `CREATE SCHEMA [IF NOT EXISTS]` alone in a job (a dataset in the job's location; recorded), `DROP TABLE`, `DROP VIEW`, and scripts with `DECLARE`, `SET`, `IF`, loops, and `BEGIN`…`END`. A script returns its last result. DML returns `numDmlAffectedRows` and `dmlStats`; DML and `CREATE` return the target table's schema. A `TIMESTAMP` string without a zone reads as UTC: literals, casts, `TIMESTAMP(string)`, and a string compared with a `TIMESTAMP` (recorded), and a string inserted into a `TIMESTAMP` column (tested locally). A `TIMESTAMP` literal cast to `STRING` renders as `2020-01-01 12:00:00+00` (recorded). `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_DATETIME`, and `CURRENT_TIME` read one instant, the job's start, as GoogleSQL fixes them per statement; dbt-bigquery's snapshots depend on it (tested locally).

## Partitioning and clustering

Recorded in `bigquery-partitioning-expiration`:

- `timePartitioning` with `type` `DAY`, `HOUR`, `MONTH`, or `YEAR` on a `TIMESTAMP`, `DATE`, or `DATETIME` column, or on ingestion time (no `field`); `rangePartitioning` on an `INT64` column; `clustering` on up to 4 columns. A body with both partitionings keeps only the range, as GCP does. `tables.get` and `patch` repeat `requirePartitionFilter: true` inside `timePartitioning`; `insert` and `list` don't. Column-partitioned tables report `partitionDefinition` (its presence is recorded).
- Refusals (400 `invalid`) name GCP's message: an unknown type, a missing or mistyped partition column, a range interval of 0, a repeated or missing clustering column, 5 clustering columns.
- `patch` changes clustering, `requirePartitionFilter`, partition `expirationMs`, and `expirationTime`. Changing the partition type, column, or range, or partitioning an unpartitioned table, is 400 "Cannot change partitioning/clustering spec for a partitioned/clustered table."
- `requirePartitionFilter`: a `SELECT`, `UPDATE`, `DELETE`, or `MERGE` reading the table must name the partition column (`_PARTITIONTIME`, `_PARTITIONDATE`, or `_PARTITION_LOAD_TIME` for ingestion time) in its own `WHERE` clause (DML: its condition); otherwise 400 `invalidQuery` "Cannot query over table 'D.T' without a filter over column(s) 'ts' that can be used for partition elimination". The ingestion-time refusal's column list is not recorded. Any mention counts; GCP also requires the filter to prune. A filter outside the `SELECT` that reads the table (an outer query over a CTE or a view) doesn't count.
- Ingestion-time tables have `_PARTITIONTIME` and, when daily, `_PARTITIONDATE`. Rows written by DML get the insert time truncated to the partition unit, and `INSERT` may set `_PARTITIONTIME` (recorded); `insertAll` rows get the insert time too. `SELECT *` leaves both out (recorded), as does `tabledata.list`. Naming one with no ingestion-time table in the query is 400 "Unrecognized name: _PARTITIONTIME at [1:8]".
- Clustering and partitioning change no query result: nothing is pruned or sorted.

## Expiration

Recorded in `bigquery-partitioning-expiration`, applied on the emulator's clock:

- `expirationTime` must be at least a second ahead ("Expiration time must be at least 1 seconds in the future"); `null` in `patch` clears it. A table past it reads as gone at once: `get` and queries 404, `list` skips it, its name is free.
- A partition expires `expirationMs` after its start (UTC). Its rows are not read once it expires, even rows a statement just wrote (recorded), and are deleted before the next query or `tabledata.list` of the table. Rows without a partition value never expire.
- `defaultTableExpirationMs` (at least 3,600,000, "Table expiration is less than 3600000ms") sets `expirationTime` on new tables, DDL tables too (not recorded). `defaultPartitionExpirationMs` sets `expirationMs` on new time-partitioned tables, which then get no table expiration (recorded). Neither changes existing tables.

## Dataset access

Recorded in `bigquery-dataset-access`. A dataset's `access` list is its IAM policy: each member entry is a binding on the dataset, which its tables inherit, evaluated with project and table grants.

- New datasets get `WRITER projectWriters`, `OWNER projectOwners`, `OWNER` the creator, and `READER projectReaders` (recorded); a dataset created with `access` keeps exactly that list.
- Member entries: `role` `READER`, `WRITER`, `OWNER` (`roles/bigquery.dataViewer`, `dataEditor`, `dataOwner`) or an IAM role, with `userByEmail` (a service account's email too), `groupByEmail`, `domain`, `specialGroup`, or `iamMember`. Entries come back sorted by IAM role, then member, with duplicates merged (an `iamMember` `serviceAccount:` entry reads back as `userByEmail`), then authorized views and datasets in the order added (recorded).
- Groups and domains resolve through the [local directory](../../architecture/iam.md#groups-and-domains); an unknown group is 400 "IAM setPolicy failed for Dataset P:D: Group G does not exist." (recorded), as is a malformed role. A role-less entry is "Invalid or missing role" (recorded).
- `projectReaders`, `projectWriters`, and `projectOwners` stand for the project's basic-role holders. Basic roles aren't in the local role catalog, so these entries grant nothing locally.
- Authorized views: a `view` entry, or a `dataset` entry with `targetTypes: ["VIEWS"]`, lets that view (or any view of that dataset) read the dataset's tables for a caller who can query the view but not the tables (recorded). The caller still needs `bigquery.tables.getData` on the view. A view authorized in a dataset reads under its own dataset's entries in turn. The view must exist ("View P:D.V to authorize not found in location US.", recorded).
- Query-time denials of a table read: 403 `accessDenied` "Access Denied: Table P:D.T: User does not have permission to query table P:D.T, or perhaps it does not exist." (recorded).

## IAM

Baselines come from recorded `roles/bigquery.*` snapshots (`admin`, `dataEditor`, `dataOwner`, `dataViewer`, `jobUser`, `resourceViewer`).

- A query needs `bigquery.jobs.create` on the project and, for each table it names, before anything runs:

| Statement | Permission | Resource |
| --- | --- | --- |
| Any read, including a `MERGE` source or a table argument | `bigquery.tables.getData` | Table, or its dataset if the table doesn't exist yet |
| `INSERT`, `TRUNCATE` | `bigquery.tables.updateData` | Target |
| `UPDATE`, `DELETE`, `MERGE` | `updateData` and `getData` | Target |
| `CREATE TABLE`, `CREATE VIEW` | `bigquery.tables.create` | Dataset |
| `CREATE SCHEMA` | `bigquery.datasets.create` | Project |
| `CREATE OR REPLACE` over an existing table | also `bigquery.tables.update`, plus `updateData` for a table | Table |
| `DROP` | `bigquery.tables.delete` | Table |

- Reading a view needs `getData` on the view and on every table it reads (views guide), unless the table's dataset authorizes the view ([access](#dataset-access)).
- A script is checked whole before its first statement runs; a later statement's denial runs nothing.
- `insertAll` checks `bigquery.tables.updateData`; `tabledata.list` checks `bigquery.tables.getData` on the table; `routines.list` checks `bigquery.routines.list` on the dataset.
- `jobs.get` and `getQueryResults` need `bigquery.jobs.get`, except for the job's creator; a job user without it gets 404 for a missing job (recorded). `jobs.list` needs `bigquery.jobs.list`.
- Dataset access entries bind on the dataset ([access](#dataset-access)); table policies (`tables.setIamPolicy`) add to them.
- Denials name the dataset, table, or project as GCP does (recorded for each); a call without credentials gets GCP's 401 with `CREDENTIALS_MISSING` (recorded).
- IAM Conditions see `resource.type` (`bigquery.googleapis.com/Dataset` or `/Table`), `resource.name` (`projects/P/datasets/D[/tables/T]`), and `resource.service` of the authorized resource (a missing table authorizes at its dataset), per the [IAM Conditions page](https://docs.cloud.google.com/bigquery/docs/conditions). GCP refuses conditions on table policies (recorded) and gates dataset policies, so they apply only through project grants; that evaluation is modeled, not recorded.

## Verification

| Recording | Date (UTC) | Observes |
| --- | --- | --- |
| `bigquery-dataset-rows-query` | 2026-10-12 | Dataset and table documents, insertAll and string-encoded reads, query and dry run shapes, dataset IAM 400, table-level grant, denied identity 403, non-empty dataset delete |
| `bigquery-jobs-insert-query` | 2026-10-12 | The Python client's `query().result()` calls: `jobs.insert`, `jobs.get` polling, `getQueryResults` pages; duplicate job id 409; creator reads its job without `bigquery.jobs.get`; ungranted reads 403; missing job 404 |
| `bigquery-api-surface` | 2026-10-13 | `errors[]` reasons and messages for duplicate, missing, denied, anonymous, bad ID, in-use, invalid query, missing query table, and denied query; unknown body names ignored; patch merges for datasets and tables; dataset and table list order, fields, paging, and page refusals; `accessPolicyVersion` 1, 2, 3 accepted and a non-integer refused; `jobs.list` order and projections; table policy version 3 refused |
| `bigquery-partitioning-expiration` | 2026-10-13 | Time-unit, ingestion-time, and range partitioning and clustering on insert, get, and list; eight refusals; `requirePartitionFilter` refusals for `SELECT` and `DELETE`; an expired partition left unread; `_PARTITIONTIME` and `_PARTITIONDATE` values, `SELECT *` without them, and the unrecognized name on a column-partitioned table; patches of clustering, the filter, and partition expiration, and the refused type change; `expirationTime` set, patched, cleared, and refused in the past; dataset default expirations applied to partitioned and unpartitioned tables, refused below an hour, cleared |
| `bigquery-dataset-access` | 2026-10-13 | The default access list; dataset IAM still allowlisted after access edits; group, `iamMember`, and `userByEmail` entries and their order; an authorized view read by a caller who reaches the view through a nested group and is denied on the source table (query, rows, get) and on an unauthorized view; an authorized dataset; the denied identity's 403s; refused view, group, role, and role-less entries |
| `bigquery-sql-engine` | 2026-10-12 | `CREATE TABLE`, `INSERT`, `CREATE TABLE AS SELECT`, a DDL table's schema, a join with `GROUP BY`/`HAVING`, a CTE with a subquery and functions, named and positional parameters, a default dataset, a script, `UPDATE`, `DELETE`, `MERGE`, a view, `tabledata.list`, a table-level reader that can read its table but gets 403 joining a second table and 403 on DML, missing table 404, unknown column 400, `DROP VIEW`, `DROP TABLE` |
| `bigquery-dbt-requests` | 2026-10-21 | dbt-bigquery 1.12's `jobs.insert` body (`jobTimeoutMs`, labels, `dryRun: false`): `CREATE SCHEMA IF NOT EXISTS` creating and skipping, the created dataset's fields and location, `routines.list` empty and denied, `CREATE OR REPLACE TABLE … AS SELECT` creating then replacing, its destination table's row count, a data test `SELECT` and its anonymous result table read by its creator and denied to another identity, `MERGE` from a subquery and the merged rows, and a staging table whose `expiration_timestamp` is 12 hours out |
| `bigquery-timestamp-zones` | 2026-10-14 | UTC readings of `TIMESTAMP '2020-01-01'`, a literal with a time, `CAST` of a string with and without a time, `TIMESTAMP(string)`, `TIMESTAMP_TRUNC` and `DATE` at 23:30 and 00:30 UTC, a `TIMESTAMP` cast to `STRING`, a string compared with a `TIMESTAMP`, and an explicit `America/New_York` zone |

Statistics such as `totalBytesProcessed` are stand-ins (`"0"`); only their presence is recorded.

```sh
sh scripts/check/recordings.sh
```

`TestLocalBigQueryDatasetAndTable` and `TestLocalBigQueryPartitionsAndAccess` (partitioned and clustered tables, `access` blocks, `google_bigquery_dataset_access`) pass ([Terraform](../../contributing/terraform-compatibility.md)). The Python client matrix creates a partitioned, clustered table and is refused an unfiltered query.

## Gaps

These return `UNIMPLEMENTED` ("BigQuery coverage gap: …"):

- Statements: `EXECUTE IMMEDIATE`, `CALL`, `CREATE FUNCTION`/`PROCEDURE`, `ALTER`, `DROP SCHEMA`, `CREATE OR REPLACE SCHEMA`, `CREATE SCHEMA` with options, in another project, or beside other statements, temporary tables and views, `CREATE TABLE … LIKE`/`COPY`/`CLONE`, transactions (`BEGIN TRANSACTION`), system variables (`SET @@…`), `EXPORT DATA`, `LOAD DATA`, ML, and graph queries.
- References: other projects' tables, wildcard tables, `INFORMATION_SCHEMA`.
- Parameters: `STRUCT`, `RANGE`, and arrays of types other than `INT64`, `FLOAT64`, `BOOL`, and `STRING`.
- Engine limits, for example `FOR SYSTEM_TIME AS OF`; the message names the engine's refusal. `MERGE` from a subquery beside other statements.
- Legacy SQL, `BATCH` priority, and every Job and `jobs.query` field the discovery schema has but the provider doesn't model, by name: requested destination tables, write and create dispositions, `maximumBytesBilled` (queries bill no bytes locally, so a cap can't be enforced), sessions, reservations, load, extract, and copy jobs, and more. Names the schema lacks are ignored, as GCP ignores them (recorded).
- Dataset fields `defaultEncryptionConfiguration`, `maxTimeTravelHours` other than 168, and the rest of the unmodeled schema. Access entries with a `condition`, `routine` entries (routines aren't modeled), and authorized views or datasets in other projects.
- Table fields `encryptionConfiguration`, `externalDataConfiguration`, `materializedView`, column options (`maxLength`, `precision`, `defaultValueExpression`, non-empty `policyTags`, …), and schema changes through `patch`.
- DDL `PARTITION BY`, `CLUSTER BY`, and the options `partition_expiration_days` and `require_partition_filter`; create such tables with `tables.insert`. An `expiration_timestamp` naming a query parameter.
- Query parameters `datasets.list` `filter`, `datasets.get` `datasetView`, `datasets.patch` `updateMode`, `tables.get` `view` and `selectedFields`, `tables.patch` `autodetect_schema`, `jobs.list` `parentJobId`.
- `tabledata.list` of an anonymous result table (read it with `getQueryResults`); `routines.list` `filter` and `readMask`; every other routine method.
- `tables.insert` with a `view`; `insertAll` into `RECORD` columns or with `skipInvalidRows` or `templateSuffix`; `tabledata.list` `selectedFields`; `timestampOutputFormat=ISO8601_STRING`; nested `fields` selectors.
- Names the local engine can't keep apart: datasets or tables whose ids differ only in case, and tables whose `dataset_table` storage names collide (`ds.t_x` and `ds_t.x`).

Not modeled:

- A script's statements share one current-time instant; GCP fixes it per statement. `MERGE` reports no `numDmlAffectedRows`. Anonymous result datasets aren't listed by `datasets.list`; their ids hash locally. Tables created by DDL report no `REQUIRED` modes or descriptions. `insertId` deduplication, streaming buffer limits, and `jobs.cancel`.
- Streamed rows get their partition time at once; GCP leaves `_PARTITIONTIME` NULL while they sit in the streaming buffer.
- A `TIMESTAMP` column cast to `STRING` renders as `2020-01-01T12:00:00Z`, not GoogleSQL's `2020-01-01 12:00:00+00`; `STRING(ts)` renders the latter. Not recorded.
- Messages of unrecorded refusals (range bounds, a negative partition expiration, an entry naming two members, an unknown special group, a missing authorized dataset) are the emulator's.
- List default page sizes (1,000) are not recorded. Query-time denials of a table use the table form recorded for `tables.get`; GCP's query wording is not recorded.
- State is in memory; restart discards it.
- Job retention follows GCP's documented bounds, not recorded: `getQueryResults` returns 404 24 hours after a job ran, when its rows are freed, and `jobs.get` returns 404 after 180 days (GCP keeps six months of job history). The 404 message is the emulator's.

Why this engine: [decision 0002](../../decisions/0002-bigquery-engine.md).
