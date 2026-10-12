package bigquery

import (
	"cmp"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Wire keys repeated across the dataset, table, and query documents.
const (
	keyKind         = "kind"
	keyETag         = "etag"
	keyLocation     = "location"
	keyCreationTime = "creationTime"
	keyProjectID    = "projectId"
	keySelfLink     = "selfLink"
	keySchema       = "schema"
	keyFields       = "fields"
	keyJobReference = "jobReference"
	keyCacheHit     = "cacheHit"
	keyQuery        = "query"
	// keyBytesProcessed is always "0": no bytes are billed locally.
	keyBytesProcessed = "totalBytesProcessed"
)

// permJobsCreate runs jobs; the creator check reuses it.
const permJobsCreate = "bigquery.jobs.create"

// selfLink prefixes the API links GCP returns on datasets and tables.
const selfLink = "https://bigquery.googleapis.com/bigquery/v2/projects/"

// etagOf renders a stable revision tag for one stored resource.
func etagOf(moment time.Time) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(moment.UnixNano(), 36)))
}

// defaultAccess is the access list GCP attaches to a new dataset. Only the
// field's presence is recorded; the entries are the documented defaults.
func defaultAccess() []any {
	entry := func(role, group string) any { return map[string]any{"role": role, "specialGroup": group} }
	return []any{entry("WRITER", "projectWriters"), entry("OWNER", "projectOwners"), entry("READER", "projectReaders")}
}

// nullableFields copies a result schema with every column NULLABLE, as GCP
// reports query result columns regardless of the source mode.
func nullableFields(fields []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		copied := map[string]any{"mode": "NULLABLE"}
		for key, value := range field {
			if key != "mode" {
				copied[key] = value
			}
		}
		out = append(out, copied)
	}
	return out
}

// cellValue renders one stored JSON value in the BigQuery cell encoding:
// every scalar is a string and null stays null.
func cellValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return value
	}
}

// queryResponse renders a completed jobs.query result; a dry run reports the
// plan without rows or execution timing.
func queryResponse(project string, job *jobRecord, dryRun bool) map[string]any {
	location := job.Location
	if location == "" {
		location = "US"
	}
	created := strconv.FormatInt(job.CreatedAt.UnixMilli(), 10)
	response := map[string]any{
		keyKind:            "bigquery#queryResponse",
		keyJobReference:    jobReference(project, job),
		"jobComplete":      true,
		keyLocation:        location,
		keySchema:          map[string]any{keyFields: job.Fields},
		"statementType":    "SELECT",
		keyCacheHit:        false,
		keyCreationTime:    created,
		"endTime":          created,
		keyBytesProcessed:  "0",
		"totalBytesBilled": "0",
	}
	if dryRun {
		return response
	}
	response["rows"] = bqRows(job.Fields, job.Rows)
	response["totalRows"] = strconv.Itoa(len(job.Rows))
	response["pageRowCount"] = strconv.Itoa(len(job.Rows))
	response["queryId"] = job.ID
	response["startTime"] = created
	response["totalSlotMs"] = "0"
	response["jobCreationReason"] = map[string]any{"code": "REQUESTED"}
	return response
}

// jobReference renders a job's reference; a job's location defaults to US.
func jobReference(project string, job *jobRecord) map[string]any {
	return map[string]any{keyProjectID: project, "jobId": job.ID, keyLocation: cmp.Or(job.Location, "US")}
}

// jobWire renders a completed query job resource (jobs.insert, jobs.get).
// No anonymous destination table is modeled, so configuration.query carries
// no destinationTable; a dry run has no start or end time.
func jobWire(project string, job *jobRecord, dryRun bool) map[string]any {
	location := cmp.Or(job.Location, "US")
	created := strconv.FormatInt(job.CreatedAt.UnixMilli(), 10)
	configuration := map[string]any{
		"jobType": "QUERY",
		keyQuery:  map[string]any{keyQuery: job.Query, "useLegacySql": false, "priority": "INTERACTIVE"},
	}
	if dryRun {
		configuration["dryRun"] = true
	}
	if len(job.Labels) > 0 {
		configuration["labels"] = job.Labels
	}
	statistics := map[string]any{
		keyCreationTime: created, keyBytesProcessed: "0",
		keyQuery: map[string]any{keyBytesProcessed: "0", "totalBytesBilled": "0", keyCacheHit: false, "statementType": "SELECT"},
	}
	if !dryRun {
		statistics["startTime"] = created
		statistics["endTime"] = created
	}
	document := map[string]any{
		keyKind:         "bigquery#job",
		keyETag:         etagOf(job.CreatedAt),
		"id":            project + ":" + location + "." + job.ID,
		keySelfLink:     selfLink + project + "/jobs/" + job.ID + "?location=" + location,
		keyJobReference: jobReference(project, job),
		"configuration": configuration,
		"statistics":    statistics,
		"status":        map[string]any{"state": "DONE"},
	}
	if _, email, ok := strings.Cut(job.Principal, ":"); ok {
		document["user_email"] = email
		document["principal_subject"] = job.Principal
	}
	document["jobCreationReason"] = map[string]any{"code": "REQUESTED"}
	return document
}

// resultsWire renders one jobs.getQueryResults page: rows start through end,
// and a pageToken while rows remain, even after maxResults=0 (recorded:
// bigquery-jobs-insert-query).
func resultsWire(project string, job *jobRecord, start, end int) map[string]any {
	response := map[string]any{
		keyKind:           "bigquery#getQueryResultsResponse",
		keyETag:           etagOf(job.CreatedAt),
		keyJobReference:   jobReference(project, job),
		keySchema:         map[string]any{keyFields: job.Fields},
		"totalRows":       strconv.Itoa(len(job.Rows)),
		"jobComplete":     true,
		keyCacheHit:       false,
		keyBytesProcessed: "0",
	}
	if end > start {
		response["rows"] = bqRows(job.Fields, job.Rows[start:end])
	}
	if end < len(job.Rows) {
		response["pageToken"] = strconv.Itoa(end)
	}
	return response
}
