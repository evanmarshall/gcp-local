package bigquery

import (
	"encoding/base64"
	"strconv"
	"time"
)

// Wire keys repeated across the dataset, table, and query documents.
const (
	keyKind         = "kind"
	keyETag         = "etag"
	keyLocation     = "location"
	keyCreationTime = "creationTime"
	keyProjectID    = "projectId"
)

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
		keyKind:               "bigquery#queryResponse",
		"jobReference":        map[string]any{keyProjectID: project, "jobId": job.ID, keyLocation: location},
		"jobComplete":         true,
		keyLocation:           location,
		"schema":              map[string]any{"fields": job.Fields},
		"statementType":       "SELECT",
		"cacheHit":            false,
		keyCreationTime:       created,
		"endTime":             created,
		"totalBytesProcessed": "0",
		"totalBytesBilled":    "0",
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
