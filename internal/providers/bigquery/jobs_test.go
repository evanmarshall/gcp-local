package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

const (
	testJobs    = "/bigquery/v2/projects/" + testProject + "/jobs"
	testQueries = "/bigquery/v2/projects/" + testProject + "/queries"
	testSQL     = "SELECT title FROM `" + testProject + "." + testDataset + "." + testTable + "`"
)

// account names one test service account by its bearer token.
func account(token string) string {
	return "serviceAccount:" + token + "@" + testProject + ".iam.gserviceaccount.com"
}

// invokeAs issues one REST request as the principal the token names.
func (h *harness) invokeAs(token, verb, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body) //nolint:errchkjson // plain map bodies
	}
	request, _ := http.NewRequestWithContext(context.Background(), verb, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.gate.ServeHTTP(recorder, request)
	return recorder
}

// want fails the test unless the response has the code and contains text.
func want(t *testing.T, step string, recorder *httptest.ResponseRecorder, code int, text string) {
	t.Helper()
	if recorder.Code != code || !strings.Contains(recorder.Body.String(), text) {
		t.Errorf("%s: got %d %s, want %d %q", step, recorder.Code, recorder.Body.String(), code, text)
	}
}

// rows fills the fixture table with two titles.
func (h *harness) rows() {
	h.t.Helper()
	h.table()
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables/"+testTable+"/insertAll", map[string]any{
		"rows": []any{
			map[string]any{"json": map[string]any{"title": "fire-and-fog"}},
			map[string]any{"json": map[string]any{"title": "still-wind"}},
		},
	}, 200)
}

// jobBody is a jobs.insert body for one Standard SQL query; edit, when set,
// changes its reference, configuration, and query objects.
func jobBody(id, sql string, edit func(reference, configuration, query map[string]any)) map[string]any {
	reference := map[string]any{"projectId": testProject, "jobId": id}
	query := map[string]any{"query": sql, "useLegacySql": false}
	configuration := map[string]any{"query": query}
	if edit != nil {
		edit(reference, configuration, query)
	}
	return map[string]any{"jobReference": reference, "configuration": configuration}
}

func queryJob(id, sql string) map[string]any { return jobBody(id, sql, nil) }

// jobDocument is the slice of a job resource the tests read.
type jobDocument struct {
	ID            string `json:"id"`
	UserEmail     string `json:"user_email"`
	Configuration struct {
		JobType string `json:"jobType"`
	} `json:"configuration"`
	Status struct {
		State string `json:"state"`
	} `json:"status"`
}

// pageDocument is the slice of a getQueryResults page the tests read.
type pageDocument struct {
	TotalRows string  `json:"totalRows"`
	PageToken *string `json:"pageToken"`
	Rows      []struct {
		F []struct {
			V string `json:"v"`
		} `json:"f"`
	} `json:"rows"`
}

// decode reads a JSON response body into value.
func decode(t *testing.T, recorder *httptest.ResponseRecorder, value any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), value); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body.String(), err)
	}
}

// page reads one getQueryResults page as its first cells and its token.
func (h *harness) page(path string) ([]string, string) {
	h.t.Helper()
	var value pageDocument
	decode(h.t, h.expect(http.MethodGet, path, nil, 200), &value)
	cells := make([]string, 0, len(value.Rows))
	for _, row := range value.Rows {
		cells = append(cells, row.F[0].V)
	}
	token := "none"
	if value.PageToken != nil {
		token = *value.PageToken
	}
	return cells, token
}

func TestJobsInsertRunsAQuery(t *testing.T) {
	h := newHarness(t)
	h.rows()
	var job jobDocument
	decode(t, h.expect(http.MethodPost, testJobs, queryJob("job-1", testSQL), 200), &job)
	if job.Status.State != "DONE" || job.ID != testProject+":US.job-1" || job.Configuration.JobType != "QUERY" {
		t.Fatalf("inserted job = %+v", job)
	}
	decode(t, h.expect(http.MethodGet, testJobs+"/job-1?location=US", nil, 200), &job)
	if job.UserEmail != "actor@"+testProject+".iam.gserviceaccount.com" {
		t.Fatalf("job get = %+v", job)
	}
	h.expect(http.MethodPost, testJobs, queryJob("job-1", testSQL), 409)
	h.expect(http.MethodPost, testJobs, jobBody("dry-1", testSQL, func(_, configuration, _ map[string]any) {
		configuration["dryRun"] = true
	}), 200)
	h.expect(http.MethodGet, testJobs+"/dry-1", nil, 404)
}

// TestJobResultsPage covers maxResults=0, which still issues a pageToken (as
// recorded), page tokens, and the top-level fields selector.
func TestJobResultsPage(t *testing.T) {
	h := newHarness(t)
	h.rows()
	h.expect(http.MethodPost, testJobs, queryJob("job-1", testSQL), 200)
	pages := []struct {
		query string
		cells []string
		token string
	}{
		{"?maxResults=0&location=US", nil, "0"},
		{"?maxResults=1&fields=rows,pageToken", []string{"fire-and-fog"}, "1"},
		{"?maxResults=1&pageToken=1", []string{"still-wind"}, "none"},
		{"", []string{"fire-and-fog", "still-wind"}, "none"},
	}
	for _, page := range pages {
		cells, token := h.page(testQueries + "/job-1" + page.query)
		if strings.Join(cells, ",") != strings.Join(page.cells, ",") || token != page.token {
			t.Errorf("%s: cells %v token %s", page.query, cells, token)
		}
	}
	var selected map[string]any
	decode(t, h.expect(http.MethodGet, testQueries+"/job-1?fields=totalRows", nil, 200), &selected)
	if len(selected) != 1 || selected["totalRows"] != "2" {
		t.Fatalf("fields=totalRows = %v", selected)
	}
}

// TestJobRefusals asserts every refusal of the job methods and its message.
func TestJobRefusals(t *testing.T) {
	h := newHarness(t)
	h.rows()
	h.expect(http.MethodPost, testJobs, queryJob("stored", testSQL), 200)
	h.expect(http.MethodPost, testDatasets+"/"+testDataset+"/tables", map[string]any{
		"tableReference": map[string]any{"datasetId": testDataset, "tableId": "stamps"},
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "at", "type": "TIMESTAMP"}}},
	}, 200)
	h.expect(http.MethodPost, testJobs, queryJob("stamped", "SELECT at FROM "+testDataset+".stamps"), 200)
	edited := func(edit func(reference, configuration, query map[string]any)) map[string]any {
		return jobBody("refused", testSQL, edit)
	}
	foreign := "SELECT title FROM `other-project." + testDataset + "." + testTable + "`"
	tests := []struct {
		name, verb, path string
		body             any
		code             int
		message          string
	}{
		{"legacy default", http.MethodPost, testJobs, edited(func(_, _, q map[string]any) { delete(q, "useLegacySql") }), 501, "Legacy SQL is not supported"},
		{"load job", http.MethodPost, testJobs, edited(func(_, c, _ map[string]any) { c["load"] = map[string]any{} }), 501, "BigQuery coverage gap: job field load"},
		{"no query", http.MethodPost, testJobs, edited(func(_, c, _ map[string]any) { delete(c, "query") }), 501, "BigQuery coverage gap: only query jobs are supported"},
		{"destination", http.MethodPost, testJobs, edited(func(_, _, q map[string]any) { q["destinationTable"] = map[string]any{} }), 501, "BigQuery coverage gap: job field destinationTable"},
		{"batch", http.MethodPost, testJobs, edited(func(_, _, q map[string]any) { q["priority"] = "BATCH" }), 501, "BigQuery coverage gap: query priority BATCH"},
		{"foreign project", http.MethodPost, testJobs, edited(func(r, _, _ map[string]any) { r["projectId"] = "other-project" }), 400, "Job project disagrees with the request path"},
		{"bad job id", http.MethodPost, testJobs, edited(func(r, _, _ map[string]any) { r["jobId"] = "bad id" }), 400, "Invalid job id"},
		{"other location", http.MethodPost, testJobs, edited(func(r, _, _ map[string]any) { r["location"] = "EU" }), 404, "was not found in location EU"},
		{"cross-project table", http.MethodPost, testJobs, queryJob("x", foreign), 501, "BigQuery coverage gap: queries read only tables in the job's project"},
		{"cross-project jobs.query", http.MethodPost, testQueries, map[string]any{"query": foreign}, 501, "queries read only tables in the job's project"},
		{"insert option", http.MethodPost, testJobs + "?userProject=x", queryJob("y", testSQL), 501, "BigQuery coverage gap: query parameter userProject"},
		{"get option", http.MethodGet, testJobs + "/stored?projection=full", nil, 501, "BigQuery coverage gap: query parameter projection"},
		{"nested fields", http.MethodGet, testQueries + "/stored?fields=schema/fields", nil, 501, "BigQuery coverage gap: nested fields selectors"},
		{"media", http.MethodGet, testQueries + "/stored?alt=media", nil, 501, "BigQuery coverage gap: alt=media"},
		{"get other location", http.MethodGet, testJobs + "/stored?location=EU", nil, 404, "job/stored"},
		{"results other location", http.MethodGet, testQueries + "/stored?location=EU", nil, 404, "job/stored"},
		{"bad maxResults", http.MethodGet, testQueries + "/stored?maxResults=-1", nil, 400, "Invalid maxResults"},
		{"bad pageToken", http.MethodGet, testQueries + "/stored?pageToken=next", nil, 400, "Invalid pageToken"},
		{"int64 timestamps", http.MethodGet, testQueries + "/stamped?formatOptions.useInt64Timestamp=true", nil, 501, "BigQuery coverage gap: result format options on TIMESTAMP columns"},
		{"timestamp format", http.MethodGet, testQueries + "/stamped?formatOptions.timestampOutputFormat=ISO8601_STRING", nil, 501, "result format options on TIMESTAMP columns"},
	}
	for _, test := range tests {
		want(t, test.name, h.invoke(test.verb, test.path, test.body), test.code, test.message)
	}
	// Format options are accepted where they change nothing.
	h.expect(http.MethodGet, testQueries+"/stored?formatOptions.useInt64Timestamp=true&timeoutMs=1000", nil, 200)
	h.expect(http.MethodGet, testJobs+"/refused", nil, 404)
}

// grantJobUsers gives the actor its roles and runner and peer job user only.
func (h *harness) grantJobUsers() {
	h.t.Helper()
	if err := h.engineSet(iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataOwner", Members: []string{testActor}},
		{Role: "roles/bigquery.jobUser", Members: []string{testActor, account("runner"), account("peer")}},
		{Role: "roles/bigquery.resourceViewer", Members: []string{testActor}},
	}}); err != nil {
		h.t.Fatal(err)
	}
}

// TestJobsAuthorizeTheTable covers the table read check, which runs after
// bigquery.jobs.create and before the job is stored.
func TestJobsAuthorizeTheTable(t *testing.T) {
	h := newHarness(t)
	h.rows()
	h.grantJobUsers()
	denied := h.invokeAs("runner", http.MethodPost, testJobs, queryJob("denied", testSQL))
	want(t, "query without table access", denied, 403, "bigquery.tables.getData")
	if denied.Header().Get(headerDecision) == "" {
		t.Error("the table denial names no decision")
	}
	want(t, "jobs.query without table access", h.invokeAs("runner", http.MethodPost, testQueries, map[string]any{"query": testSQL}), 403, "bigquery.tables.getData")
	h.expect(http.MethodGet, testJobs+"/denied", nil, 404)
	want(t, "job user read of a missing job", h.invokeAs("runner", http.MethodGet, testJobs+"/denied", nil), 404, "job/denied")
	want(t, "ungranted read of a missing job", h.invokeAs("stranger", http.MethodGet, testJobs+"/denied", nil), 403, "bigquery.jobs.create")
	if _, err := h.engine.SetPolicy(tableResource(testProject, testDataset, testTable), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataViewer", Members: []string{account("reader")}},
	}}); err != nil {
		t.Fatal(err)
	}
	want(t, "query without jobs.create", h.invokeAs("reader", http.MethodPost, testJobs, queryJob("reader", testSQL)), 403, "bigquery.jobs.create")
}

// TestJobsCreatorReadsItsJob covers the creator's read of a job without
// bigquery.jobs.get, and another job user's denied read.
func TestJobsCreatorReadsItsJob(t *testing.T) {
	h := newHarness(t)
	h.rows()
	h.grantJobUsers()
	if _, err := h.engine.SetPolicy(tableResource(testProject, testDataset, testTable), iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataViewer", Members: []string{account("runner")}},
	}}); err != nil {
		t.Fatal(err)
	}
	want(t, "granted query", h.invokeAs("runner", http.MethodPost, testJobs, queryJob("owned", testSQL)), 200, "owned")
	for _, path := range []string{testJobs + "/owned", testQueries + "/owned"} {
		want(t, "creator read "+path, h.invokeAs("runner", http.MethodGet, path, nil), 200, "owned")
		want(t, "another job user read "+path, h.invokeAs("peer", http.MethodGet, path, nil), 403, "bigquery.jobs.get")
		h.expect(http.MethodGet, path, nil, 200)
	}
}
