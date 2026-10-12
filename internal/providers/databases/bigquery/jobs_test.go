package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
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
		"schema":         map[string]any{"fields": []any{map[string]any{"name": "stamp", "type": "TIMESTAMP"}}},
	}, 200)
	h.expect(http.MethodPost, testJobs, queryJob("stamped", "SELECT stamp FROM "+testDataset+".stamps"), 200)
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
		{"cross-project table", http.MethodPost, testJobs, queryJob("x", foreign), 501, "BigQuery coverage gap: cross-project table references (other-project." + testDataset + "." + testTable + ")"},
		{"cross-project jobs.query", http.MethodPost, testQueries, map[string]any{"query": foreign}, 501, "BigQuery coverage gap: cross-project table references"},
		{"insert option", http.MethodPost, testJobs + "?userProject=x", queryJob("y", testSQL), 501, "BigQuery coverage gap: query parameter userProject"},
		{"get option", http.MethodGet, testJobs + "/stored?projection=full", nil, 501, "BigQuery coverage gap: query parameter projection"},
		{"nested fields", http.MethodGet, testQueries + "/stored?fields=schema/fields", nil, 501, "BigQuery coverage gap: nested fields selectors"},
		{"media", http.MethodGet, testQueries + "/stored?alt=media", nil, 501, "BigQuery coverage gap: alt=media"},
		{"get other location", http.MethodGet, testJobs + "/stored?location=EU", nil, 404, "job/stored"},
		{"results other location", http.MethodGet, testQueries + "/stored?location=EU", nil, 404, "job/stored"},
		{"bad maxResults", http.MethodGet, testQueries + "/stored?maxResults=-1", nil, 400, "Invalid maxResults"},
		{"bad pageToken", http.MethodGet, testQueries + "/stored?pageToken=next", nil, 400, "Invalid pageToken"},
		{"timestamp format", http.MethodGet, testQueries + "/stamped?formatOptions.timestampOutputFormat=ISO8601_STRING", nil, 501, "BigQuery coverage gap: formatOptions.timestampOutputFormat=ISO8601_STRING"},
	}
	for _, test := range tests {
		want(t, test.name, h.invoke(test.verb, test.path, test.body), test.code, test.message)
	}
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

// queryAs runs one jobs.query as the token's principal; extra adds body
// fields such as queryParameters.
func (h *harness) queryAs(token, sql string, extra map[string]any) *httptest.ResponseRecorder {
	h.t.Helper()
	body := map[string]any{"query": sql, "useLegacySql": false}
	maps.Copy(body, extra)
	return h.invokeAs(token, http.MethodPost, testQueries, body)
}

// rowsOf reads a query or tabledata response's rows as top-level cell
// strings; a nested cell renders as its JSON.
func rowsOf(t *testing.T, recorder *httptest.ResponseRecorder) [][]string {
	t.Helper()
	var value struct {
		Rows []struct {
			F []struct {
				V any `json:"v"`
			} `json:"f"`
		} `json:"rows"`
	}
	decode(t, recorder, &value)
	rows := make([][]string, 0, len(value.Rows))
	for _, row := range value.Rows {
		cells := []string{}
		for _, cell := range row.F {
			switch typed := cell.V.(type) {
			case string:
				cells = append(cells, typed)
			case nil:
				cells = append(cells, "NULL")
			default:
				encoded, _ := json.Marshal(typed) //nolint:errchkjson // decoded JSON
				cells = append(cells, string(encoded))
			}
		}
		rows = append(rows, cells)
	}
	return rows
}

// rowsAre fails unless a successful query returned exactly these rows.
func (h *harness) rowsAre(step string, recorder *httptest.ResponseRecorder, want ...string) {
	h.t.Helper()
	if recorder.Code != 200 {
		h.t.Errorf("%s: status %d %s", step, recorder.Code, recorder.Body.String())
		return
	}
	got := []string{}
	for _, row := range rowsOf(h.t, recorder) {
		got = append(got, strings.Join(row, ","))
	}
	if !reflect.DeepEqual(got, append([]string{}, want...)) {
		h.t.Errorf("%s: rows %q, want %q", step, got, want)
	}
}

// people creates two datasets of tables the SQL tests share: shop.people,
// shop.orders, and vault.secrets.
func (h *harness) people() {
	h.t.Helper()
	for _, dataset := range []string{"shop", "vault"} {
		h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": dataset}}, 200)
	}
	table := func(dataset, name string, fields []any, rows []any) {
		h.expect(http.MethodPost, testDatasets+"/"+dataset+"/tables", map[string]any{
			"tableReference": map[string]any{"tableId": name}, "schema": map[string]any{"fields": fields},
		}, 200)
		recorder := h.expect(http.MethodPost, testDatasets+"/"+dataset+"/tables/"+name+"/insertAll", map[string]any{"rows": rows}, 200)
		if strings.Contains(recorder.Body.String(), "insertErrors") {
			h.t.Fatalf("seed %s.%s: %s", dataset, name, recorder.Body.String())
		}
	}
	field := func(name, kind string) any { return map[string]any{"name": name, "type": kind} }
	row := func(values map[string]any) any { return map[string]any{"json": values} }
	table("shop", "people", []any{field("id", "INTEGER"), field("name", "STRING"), field("active", "BOOLEAN"), field("joined", "DATE")}, []any{
		row(map[string]any{"id": 1, "name": "ada", "active": true, "joined": "2026-01-02"}),
		row(map[string]any{"id": 2, "name": "grace", "active": false, "joined": "2026-02-03"}),
		row(map[string]any{"id": 3, "name": "linus", "active": true, "joined": "2026-03-04"}),
	})
	table("shop", "orders", []any{field("person", "INTEGER"), field("total", "FLOAT")}, []any{
		row(map[string]any{"person": 1, "total": 10.5}),
		row(map[string]any{"person": 1, "total": 4.5}),
		row(map[string]any{"person": 3, "total": 7}),
	})
	table("vault", "secrets", []any{field("person", "INTEGER"), field("code", "STRING")}, []any{
		row(map[string]any{"person": 1, "code": "s3cret"}),
	})
}

// readerOf grants the "reader" principal job user on the project and
// dataViewer on the named tables only.
func (h *harness) readerOf(tables ...string) {
	const token = "reader"
	h.t.Helper()
	if err := h.engineSet(iam.Policy{Bindings: []iam.Binding{
		{Role: "roles/bigquery.dataOwner", Members: []string{testActor}},
		{Role: "roles/bigquery.jobUser", Members: []string{testActor, account(token)}},
		{Role: "roles/bigquery.resourceViewer", Members: []string{testActor}},
	}}); err != nil {
		h.t.Fatal(err)
	}
	for _, table := range tables {
		dataset, id, _ := strings.Cut(table, ".")
		if _, err := h.engine.SetPolicy(tableResource(testProject, dataset, id), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/bigquery.dataViewer", Members: []string{account(token)}},
		}}); err != nil {
			h.t.Fatal(err)
		}
	}
}

func TestQueryFeatures(t *testing.T) {
	h := newHarness(t)
	h.people()
	cases := []struct {
		name, sql string
		want      []string
	}{
		{"join and aggregate", "SELECT p.name, COUNT(*) AS n, SUM(o.total) AS total FROM shop.people p JOIN shop.orders o ON o.person = p.id GROUP BY p.name ORDER BY p.name", []string{"ada,2,15.0", "linus,1,7.0"}},
		{"left join", "SELECT p.name, o.total FROM shop.people p LEFT JOIN shop.orders o ON o.person = p.id WHERE p.id = 2", []string{"grace,NULL"}},
		{"having", "SELECT person, SUM(total) AS t FROM shop.orders GROUP BY person HAVING SUM(total) > 10", []string{"1,15.0"}},
		{"order and limit", "SELECT name FROM shop.people ORDER BY id DESC LIMIT 2", []string{"linus", "grace"}},
		{"cte", "WITH spenders AS (SELECT person FROM shop.orders WHERE total > 5) SELECT name FROM shop.people WHERE id IN (SELECT person FROM spenders) ORDER BY name", []string{"ada", "linus"}},
		{"scalar subquery", "SELECT (SELECT MAX(total) FROM shop.orders) AS top", []string{"10.5"}},
		{"functions", "SELECT UPPER(name), CONCAT(name, '!'), EXTRACT(YEAR FROM joined), DATE_ADD(joined, INTERVAL 1 DAY) FROM shop.people WHERE id = 1", []string{"ADA,ada!,2026,2026-01-03"}},
		{"window", "SELECT name, ROW_NUMBER() OVER (ORDER BY id DESC) FROM shop.people ORDER BY id", []string{"ada,3", "grace,2", "linus,1"}},
		// The parser the engine replaced returned no rows for these (audit).
		{"AND filter", "SELECT name FROM shop.people WHERE active = true AND id > 1", []string{"linus"}},
		{"boolean literal", "SELECT name FROM shop.people WHERE active = true ORDER BY id", []string{"ada", "linus"}},
		{"multi-line", "SELECT name\nFROM shop.people\n-- a comment\nWHERE id = 2", []string{"grace"}},
		{"backtick project path", "SELECT name FROM `" + testProject + ".shop.people` WHERE id = 3", []string{"linus"}},
		{"dashed project path", "SELECT COUNT(*) FROM " + testProject + ".shop.people", []string{"3"}},
		{"script returns its last result", "DECLARE n INT64 DEFAULT 1; SET n = n + 1; SELECT name FROM shop.people WHERE id = n", []string{"grace"}},
		{"case folds like the engine", "SELECT COUNT(*) FROM SHOP.PEOPLE", []string{"3"}},
		{"date literal", "SELECT DATE '2026-01-02', CAST(DATE '2026-01-02' AS STRING)", []string{"2026-01-02,2026-01-02"}},
	}
	for _, c := range cases {
		h.rowsAre(c.name, h.queryAs(testToken, c.sql, nil), c.want...)
	}
	h.rowsAre("default dataset", h.queryAs(testToken, "SELECT name FROM people WHERE id = 1",
		map[string]any{"defaultDataset": map[string]any{"datasetId": "shop"}}), "ada")
}

// The parser the engine replaced ignored parameters (audit).
func TestQueryParameters(t *testing.T) {
	h := newHarness(t)
	h.people()
	scalar := func(name, kind, value string) any {
		return map[string]any{"name": name, "parameterType": map[string]any{"type": kind}, "parameterValue": map[string]any{"value": value}}
	}
	h.rowsAre("named", h.queryAs(testToken, "SELECT name FROM shop.people WHERE id = @id AND active = @on",
		map[string]any{"parameterMode": "NAMED", "queryParameters": []any{scalar("id", "INT64", "3"), scalar("on", "BOOL", "true")}}), "linus")
	h.rowsAre("positional", h.queryAs(testToken, "SELECT name FROM shop.people WHERE name = ? OR id = ? ORDER BY id",
		map[string]any{"parameterMode": "POSITIONAL", "queryParameters": []any{scalar("", "STRING", "ada"), scalar("", "INT64", "2")}}), "ada", "grace")
	h.rowsAre("date cast", h.queryAs(testToken, "SELECT name FROM shop.people WHERE joined > @after ORDER BY id",
		map[string]any{"parameterMode": "NAMED", "queryParameters": []any{scalar("after", "DATE", "2026-02-01")}}), "grace", "linus")
	h.rowsAre("array", h.queryAs(testToken, "SELECT name FROM shop.people WHERE id IN UNNEST(@ids) ORDER BY id",
		map[string]any{"parameterMode": "NAMED", "queryParameters": []any{map[string]any{
			"name": "ids", "parameterType": map[string]any{"type": "ARRAY", "arrayType": map[string]any{"type": "INT64"}},
			"parameterValue": map[string]any{"arrayValues": []any{map[string]any{"value": "1"}, map[string]any{"value": "3"}}},
		}}}), "ada", "linus")
	h.rowsAre("null", h.queryAs(testToken, "SELECT @n IS NULL",
		map[string]any{"parameterMode": "NAMED", "queryParameters": []any{map[string]any{"name": "n", "parameterType": map[string]any{"type": "INT64"}}}}), "true")
	refusals := []struct {
		name       string
		sql        string
		parameters []any
		code       int
		message    string
	}{
		{"missing", "SELECT @absent", nil, 400, "Query parameter 'absent' not found"},
		{"bad value", "SELECT @id", []any{scalar("id", "INT64", "x")}, 400, "Invalid INT64 query parameter value"},
		{"struct", "SELECT @s", []any{map[string]any{"name": "s", "parameterType": map[string]any{"type": "STRUCT"}}}, 501, "BigQuery coverage gap: STRUCT query parameters"},
		{"no type", "SELECT @s", []any{map[string]any{"name": "s"}}, 400, "A query parameter requires a parameterType"},
	}
	for _, r := range refusals {
		want(t, r.name, h.queryAs(testToken, r.sql, map[string]any{"parameterMode": "NAMED", "queryParameters": r.parameters}), r.code, r.message)
	}
	want(t, "positional without value", h.queryAs(testToken, "SELECT ?", map[string]any{"parameterMode": "POSITIONAL"}), 400, "Positional parameter 1 has no value")
}

func TestDMLAndDDL(t *testing.T) {
	h := newHarness(t)
	h.people()
	affected := func(step string, recorder *httptest.ResponseRecorder, count string) {
		t.Helper()
		var value struct {
			Affected      string `json:"numDmlAffectedRows"`
			StatementType string `json:"statementType"`
		}
		decode(t, recorder, &value)
		if recorder.Code != 200 || value.Affected != count {
			t.Errorf("%s: %d %s, want %s affected rows", step, recorder.Code, recorder.Body.String(), count)
		}
	}
	affected("insert", h.queryAs(testToken, "INSERT INTO shop.people (id, name, active) VALUES (4, 'ken', true), (5, 'bjarne', false)", nil), "2")
	affected("update", h.queryAs(testToken, "UPDATE shop.people SET active = false WHERE id IN (1, 4)", nil), "2")
	affected("delete", h.queryAs(testToken, "DELETE FROM shop.people WHERE id = 5", nil), "1")
	want(t, "merge", h.queryAs(testToken, "MERGE shop.people T USING shop.orders S ON T.id = S.person WHEN MATCHED THEN UPDATE SET active = true", nil), 200, `"statementType":"MERGE"`)
	h.rowsAre("rows after DML", h.invoke(http.MethodGet, testDatasets+"/shop/tables/people/data", nil),
		"1,ada,true,2026-01-02", "2,grace,false,2026-02-03", "3,linus,true,2026-03-04", "4,ken,false,NULL")

	// DDL creates, replaces, and drops tables and views the REST surface sees.
	want(t, "create table", h.queryAs(testToken, "CREATE TABLE shop.totals (person INT64, total FLOAT64)", nil), 200, `"statementType":"CREATE_TABLE"`)
	want(t, "table metadata", h.invoke(http.MethodGet, testDatasets+"/shop/tables/totals", nil), 200, `"name":"person","type":"INTEGER"`)
	h.expect(http.MethodPost, testDatasets+"/shop/tables/totals/insertAll", map[string]any{"rows": []any{map[string]any{"json": map[string]any{"person": 9, "total": 1}}}}, 200)
	want(t, "create as select", h.queryAs(testToken, "CREATE OR REPLACE TABLE shop.totals AS SELECT person, SUM(total) AS total FROM shop.orders GROUP BY person", nil), 200, "CREATE_TABLE_AS_SELECT")
	h.rowsAre("replaced rows", h.queryAs(testToken, "SELECT person, total FROM shop.totals ORDER BY person", nil), "1,15.0", "3,7.0")
	want(t, "create view", h.queryAs(testToken, "CREATE VIEW shop.big AS SELECT person FROM shop.orders WHERE total > 5", nil), 200, "CREATE_VIEW")
	want(t, "view metadata", h.invoke(http.MethodGet, testDatasets+"/shop/tables/big", nil), 200, `"type":"VIEW"`)
	h.rowsAre("query a view", h.queryAs(testToken, "SELECT COUNT(*) FROM shop.big", nil), "2")
	want(t, "list a view", h.invoke(http.MethodGet, testDatasets+"/shop/tables/big/data", nil), 400, "Cannot list a table of type VIEW.")
	want(t, "drop view", h.queryAs(testToken, "DROP VIEW shop.big", nil), 200, "DROP_VIEW")
	want(t, "drop table", h.queryAs(testToken, "DROP TABLE shop.totals", nil), 200, "DROP_TABLE")
	h.expect(http.MethodGet, testDatasets+"/shop/tables/totals", nil, 404)
	h.expect(http.MethodGet, testDatasets+"/shop/tables/big", nil, 404)
	want(t, "dropped table", h.queryAs(testToken, "SELECT person FROM shop.totals", nil), 404, "Not found: Table")

	// A script that creates a table and then fails keeps the table, as its
	// first statement already ran.
	want(t, "partial script", h.queryAs(testToken, "CREATE TABLE shop.kept (x INT64); SELECT ERROR('boom')", nil), 400, "boom")
	h.expect(http.MethodGet, testDatasets+"/shop/tables/kept", nil, 200)

	// DDL-created tables delete through the REST surface too.
	h.expect(http.MethodDelete, testDatasets+"/shop/tables/kept", nil, 204)
	want(t, "deleted through REST", h.queryAs(testToken, "SELECT x FROM shop.kept", nil), 404, "Not found: Table")
}

// Every table a statement names is authorized before anything runs.
func TestQueryAuthorizesEveryTable(t *testing.T) {
	h := newHarness(t)
	h.people()
	h.readerOf("shop.people", "shop.orders")
	ok := "SELECT p.name FROM shop.people p JOIN shop.orders o ON o.person = p.id WHERE o.total > 10"
	h.rowsAre("granted join", h.queryAs("reader", ok, nil), "ada")
	denied := []struct{ name, sql, resource string }{
		{"second table in a join", "SELECT p.name, s.code FROM shop.people p JOIN vault.secrets s ON s.person = p.id", "vault/tables/secrets"},
		{"subquery", "SELECT name FROM shop.people WHERE id IN (SELECT person FROM vault.secrets)", "vault/tables/secrets"},
		{"cte", "WITH s AS (SELECT * FROM vault.secrets) SELECT * FROM s", "vault/tables/secrets"},
		{"case folded name", "SELECT * FROM VAULT.Secrets", "vault/tables/secrets"},
		{"behind a comment line", "SELECT 1 -- note\nFROM vault.secrets", "vault/tables/secrets"},
		{"later script statement", "SELECT 1; SELECT * FROM vault.secrets", "vault/tables/secrets"},
		{"correlated-looking alias", "SELECT * FROM shop.people AS vault, vault.secrets", "vault/tables/secrets"},
		{"table argument", "SELECT * FROM APPENDS(TABLE vault.secrets, NULL, NULL)", "vault/tables/secrets"},
		{"missing table in a readable dataset", "SELECT * FROM vault.absent", "datasets/vault"},
	}
	for _, d := range denied {
		recorder := h.queryAs("reader", d.sql, nil)
		want(t, d.name, recorder, 403, "bigquery.tables.getData")
		want(t, d.name+" names the resource", recorder, 403, d.resource)
	}
	// A string that spells a node line forges nothing; the real read of
	// vault.secrets is still checked.
	want(t, "forged dump line", h.queryAs("reader", "SELECT '''x\n      TablePathExpression [0-1]''' AS s FROM vault.secrets", nil), 403, "vault/tables/secrets")

	before := rowsOf(t, h.invoke(http.MethodGet, testDatasets+"/shop/tables/people/data", nil))
	writes := []struct{ name, sql, permission string }{
		{"insert", "INSERT INTO shop.people (id) VALUES (9)", "bigquery.tables.updateData"},
		{"update", "UPDATE shop.people SET name = 'x' WHERE TRUE", "bigquery.tables.updateData"},
		{"delete", "DELETE FROM shop.people WHERE TRUE", "bigquery.tables.updateData"},
		{"merge", "MERGE shop.people T USING shop.orders S ON T.id = S.person WHEN MATCHED THEN DELETE", "bigquery.tables.updateData"},
		{"truncate", "TRUNCATE TABLE shop.people", "bigquery.tables.updateData"},
		{"create", "CREATE TABLE shop.copy AS SELECT * FROM shop.people", "bigquery.tables.create"},
		{"replace", "CREATE OR REPLACE TABLE shop.people (id INT64)", "bigquery.tables.create"},
		{"drop", "DROP TABLE shop.orders", "bigquery.tables.delete"},
		{"write after a read", "SELECT 1 FROM shop.people; DELETE FROM shop.people WHERE TRUE", "bigquery.tables.updateData"},
	}
	for _, w := range writes {
		want(t, w.name, h.queryAs("reader", w.sql, nil), 403, w.permission)
	}
	after := rowsOf(t, h.invoke(http.MethodGet, testDatasets+"/shop/tables/people/data", nil))
	if !reflect.DeepEqual(before, after) {
		t.Errorf("denied writes changed rows: %v -> %v", before, after)
	}
	h.expect(http.MethodGet, testDatasets+"/shop/tables/copy", nil, 404)
	h.expect(http.MethodGet, testDatasets+"/shop/tables/orders", nil, 200)
}

// A view reads its tables: querying it needs getData on them too.
func TestViewNeedsItsTables(t *testing.T) {
	h := newHarness(t)
	h.people()
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "CREATE VIEW shop.leak AS SELECT code FROM vault.secrets"}, 200)
	h.readerOf("shop.leak")
	want(t, "view over an unreadable table", h.queryAs("reader", "SELECT code FROM shop.leak", nil), 403, "vault/tables/secrets")
	h.readerOf("shop.leak", "vault.secrets")
	h.rowsAre("view and table readable", h.queryAs("reader", "SELECT code FROM shop.leak", nil), "s3cret")
}

// A bare name resolves in the default dataset, as in GCP; the engine alone
// would pick whichever table of that name it registered first.
func TestDefaultDatasetPicksItsTable(t *testing.T) {
	h := newHarness(t)
	h.people()
	h.expect(http.MethodPost, testQueries, map[string]any{"query": "CREATE TABLE vault.people AS SELECT 'vault' AS name"}, 200)
	h.readerOf("vault.people")
	h.rowsAre("default dataset table", h.queryAs("reader", "SELECT name FROM people",
		map[string]any{"defaultDataset": map[string]any{"projectId": testProject, "datasetId": "vault"}}), "vault")
	want(t, "unqualified without a default", h.queryAs("reader", "SELECT name FROM people", nil), 400,
		`Table \"people\" must be qualified with a dataset (e.g. dataset.table).`)
}

// TestQueryRefusals asserts each explicit refusal and its message.
func TestQueryRefusals(t *testing.T) {
	h := newHarness(t)
	h.people()
	refusals := []struct {
		name, sql string
		code      int
		message   string
	}{
		{"execute immediate", "EXECUTE IMMEDIATE 'SELECT 1'", 501, "BigQuery coverage gap: statement ExecuteImmediateStatement"},
		{"call", "CALL shop.proc()", 501, "BigQuery coverage gap: statement CallStatement"},
		{"persistent function", "CREATE FUNCTION shop.f(x INT64) AS (x)", 501, "BigQuery coverage gap: statement CreateFunctionStatement"},
		{"transaction", "BEGIN TRANSACTION; SELECT 1; COMMIT TRANSACTION", 501, "BigQuery coverage gap: statement BeginStatement"},
		{"system variable", "SET @@dataset_id = 'vault'; SELECT 1", 501, "BigQuery coverage gap: statement SystemVariableAssignment"},
		{"alter", "ALTER TABLE shop.people ADD COLUMN x INT64", 501, "BigQuery coverage gap: statement AlterTableStatement"},
		{"drop schema", "DROP SCHEMA shop", 501, "BigQuery coverage gap: DROP SCHEMA"},
		{"temporary table", "CREATE TEMP TABLE t AS SELECT 1 AS x", 501, "BigQuery coverage gap: temporary tables"},
		{"create like", "CREATE TABLE shop.p2 LIKE shop.people", 501, "BigQuery coverage gap: CREATE TABLE ... LIKE"},
		{"create copy", "CREATE TABLE shop.p2 COPY shop.people", 501, "BigQuery coverage gap: CREATE TABLE ... COPY"},
		{"information schema", "SELECT * FROM shop.INFORMATION_SCHEMA.TABLES", 501, "BigQuery coverage gap: INFORMATION_SCHEMA"},
		{"wildcard", "SELECT * FROM `shop.peo*`", 501, "BigQuery coverage gap: wildcard tables"},
		{"cross project", "SELECT * FROM `other-project.shop.people`", 501, "BigQuery coverage gap: cross-project table references (other-project.shop.people)"},
		{"missing dataset", "SELECT * FROM nowhere.t", 404, "Not found: Dataset " + testProject + ":nowhere"},
		{"missing table", "SELECT * FROM shop.absent", 404, "Not found: Table"},
		{"syntax", "SELEC 1", 400, "Syntax error"},
		{"unknown column", "SELECT nope FROM shop.people", 400, "Unrecognized name: nope"},
		{"engine gap", "MERGE shop.people T USING (SELECT 1 AS id) S ON T.id = S.id WHEN MATCHED THEN DELETE", 501, "BigQuery coverage gap: the engine does not run this query"},
		{"storage collision", "CREATE TABLE shop_people.x (a INT64); CREATE TABLE shop.people_x (a INT64)", 501, "share one storage name in the local engine"},
	}
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": "shop_people"}}, 200)
	for _, r := range refusals {
		want(t, r.name, h.queryAs(testToken, r.sql, nil), r.code, r.message)
	}
	h.expect(http.MethodGet, testDatasets+"/shop_people/tables/x", nil, 404)
	want(t, "api storage collision", h.invoke(http.MethodPost, testDatasets+"/shop_people/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "x"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "INTEGER"}}},
	}), 200, `"tableId":"x"`)
	want(t, "api storage collision", h.invoke(http.MethodPost, testDatasets+"/shop/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "people_x"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "a", "type": "INTEGER"}}},
	}), 501, "tables shop_people.x and shop.people_x share one storage name in the local engine")
	want(t, "dataset case collision", h.invoke(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": "SHOP"}}), 501,
		"BigQuery coverage gap: datasets shop and SHOP differ only in case")
	want(t, "view through tables.insert", h.invoke(http.MethodPost, testDatasets+"/shop/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "v"}, "view": map[string]any{"query": "SELECT 1"},
	}), 501, "BigQuery coverage gap: views through tables.insert; create them with CREATE VIEW")
	want(t, "unknown jobs.query field", h.queryAs(testToken, "SELECT 1", map[string]any{"createSession": true}), 501, "BigQuery coverage gap: job field createSession")
	want(t, "legacy jobs.query", h.queryAs(testToken, "SELECT 1", map[string]any{"useLegacySql": true}), 501, "Legacy SQL is not supported")
	want(t, "cross-project default dataset", h.queryAs(testToken, "SELECT 1", map[string]any{"defaultDataset": map[string]any{"projectId": "other", "datasetId": "shop"}}),
		501, "BigQuery coverage gap: cross-project table references (default dataset other.shop)")
}

func TestInsertAllValidation(t *testing.T) {
	h := newHarness(t)
	h.people()
	rows := func(values ...map[string]any) map[string]any {
		out := make([]any, 0, len(values))
		for _, value := range values {
			out = append(out, map[string]any{"json": value})
		}
		return map[string]any{"rows": out}
	}
	insert := testDatasets + "/shop/tables/people/insertAll"
	want(t, "bad value stops the request", h.invoke(http.MethodPost, insert, rows(map[string]any{"id": 7}, map[string]any{"id": "seven"})), 200, `{"errors":[{"message":"Cannot convert value of field id: not a valid INT64","reason":"invalid"}],"index":1}`)
	want(t, "unknown field", h.invoke(http.MethodPost, insert, rows(map[string]any{"nope": 1})), 200, `"message":"no such field: nope.","reason":"invalid"`)
	h.rowsAre("nothing inserted", h.queryAs(testToken, "SELECT COUNT(*) FROM shop.people", nil), "3")
	h.expect(http.MethodPost, insert, map[string]any{"rows": []any{map[string]any{"json": map[string]any{"id": 8, "nope": 1}}}, "ignoreUnknownValues": true}, 200)
	want(t, "skipInvalidRows", h.invoke(http.MethodPost, insert, map[string]any{"rows": []any{map[string]any{"json": map[string]any{"id": 1}}}, "skipInvalidRows": true}), 501, "BigQuery coverage gap: insertAll skipInvalidRows")
	h.expect(http.MethodPost, testDatasets+"/shop/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "strict"}, "schema": map[string]any{"fields": []any{
			map[string]any{"name": "k", "type": "STRING", "mode": "REQUIRED"},
			map[string]any{"name": "at", "type": "TIMESTAMP"},
			map[string]any{"name": "tags", "type": "STRING", "mode": "REPEATED"},
		}},
	}, 200)
	strict := testDatasets + "/shop/tables/strict"
	want(t, "required field", h.invoke(http.MethodPost, strict+"/insertAll", rows(map[string]any{"at": "2026-01-02 03:04:05"})), 200, `"reason":"invalid"`)
	h.expect(http.MethodPost, strict+"/insertAll", rows(map[string]any{"k": "a", "at": "2026-01-02T03:04:05.5Z", "tags": []any{"x", "y"}}), 200)
	h.rowsAre("float timestamps and arrays", h.invoke(http.MethodGet, strict+"/data", nil), `a,1.7673230455E9,[{"v":"x"},{"v":"y"}]`)
	h.rowsAre("int64 timestamps", h.invoke(http.MethodGet, strict+"/data?formatOptions.useInt64Timestamp=true", nil), `a,1767323045500000,[{"v":"x"},{"v":"y"}]`)
	want(t, "record column gap", h.invoke(http.MethodPost, testDatasets+"/shop/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "nested"}, "schema": map[string]any{"fields": []any{map[string]any{
			"name": "r", "type": "RECORD", "fields": []any{map[string]any{"name": "a", "type": "INTEGER"}}}}},
	}), 200, `"tableId":"nested"`)
	want(t, "record insert gap", h.invoke(http.MethodPost, testDatasets+"/shop/tables/nested/insertAll", rows(map[string]any{"r": map[string]any{"a": 1}})), 501, "BigQuery coverage gap: insertAll into RECORD columns")
	want(t, "selectedFields", h.invoke(http.MethodGet, strict+"/data?selectedFields=k", nil), 501, "BigQuery coverage gap: query parameter selectedFields")
	want(t, "page", h.invoke(http.MethodGet, testDatasets+"/shop/tables/people/data?maxResults=1&startIndex=1", nil), 200, `"pageToken":"2"`)
}

// Each project has its own engine, and reset clears it.
func TestEngineScopes(t *testing.T) {
	h := newHarness(t)
	h.people()
	peer := "/bigquery/v2/projects/" + testPeerProject
	want(t, "peer project sees no dataset", h.invoke(http.MethodPost, peer+"/queries", map[string]any{"query": "SELECT * FROM shop.people"}), 404, "Not found: Dataset "+testPeerProject+":shop")
	h.expect(http.MethodPost, peer+"/datasets", map[string]any{"datasetReference": map[string]any{"datasetId": "shop"}}, 200)
	want(t, "peer dataset has no table", h.invoke(http.MethodPost, peer+"/queries", map[string]any{"query": "SELECT * FROM shop.people"}), 404, "Not found: Table")
	key := backend.Key{Service: serviceName, Scope: state.Scope{Project: testProject, Location: state.Global}}
	if err := h.manager.Reset(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	want(t, "reset dataset", h.invoke(http.MethodGet, testDatasets+"/shop", nil), 404, "shop")
	h.expect(http.MethodPost, testDatasets, map[string]any{"datasetReference": map[string]any{"datasetId": "shop"}}, 200)
	h.expect(http.MethodPost, testDatasets+"/shop/tables", map[string]any{
		"tableReference": map[string]any{"tableId": "people"}, "schema": map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INTEGER"}}},
	}, 200)
	h.rowsAre("reset table is empty", h.queryAs(testToken, "SELECT COUNT(*) FROM shop.people", nil), "0")
	want(t, "peer kept its dataset", h.invoke(http.MethodGet, peer+"/datasets/shop", nil), 200, fmt.Sprintf(`"projectId":%q`, testPeerProject))
}

func TestJobsCarryResults(t *testing.T) {
	h := newHarness(t)
	h.people()
	job := jobBody("dml-1", "UPDATE shop.people SET active = true WHERE id = 2", nil)
	want(t, "dml job", h.expect(http.MethodPost, testJobs, job, 200), 200, `"numDmlAffectedRows":"1"`)
	want(t, "dml results", h.invoke(http.MethodGet, testQueries+"/dml-1", nil), 200, `"numDmlAffectedRows":"1"`)
	params := jobBody("param-1", "SELECT name FROM shop.people WHERE id = @id", func(_, _, query map[string]any) {
		query["parameterMode"] = "NAMED"
		query["queryParameters"] = []any{map[string]any{"name": "id", "parameterType": map[string]any{"type": "INT64"}, "parameterValue": map[string]any{"value": "3"}}}
	})
	h.expect(http.MethodPost, testJobs, params, 200)
	h.rowsAre("parameterized job", h.invoke(http.MethodGet, testQueries+"/param-1", nil), "linus")
	dry := jobBody("dry-2", "SELECT name, id FROM shop.people", func(_, configuration, _ map[string]any) { configuration["dryRun"] = true })
	want(t, "dry run schema", h.expect(http.MethodPost, testJobs, dry, 200), 200, `"schema":{"fields":[{"mode":"NULLABLE","name":"name","type":"STRING"}`)
	h.rowsAre("dry run changed nothing", h.queryAs(testToken, "SELECT COUNT(*) FROM shop.people WHERE active", nil), "3")
}

// TestFloatCells renders FLOAT64 cells as the REST surface does.
func TestFloatCells(t *testing.T) {
	for value, text := range map[float64]string{
		15: "15.0", 7.25: "7.25", 0: "0.0", -2: "-2.0", 1e7: "1.0E7", 1.5e10: "1.5E10", 0.0001: "1.0E-4", 1234567.5: "1234567.5",
	} {
		if got := floatCell(value); got != text {
			t.Errorf("floatCell(%v) = %s, want %s", value, got, text)
		}
	}
}

// generatedJobID is the shape of the ids newJobID assigns.
var generatedJobID = regexp.MustCompile(`^job_[A-Za-z0-9_-]{28}$`)

// TestJobIDsAreUnique runs jobs.query and id-less jobs.insert calls in one
// clock tick: each job gets its own id, so none replaces another's results.
func TestJobIDsAreUnique(t *testing.T) {
	h := newHarness(t)
	h.people()
	tick := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	h.bq.clock = func() time.Time { return tick }
	idOf := func(recorder *httptest.ResponseRecorder) string {
		var job struct {
			JobReference struct {
				JobID string `json:"jobId"`
			} `json:"jobReference"`
		}
		decode(t, recorder, &job)
		return job.JobReference.JobID
	}
	results := map[string]string{}
	for _, name := range []string{"ada", "grace"} {
		queried := idOf(h.queryAs(testToken, "SELECT name FROM shop.people WHERE name = '"+name+"'", nil))
		inserted := idOf(h.expect(http.MethodPost, testJobs, jobBody("", "SELECT '"+name+"-job'", nil), 200))
		for id, want := range map[string]string{queried: name, inserted: name + "-job"} {
			if !generatedJobID.MatchString(id) || results[id] != "" {
				t.Fatalf("job id %q is malformed or reused: %v", id, results)
			}
			results[id] = want
		}
	}
	for id, name := range results {
		h.rowsAre("results of "+id, h.invoke(http.MethodGet, testQueries+"/"+id, nil), name)
	}
}
