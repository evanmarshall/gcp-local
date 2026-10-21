package bigquery

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tableName is one table of the job's project, as stored.
type tableName struct{ dataset, table string }

// catalog is the planner's copy of the scope's datasets and tables. The
// engine folds name case, so lookups do too; datasets or tables whose ids
// differ only in case are refused at creation. access holds each dataset's
// access list, for authorized views.
type catalog struct {
	project  string
	datasets map[string]string       // lowercased id -> id
	tables   map[string]*tableRecord // lowercased dataset/table -> record
	access   map[string]accessList   // lowercased id -> access list
}

func foldKey(dataset, table string) string {
	return strings.ToLower(dataset) + "/" + strings.ToLower(table)
}

// storageKey is the engine's physical table name: the engine joins path
// parts with underscores, so ds.t_x and ds_t.x would share storage.
func storageKey(dataset, table string) string { return strings.ToLower(dataset + "_" + table) }

// tableCheck is one IAM check a query needs before it runs.
type tableCheck struct{ method, resource string }

// ddlTarget is a table a script may create or drop; the provider reconciles
// its metadata with the engine after the run.
type ddlTarget struct {
	name      tableName
	create    bool
	replace   bool
	view      bool
	viewQuery string
	viewRefs  []tableName
	// existed reports the table existed when the script was planned.
	existed bool
	// expirationSpan is the expiration_timestamp option's expression;
	// expiration is its rewritten text, or "", and expires its value,
	// evaluated before the script runs.
	expirationSpan *[2]int
	expiration     string
	expires        time.Time
}

// operation names what a single DDL statement did, as GCP's
// ddlOperationPerformed does: CREATE and REPLACE (recorded:
// bigquery-dbt-requests); SKIP and DROP follow the JobStatistics2 reference.
func (t ddlTarget) operation() string {
	switch {
	case !t.create && t.existed:
		return "DROP"
	case t.create && !t.existed:
		return ddlCreate
	case t.create && t.replace:
		return "REPLACE"
	default:
		return ddlSkip
	}
}

// ddlOperationPerformed values a CREATE SCHEMA reports (recorded:
// bigquery-dbt-requests).
const (
	ddlCreate = "CREATE"
	ddlSkip   = "SKIP"
)

// schemaTarget is the dataset a CREATE SCHEMA statement creates.
type schemaTarget struct {
	dataset     string
	ifNotExists bool
}

// mergePlan runs a MERGE whose source is a subquery, which the engine does
// not merge from: stage creates a table from the subquery, script merges
// from it, and the staged table is dropped after.
type mergePlan struct {
	staged tableName
	stage  string
	script string
}

// queryPlan is a script ready to run: every table path rewritten to its
// canonical engine name, its arguments typed, and the IAM checks it needs.
type queryPlan struct {
	script        string
	args          []any
	checks        []tableCheck
	ddl           []ddlTarget
	statementType string
	// target is the table a single DML or CREATE statement writes; its
	// schema is the response schema (recorded: bigquery-sql-engine).
	target *tableName
	// tables are the existing tables the script reads or writes, whose
	// expired partitions go before it runs.
	tables []tableName
	// schema is a CREATE SCHEMA statement's dataset, or nil; such a script
	// has only that statement and never reaches the engine.
	schema *schemaTarget
	// merge stages a MERGE's subquery source, or is nil.
	merge *mergePlan
}

// queryParameter is one jobs.query or jobs.insert query parameter.
type queryParameter struct {
	Name           string          `json:"name"`
	ParameterType  *parameterType  `json:"parameterType"`
	ParameterValue *parameterValue `json:"parameterValue"`
}

type parameterType struct {
	Type      string         `json:"type"`
	ArrayType *parameterType `json:"arrayType"`
	// StructTypes and RangeElementType are refused as coverage gaps.
	StructTypes      []any          `json:"structTypes"`
	RangeElementType *parameterType `json:"rangeElementType"`
}

type parameterValue struct {
	Value        *string                   `json:"value"`
	ArrayValues  []parameterValue          `json:"arrayValues"`
	StructValues map[string]parameterValue `json:"structValues"`
	RangeValue   any                       `json:"rangeValue"`
}

// boundParameter is one parameter's engine value and the expression that
// gives it its declared type: the engine types only Go primitives, so other
// scalar types bind as text inside a cast.
type boundParameter struct {
	value any
	wrap  string // "" binds natively; otherwise a format with %s for the parameter
}

// planner resolves one script against a catalog.
type planner struct {
	catalog        *catalog
	defaultDataset string
	rangeVars      map[string]bool
	ctes           map[string]bool
	lastTarget     tableName
	rewrites       []rewrite
	plan           *queryPlan
	seen           map[tableCheck]bool
	// resolved maps a table path's start to the table it names; missing
	// reports a path naming no table, which the engine refuses as 404.
	resolved map[int]*tableRecord
	missing  bool
}

type rewrite struct {
	start, end int
	text       string
}

// planQuery parses a script, resolves its tables, and lists its checks.
// Nothing runs and nothing is authorized here.
func planQuery(c *catalog, script, defaultDataset string, mode string, parameters []queryParameter, now time.Time) (*queryPlan, error) {
	root, err := parseTree(script)
	if err != nil {
		return nil, err
	}
	refs, err := scanScript(root)
	if err != nil {
		return nil, err
	}
	if len(refs.statements) == 0 {
		return nil, invalidQuery("Syntax error: Unexpected end of script")
	}
	p := &planner{catalog: c, defaultDataset: defaultDataset, rangeVars: refs.rangeVars, ctes: refs.ctes, plan: &queryPlan{},
		seen: map[tableCheck]bool{}, resolved: map[int]*tableRecord{}}
	if err := p.resolveScript(script, refs, mode, parameters); err != nil {
		return nil, err
	}
	return p.plan, p.finish(script, refs, now)
}

// finish pins the clock, reads the expiration options, stages a MERGE's
// subquery, and writes the script the engine runs.
func (p *planner) finish(script string, refs scriptRefs, now time.Time) error {
	p.pinClock(script, refs.clock, now)
	for index, target := range p.plan.ddl {
		if span := target.expirationSpan; span != nil {
			p.plan.ddl[index].expiration = applyRewrites(script[span[0]:span[1]], within(p.rewrites, *span))
		}
	}
	if err := p.stageMerge(script, refs.statements); err != nil {
		return err
	}
	p.plan.script = applyRewrites(script, slices.Clone(p.rewrites))
	p.plan.statementType = statementType(refs.statements)
	if only := refs.statements; len(only) == 1 && only[0].target != nil && only[0].kind != nodeDrop && only[0].kind != nodeSchema {
		p.plan.target = &p.lastTarget
	}
	return nil
}

// stageMerge plans a MERGE whose USING clause is a subquery: the engine
// merges only from a table, so the subquery is staged in a table of the
// target's dataset that no metadata names, and the MERGE reads that table
// under the subquery's alias. The subquery's reads are checked like any
// other. Only a script of that one statement is served.
func (p *planner) stageMerge(script string, statements []statementRefs) error {
	var merge *mergeSubquery
	for _, statement := range statements {
		if statement.merge != nil {
			merge = statement.merge
		}
	}
	if merge == nil {
		return nil
	}
	if len(statements) != 1 {
		return gap("MERGE from a subquery inside a multi-statement script")
	}
	raw := make([]byte, 12)
	_, _ = rand.Read(raw) // crypto/rand.Read never returns an error.
	staged := tableName{dataset: p.lastTarget.dataset, table: "_gcp_local_merge_" + hex.EncodeToString(raw)}
	path := canonicalPath(p.catalog.project, staged.dataset, staged.table)
	query := applyRewrites(script[merge.query[0]:merge.query[1]], within(p.rewrites, merge.query))
	var outside []rewrite
	for _, r := range p.rewrites {
		if r.end <= merge.source[0] || r.start >= merge.source[1] {
			outside = append(outside, r)
		}
	}
	outside = append(outside, rewrite{start: merge.source[0], end: merge.source[1], text: path + " "})
	p.plan.merge = &mergePlan{staged: staged, stage: "CREATE TABLE " + path + " AS " + query, script: applyRewrites(script, outside)}
	return nil
}

// pinClock rewrites every current-time call to the job's start, so each
// call in a statement reads the same instant, as GoogleSQL evaluates them
// once per statement ("Function calls", current date and time functions).
// A script's statements share the job's instant (not recorded).
func (p *planner) pinClock(script string, calls []clockRef, now time.Time) {
	instant := "TIMESTAMP_MICROS(" + strconv.FormatInt(now.UnixMicro(), 10) + ")"
	for _, call := range calls {
		text := instant
		if call.name != clockTimestamp {
			zone := ""
			if call.zone != nil {
				zone = ", " + script[call.zone[0]:call.zone[1]]
			}
			text = strings.ToUpper(strings.TrimPrefix(call.name, "current_")) + "(" + instant + zone + ")"
		}
		p.rewrites = append(p.rewrites, rewrite{start: call.start, end: call.end, text: text})
	}
}

// within returns the rewrites inside span, shifted to start at its start.
func within(rewrites []rewrite, span [2]int) []rewrite {
	var inner []rewrite
	for _, r := range rewrites {
		if r.start >= span[0] && r.end <= span[1] {
			inner = append(inner, rewrite{start: r.start - span[0], end: r.end - span[0], text: r.text})
		}
	}
	return inner
}

// resolveScript resolves every statement, then the pseudo-columns and the
// parameters.
func (p *planner) resolveScript(script string, refs scriptRefs, mode string, parameters []queryParameter) error {
	for _, statement := range refs.statements {
		if statement.kind == nodeSchema {
			if err := p.schema(statement, len(refs.statements)); err != nil {
				return err
			}
			continue
		}
		if err := p.statement(script, statement); err != nil {
			return err
		}
	}
	if err := p.pseudoColumns(script, refs); err != nil {
		return err
	}
	return p.bind(refs.parameters, mode, parameters)
}

// statement resolves one statement's target and reads.
func (p *planner) statement(script string, statement statementRefs) error {
	target, record, err := p.target(statement)
	if err != nil {
		return err
	}
	var reads []tableName
	for _, path := range statement.reads {
		names, err := p.read(path)
		if err != nil {
			return err
		}
		reads = append(reads, names...)
	}
	exists := record != nil
	switch statement.kind {
	case nodeInsert:
		p.check(checkQueryWrite, target, exists)
		p.insertColumns(statement, record)
	case nodeTruncate:
		p.check(checkQueryWrite, target, exists)
	case nodeUpdate, nodeDelete, nodeMerge:
		p.check(checkQueryWrite, target, exists)
		if err := requireFilter(record, statement.targetFilter); err != nil {
			return err
		}
		return p.readTable(target, 0, nil)
	case nodeCreateTable, nodeCreateView:
		return p.create(script, statement, target, exists, reads)
	case nodeDrop:
		p.check(checkQueryDrop, target, exists)
		p.plan.ddl = append(p.plan.ddl, ddlTarget{name: target, existed: exists})
	}
	return nil
}

// expiration takes a CREATE statement's expiration_timestamp option out of
// the script the engine runs and returns its expression's span, or nil: the
// expression is evaluated on its own.
func (p *planner) expiration(statement statementRefs) *[2]int {
	option := statement.expiration
	if option == nil {
		return nil
	}
	p.rewrites = append(p.rewrites, rewrite{start: option.entry[0], end: option.entry[1]})
	return &option.value
}

// schema plans CREATE SCHEMA [IF NOT EXISTS] [project.]dataset: it needs
// bigquery.datasets.create on the project (DDL reference, "Required
// permissions"), and must be the script's only statement.
func (p *planner) schema(statement statementRefs, count int) error {
	if count != 1 {
		return gap("CREATE SCHEMA inside a multi-statement script")
	}
	parts := statement.target.parts
	if len(parts) == 2 && !strings.EqualFold(parts[0], p.catalog.project) {
		return gap("cross-project CREATE SCHEMA (" + strings.Join(parts, ".") + ")")
	}
	dataset := parts[len(parts)-1]
	if len(parts) > 2 || !validDatasetID(dataset) {
		return invalidQuery("Invalid dataset ID: " + strings.Join(parts, "."))
	}
	p.plan.schema = &schemaTarget{dataset: dataset, ifNotExists: strings.Contains(statement.flags, "is_if_not_exists")}
	entry := tableCheck{method: checkQuerySchema, resource: provider.ProjectResource(p.catalog.project)}
	p.seen[entry] = true
	p.plan.checks = append(p.plan.checks, entry)
	return nil
}

// target resolves a statement's target table and returns its record, or nil
// when it doesn't exist.
func (p *planner) target(statement statementRefs) (tableName, *tableRecord, error) {
	if statement.target == nil {
		return tableName{}, nil, nil
	}
	target, err := p.resolve(*statement.target)
	if err != nil {
		return target, nil, err
	}
	p.lastTarget = target
	record := p.catalog.tables[foldKey(target.dataset, target.table)]
	if record != nil {
		p.resolved[statement.target.start] = record
		p.plan.tables = append(p.plan.tables, target)
	}
	return target, record, nil
}

// insertColumns names the columns of an INSERT into an ingestion-time table
// that names none, so the hidden pseudo-column takes its default.
func (p *planner) insertColumns(statement statementRefs, record *tableRecord) {
	if record != nil && record.ingestionTime() && !statement.columnList {
		end := statement.target.end
		p.rewrites = append(p.rewrites, rewrite{start: end, end: end, text: " (" + columnList(record.Schema) + ")"})
	}
}

// create checks a CREATE TABLE or CREATE VIEW target: bigquery.tables.create
// on the dataset, and with OR REPLACE over an existing table also
// bigquery.tables.update, plus bigquery.tables.updateData for a table (DDL
// reference, "Required permissions").
func (p *planner) create(script string, statement statementRefs, target tableName, exists bool, reads []tableName) error {
	if err := p.catalog.collision(target); err != nil {
		return err
	}
	for _, earlier := range p.plan.ddl {
		if storageKey(earlier.name.dataset, earlier.name.table) == storageKey(target.dataset, target.table) && earlier.name != target {
			return collisionGap(earlier.name, target)
		}
	}
	p.check(checkQueryCreate, tableName{dataset: target.dataset}, false)
	view := statement.kind == nodeCreateView
	if exists && strings.Contains(statement.flags, "is_or_replace") {
		p.check(checkQueryUpdate, target, true)
		if !view {
			p.check(checkQueryWrite, target, true)
		}
	}
	entry := ddlTarget{name: target, create: true, replace: strings.Contains(statement.flags, "is_or_replace"), view: view, existed: exists}
	entry.expirationSpan = p.expiration(statement)
	if view && statement.query != nil {
		entry.viewQuery = script[statement.query[0]:statement.query[1]]
		entry.viewRefs = reads
	}
	p.plan.ddl = append(p.plan.ddl, entry)
	return nil
}

// resolve names the table a path means in GCP: dataset.table, the job's
// project.dataset.table, or a bare table in the default dataset. The path
// is rewritten to that table's canonical engine name.
func (p *planner) resolve(path tablePath) (tableName, error) {
	parts := path.parts
	var dataset, table string
	switch len(parts) {
	case 1:
		if p.defaultDataset == "" {
			return tableName{}, invalidQuery(`Table "` + parts[0] + `" must be qualified with a dataset (e.g. dataset.table).`)
		}
		dataset, table = p.defaultDataset, parts[0]
	case 2:
		dataset, table = parts[0], parts[1]
	case 3:
		if !strings.EqualFold(parts[0], p.catalog.project) {
			return tableName{}, gap("cross-project table references (" + strings.Join(parts, ".") + ")")
		}
		dataset, table = parts[1], parts[2]
	default:
		return tableName{}, invalidQuery("Invalid table name: " + strings.Join(parts, "."))
	}
	stored, ok := p.catalog.datasets[strings.ToLower(dataset)]
	if !ok {
		return tableName{}, status.Errorf(codes.NotFound, "Not found: Dataset %s:%s", p.catalog.project, dataset)
	}
	if !validTableID(table) {
		return tableName{}, invalidQuery("Invalid table name: " + table)
	}
	name := tableName{dataset: stored, table: table}
	if record := p.catalog.tables[foldKey(stored, table)]; record != nil {
		name.table = record.ID
	}
	p.rewrites = append(p.rewrites, rewrite{start: path.start, end: path.end,
		text: canonicalPath(p.catalog.project, name.dataset, name.table)})
	return name, nil
}

// read resolves one table read. A one-part path naming a CTE, or a longer
// path headed by an alias, CTE name, or loop variable (possibly a correlated
// column path), is not rewritten; instead every table the engine could
// resolve it to must be readable.
func (p *planner) read(path tablePath) ([]tableName, error) {
	head := strings.ToLower(path.parts[0])
	if (len(path.parts) == 1 && p.ctes[head]) || (len(path.parts) > 1 && p.rangeVars[head]) {
		var names []tableName
		for _, record := range p.catalog.looseMatches(path.parts) {
			name := tableName{dataset: record.Dataset, table: record.ID}
			if err := p.readTable(name, 0, nil); err != nil {
				return nil, err
			}
			names = append(names, name)
		}
		return names, nil
	}
	name, err := p.resolve(path)
	if err != nil {
		return nil, err
	}
	if err := p.noteRead(path, name); err != nil {
		return nil, err
	}
	return []tableName{name}, p.readTable(name, 0, nil)
}

// noteRead records the table a path reads, or that it reads none, and
// checks its partition filter.
func (p *planner) noteRead(path tablePath, name tableName) error {
	record := p.catalog.tables[foldKey(name.dataset, name.table)]
	if record == nil {
		p.missing = true
		return nil
	}
	p.resolved[path.start] = record
	p.plan.tables = append(p.plan.tables, name)
	return requireFilter(record, path.filter)
}

// maxViewDepth bounds view expansion.
const maxViewDepth = 16

// readTable checks bigquery.tables.getData on a table, and on every table a
// view reads: querying a view needs access to its underlying tables (views
// guide), unless the table's dataset authorizes the view (via), by name or
// through the view's dataset (authorized views and datasets; recorded:
// bigquery-dataset-access). A view authorized that way reads under its own
// authorizations in turn.
func (p *planner) readTable(name tableName, depth int, via *tableName) error {
	if depth > maxViewDepth {
		return gap("views nested deeper than 16 levels")
	}
	record := p.catalog.tables[foldKey(name.dataset, name.table)]
	if via == nil || !p.catalog.access[strings.ToLower(name.dataset)].authorizes(*via) {
		p.check(checkQueryTable, name, record != nil)
	}
	if record == nil {
		return nil
	}
	for _, ref := range record.ViewRefs {
		if err := p.readTable(ref, depth+1, &name); err != nil {
			return err
		}
	}
	return nil
}

// requireFilter refuses a read of a partitioned table with
// requirePartitionFilter whose filter doesn't name its partitioning column
// (or, for an ingestion-time table, a pseudo-column), as GCP does
// (recorded). GCP also wants the filter usable for partition elimination;
// any mention of the column passes here. Filters a view or CTE applies
// outside the SELECT that reads the table don't count.
func requireFilter(record *tableRecord, filter []*astNode) error {
	if record == nil || record.RequirePartitionFilter == nil || !*record.RequirePartitionFilter ||
		(record.TimePartitioning == nil && record.RangePartitioning == nil) {
		return nil
	}
	if mentions(filter, record.requiredFilterColumns()) {
		return nil
	}
	return filterRequired(record)
}

// mentions reports whether a subtree names one of columns as a path's last
// part.
func mentions(nodes []*astNode, columns []string) bool {
	for _, node := range nodes {
		if node.kind == nodePath && len(node.children) > 0 {
			name, err := identifierText(node.children[len(node.children)-1].payload)
			if err == nil && slices.ContainsFunc(columns, func(column string) bool { return strings.EqualFold(column, name) }) {
				return true
			}
		}
		if mentions(node.children, columns) {
			return true
		}
	}
	return false
}

// pseudoColumns rewrites the script's _PARTITIONTIME and _PARTITIONDATE
// references to the hidden column of the ingestion-time tables it reads, and
// keeps the stars over those tables from expanding to it, as GCP excludes
// pseudo-columns from SELECT * (recorded). A reference with no
// ingestion-time table to answer it is GCP's unrecognized name (recorded);
// _PARTITIONDATE needs a daily one.
func (p *planner) pseudoColumns(script string, refs scriptRefs) error {
	found, daily := p.ingestionTables()
	if !found && p.missing {
		// The engine refuses the missing table first, as GCP does.
		return nil
	}
	for _, ref := range refs.pseudo {
		if !found || (ref.name == namePartitionDate && !daily) {
			return invalidQuery("Unrecognized name: " + ref.name + " at " + position(script, ref.last))
		}
		text := script[ref.start:ref.last] + pseudoColumn
		if ref.name == namePartitionDate {
			text = "DATE(" + text + ")"
			if ref.named {
				// Quoted, so the engine's own rewrite leaves the alias alone.
				text += " AS `" + namePartitionDate + "`"
			}
		}
		p.rewrites = append(p.rewrites, rewrite{start: ref.start, end: ref.end, text: text})
	}
	for _, query := range refs.selects {
		p.excludePseudo(query)
	}
	return nil
}

// ingestionTables reports whether the script names an ingestion-time table,
// and a daily one.
func (p *planner) ingestionTables() (bool, bool) {
	found, daily := false, false
	for _, record := range p.resolved {
		if record.ingestionTime() {
			found, daily = true, daily || record.TimePartitioning.Type == partitionDay
		}
	}
	return found, daily
}

// excludePseudo adds the pseudo-column to the EXCEPT list of each star of a
// SELECT that reads an ingestion-time table: every * and that table's
// alias.*.
func (p *planner) excludePseudo(query *selectRefs) {
	aliases := map[string]bool{}
	for _, table := range query.tables {
		if record := p.resolved[table.start]; record != nil && record.ingestionTime() {
			aliases[table.alias] = true
		}
	}
	if len(aliases) == 0 {
		return
	}
	for _, star := range query.stars {
		switch {
		case star.alias != "" && !aliases[star.alias]:
		case star.except >= 0:
			p.rewrites = append(p.rewrites, rewrite{start: star.except, end: star.except, text: ", " + pseudoColumn})
		default:
			p.rewrites = append(p.rewrites, rewrite{start: star.insert, end: star.insert, text: " EXCEPT (" + pseudoColumn + ")"})
		}
	}
}

// position spells a byte offset as GoogleSQL's [line:column].
func position(script string, offset int) string {
	line := strings.Count(script[:offset], "\n") + 1
	column := offset - strings.LastIndex(script[:offset], "\n")
	return "[" + strconv.Itoa(line) + ":" + strconv.Itoa(column) + "]"
}

// check adds one IAM check: on the table when it exists, otherwise on its
// dataset, whose policy a new table would inherit.
func (p *planner) check(method string, name tableName, exists bool) {
	resource := datasetResource(p.catalog.project, name.dataset)
	if exists {
		resource = tableResource(p.catalog.project, name.dataset, name.table)
	}
	entry := tableCheck{method: method, resource: resource}
	if !p.seen[entry] {
		p.seen[entry] = true
		p.plan.checks = append(p.plan.checks, entry)
	}
}

// looseMatches lists the tables the engine resolves a path to: table
// p.D.T answers to T, p.T, D.T, and p.D.T, case-insensitively.
func (c *catalog) looseMatches(parts []string) []*tableRecord {
	path := strings.ToLower(strings.Join(parts, "."))
	project := strings.ToLower(c.project)
	var matches []*tableRecord
	for _, record := range c.tables {
		dataset, table := strings.ToLower(record.Dataset), strings.ToLower(record.ID)
		forms := []string{table, project + "." + table, dataset + "." + table, project + "." + dataset + "." + table}
		if slices.Contains(forms, path) {
			matches = append(matches, record)
		}
	}
	slices.SortFunc(matches, func(a, b *tableRecord) int {
		return strings.Compare(foldKey(a.Dataset, a.ID), foldKey(b.Dataset, b.ID))
	})
	return matches
}

// collision refuses a table whose engine storage another table already
// uses; GCP keeps them apart, the engine would not.
func (c *catalog) collision(name tableName) error {
	for _, record := range c.tables {
		if storageKey(record.Dataset, record.ID) == storageKey(name.dataset, name.table) &&
			foldKey(record.Dataset, record.ID) != foldKey(name.dataset, name.table) {
			return collisionGap(tableName{record.Dataset, record.ID}, name)
		}
	}
	return nil
}

func collisionGap(existing, created tableName) error {
	return gap("tables " + existing.dataset + "." + existing.table + " and " + created.dataset + "." + created.table +
		" share one storage name in the local engine")
}

// applyRewrites replaces spans from the end so earlier offsets stay valid.
func applyRewrites(script string, rewrites []rewrite) string {
	slices.SortFunc(rewrites, func(a, b rewrite) int { return b.start - a.start })
	for _, r := range rewrites {
		if !nested(r, rewrites) {
			script = script[:r.start] + r.text + script[r.end:]
		}
	}
	return script
}

// nested reports whether a non-empty rewrite lies inside a longer one, which
// replaces its text anyway (a removed option entry, a staged subquery).
func nested(r rewrite, rewrites []rewrite) bool {
	for _, outer := range rewrites {
		if r.start < r.end && outer.start <= r.start && r.end <= outer.end && outer.end-outer.start > r.end-r.start {
			return true
		}
	}
	return false
}

// statementTypes name a single statement the way query statistics do.
var statementTypes = map[string]string{
	nodeQuery: statementSelect, nodeInsert: statementInsert, nodeUpdate: statementUpdate, nodeDelete: statementDelete, nodeMerge: statementMerge,
	nodeTruncate: "TRUNCATE_TABLE", nodeCreateTable: "CREATE_TABLE", nodeCreateView: "CREATE_VIEW", nodeDrop: "DROP_TABLE",
	nodeSchema: "CREATE_SCHEMA",
}

// statementType names the job's statement; a script of several statements,
// or of scripting statements, is SCRIPT.
func statementType(statements []statementRefs) string {
	if len(statements) != 1 {
		return statementScript
	}
	statement := statements[0]
	switch {
	case statement.kind == nodeCreateTable && statement.query != nil:
		return statementCTAS
	case statement.kind == nodeDrop && strings.HasPrefix(statement.flags, " VIEW"):
		return "DROP_VIEW"
	}
	if name, ok := statementTypes[statement.kind]; ok {
		return name
	}
	return statementScript
}

// bind types every parameter occurrence and orders the engine arguments. A
// parameter whose type the engine cannot bind natively is wrapped where it
// occurs, for example @d becomes CAST(@d AS DATE).
func (p *planner) bind(occurrences []parameterRef, mode string, parameters []queryParameter) error {
	named := map[string]boundParameter{}
	var positional []boundParameter
	for _, parameter := range parameters {
		bound, err := bindParameter(parameter.ParameterType, parameter.ParameterValue)
		if err != nil {
			return err
		}
		switch {
		case mode == modePositional:
			positional = append(positional, bound)
			p.plan.args = append(p.plan.args, bound.value)
		case parameter.Name == "":
			return invalidQuery("A named query parameter requires a name")
		default:
			named[strings.ToLower(parameter.Name)] = bound
			p.plan.args = append(p.plan.args, sql.Named(parameter.Name, bound.value))
		}
	}
	for _, occurrence := range occurrences {
		if err := p.wrapParameter(occurrence, mode, named, positional); err != nil {
			return err
		}
	}
	return nil
}

// wrapParameter types one parameter occurrence.
func (p *planner) wrapParameter(occurrence parameterRef, mode string, named map[string]boundParameter, positional []boundParameter) error {
	var bound boundParameter
	reference := "?"
	if occurrence.name != "" {
		var ok bool
		if bound, ok = named[occurrence.name]; !ok || mode == modePositional {
			return invalidQuery("Query parameter '" + occurrence.name + "' not found")
		}
		reference = "@`" + occurrence.name + "`"
	} else {
		if mode != modePositional || occurrence.position > len(positional) {
			return invalidQuery("Positional parameter " + strconv.Itoa(occurrence.position) + " has no value")
		}
		bound = positional[occurrence.position-1]
	}
	if bound.wrap != "" {
		p.rewrites = append(p.rewrites, rewrite{start: occurrence.start, end: occurrence.end,
			text: strings.ReplaceAll(bound.wrap, "%s", reference)})
	}
	return nil
}

// bindParameter converts one parameter value for its declared type. A NULL
// of any scalar type binds inside a cast, so it keeps its type.
func bindParameter(kind *parameterType, value *parameterValue) (boundParameter, error) {
	if kind == nil || kind.Type == "" {
		return boundParameter{}, invalidQuery("A query parameter requires a parameterType")
	}
	typeName := sqlTypeName(kind.Type)
	switch typeName {
	case sqlArray:
		return bindArray(kind.ArrayType, value)
	case sqlStruct, sqlRange:
		return boundParameter{}, gap(typeName + " query parameters")
	}
	return scalarParameter(typeName, value)
}

// scalarParameter binds one scalar parameter.
func scalarParameter(typeName string, value *parameterValue) (boundParameter, error) {
	wrap, typed := typingCalls[typeName]
	parse, native := nativeParsers[typeName]
	switch {
	case !typed && !native:
		return boundParameter{}, gap("query parameter type " + typeName)
	case value == nil || value.Value == nil:
		return boundParameter{wrap: "CAST(%s AS " + typeName + ")"}, nil
	case typed:
		return boundParameter{value: *value.Value, wrap: wrap}, nil
	}
	parsed, err := parse(*value.Value)
	if err != nil {
		return boundParameter{}, invalidQuery("Invalid " + typeName + " query parameter value " + strconv.Quote(*value.Value))
	}
	return boundParameter{value: parsed}, nil
}

// nativeParsers parse the scalars the engine binds as Go values.
var nativeParsers = map[string]func(string) (any, error){
	sqlInt64:   func(text string) (any, error) { return strconv.ParseInt(text, 10, 64) },
	sqlFloat64: func(text string) (any, error) { return strconv.ParseFloat(text, 64) },
	sqlBool:    func(text string) (any, error) { return strconv.ParseBool(text) },
	sqlString:  func(text string) (any, error) { return text, nil },
}

// bindArray binds an array of a natively typed scalar as a typed slice.
func bindArray(element *parameterType, value *parameterValue) (boundParameter, error) {
	if element == nil {
		return boundParameter{}, invalidQuery("An ARRAY query parameter requires an arrayType")
	}
	typeName := sqlTypeName(element.Type)
	var items []parameterValue
	if value != nil {
		items = value.ArrayValues
	}
	texts := make([]string, 0, len(items))
	for _, item := range items {
		if item.Value == nil {
			return boundParameter{}, invalidQuery("An ARRAY query parameter cannot hold NULL")
		}
		texts = append(texts, *item.Value)
	}
	var bound any
	var err error
	switch typeName {
	case sqlInt64:
		bound, err = parsedSlice[int64](typeName, texts)
	case sqlFloat64:
		bound, err = parsedSlice[float64](typeName, texts)
	case sqlBool:
		bound, err = parsedSlice[bool](typeName, texts)
	case sqlString:
		bound = texts
	default:
		return boundParameter{}, gap("ARRAY<" + typeName + "> query parameters")
	}
	return boundParameter{value: bound}, err
}

// parsedSlice parses array parameter values into a typed slice.
func parsedSlice[T any](typeName string, texts []string) ([]T, error) {
	out := make([]T, 0, len(texts))
	for _, text := range texts {
		parsed, err := nativeParsers[typeName](text)
		typed, ok := parsed.(T)
		if err != nil || !ok {
			return nil, invalidQuery("Invalid " + typeName + " query parameter value " + strconv.Quote(text))
		}
		out = append(out, typed)
	}
	return out, nil
}
