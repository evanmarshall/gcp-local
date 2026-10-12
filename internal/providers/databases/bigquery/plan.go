package bigquery

import (
	"database/sql"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tableName is one table of the job's project, as stored.
type tableName struct{ dataset, table string }

// catalog is the planner's copy of the scope's datasets and tables. The
// engine folds name case, so lookups do too; datasets or tables whose ids
// differ only in case are refused at creation.
type catalog struct {
	project  string
	datasets map[string]string       // lowercased id -> id
	tables   map[string]*tableRecord // lowercased dataset/table -> record
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
}

type rewrite struct {
	start, end int
	text       string
}

// planQuery parses a script, resolves its tables, and lists its checks.
// Nothing runs and nothing is authorized here.
func planQuery(c *catalog, script, defaultDataset string, mode string, parameters []queryParameter) (*queryPlan, error) {
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
	p := &planner{catalog: c, defaultDataset: defaultDataset, rangeVars: refs.rangeVars, ctes: refs.ctes, plan: &queryPlan{}, seen: map[tableCheck]bool{}}
	for _, statement := range refs.statements {
		if err := p.statement(script, statement); err != nil {
			return nil, err
		}
	}
	if err := p.bind(refs.parameters, mode, parameters); err != nil {
		return nil, err
	}
	p.plan.script = applyRewrites(script, p.rewrites)
	p.plan.statementType = statementType(refs.statements)
	if only := refs.statements; len(only) == 1 && only[0].target != nil && only[0].kind != nodeDrop {
		p.plan.target = &p.lastTarget
	}
	return p.plan, nil
}

// statement resolves one statement's target and reads.
func (p *planner) statement(script string, statement statementRefs) error {
	var target tableName
	var exists bool
	if statement.target != nil {
		var err error
		if target, err = p.resolve(*statement.target); err != nil {
			return err
		}
		p.lastTarget = target
		exists = p.catalog.tables[foldKey(target.dataset, target.table)] != nil
	}
	var reads []tableName
	for _, path := range statement.reads {
		names, err := p.read(path)
		if err != nil {
			return err
		}
		reads = append(reads, names...)
	}
	switch statement.kind {
	case nodeInsert, nodeTruncate:
		p.check(checkQueryWrite, target, exists)
	case nodeUpdate, nodeDelete, nodeMerge:
		p.check(checkQueryWrite, target, exists)
		if err := p.readTable(target, 0); err != nil {
			return err
		}
	case nodeCreateTable, nodeCreateView:
		return p.create(script, statement, target, exists, reads)
	case nodeDrop:
		p.check(checkQueryDrop, target, exists)
		p.plan.ddl = append(p.plan.ddl, ddlTarget{name: target})
	}
	return nil
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
	entry := ddlTarget{name: target, create: true, replace: strings.Contains(statement.flags, "is_or_replace"), view: view}
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
			if err := p.readTable(name, 0); err != nil {
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
	return []tableName{name}, p.readTable(name, 0)
}

// maxViewDepth bounds view expansion.
const maxViewDepth = 16

// readTable checks bigquery.tables.getData on a table, and on every table a
// view reads: querying a view needs access to its underlying tables (views
// guide). Authorized views are not modeled.
func (p *planner) readTable(name tableName, depth int) error {
	if depth > maxViewDepth {
		return gap("views nested deeper than 16 levels")
	}
	record := p.catalog.tables[foldKey(name.dataset, name.table)]
	p.check(checkQueryTable, name, record != nil)
	if record == nil {
		return nil
	}
	for _, ref := range record.ViewRefs {
		if err := p.readTable(ref, depth+1); err != nil {
			return err
		}
	}
	return nil
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
		script = script[:r.start] + r.text + script[r.end:]
	}
	return script
}

// statementTypes name a single statement the way query statistics do.
var statementTypes = map[string]string{
	nodeQuery: "SELECT", nodeInsert: statementInsert, nodeUpdate: statementUpdate, nodeDelete: statementDelete, nodeMerge: statementMerge,
	nodeTruncate: "TRUNCATE_TABLE", nodeCreateTable: "CREATE_TABLE", nodeCreateView: "CREATE_VIEW", nodeDrop: "DROP_TABLE",
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
		return "CREATE_TABLE_AS_SELECT"
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
