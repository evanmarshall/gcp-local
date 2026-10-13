package bigquery

import (
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	googlesql "github.com/goccy/go-googlesql"
)

// The parse step finds every table a script names before anything runs. It
// parses with GoogleSQL's own parser (the one the engine embeds) and reads
// the parser's complete AST dump, so quoting, comments, dashed project ids,
// and nesting follow the real grammar rather than a hand-written subset.

// astNode is one line of the parser's AST dump: a node kind, its printed
// payload (an identifier, flags, or a literal), its source span, and its
// children.
type astNode struct {
	kind     string
	payload  string
	start    int
	end      int
	children []*astNode
}

// astLine matches one dump line: two-space indentation, the node kind, an
// optional payload, and the byte span. Every line must match, so a dump this
// reader does not understand fails closed.
var astLine = regexp.MustCompile(`^((?:  )*)([A-Z][A-Za-z0-9_]*)(.*) \[(\d+)-(\d+)\]$`)

// languageOptions enables every GoogleSQL feature and statement kind, so the
// parse step accepts at least what the engine accepts; anything this parse
// refuses is refused before the engine sees it.
var parserOptions = sync.OnceValues(func() (*googlesql.ParserOptions, error) {
	if err := googlesql.Init(); err != nil {
		return nil, fmt.Errorf("initialize GoogleSQL: %w", err)
	}
	language, err := googlesql.NewLanguageOptions()
	if err != nil {
		return nil, fmt.Errorf("GoogleSQL language options: %w", err)
	}
	if err := language.EnableMaximumLanguageFeatures(); err != nil {
		return nil, fmt.Errorf("GoogleSQL language features: %w", err)
	}
	if err := language.SetSupportsAllStatementKinds(); err != nil {
		return nil, fmt.Errorf("GoogleSQL statement kinds: %w", err)
	}
	if err := language.EnableReservableKeyword("QUALIFY", true); err != nil {
		return nil, fmt.Errorf("GoogleSQL keywords: %w", err)
	}
	options, err := googlesql.NewParserOptions()
	if err != nil {
		return nil, fmt.Errorf("GoogleSQL parser options: %w", err)
	}
	if err := options.SetLanguageOptions(language); err != nil {
		return nil, fmt.Errorf("GoogleSQL parser language: %w", err)
	}
	return options, nil
})

// parseTree parses a script and returns its AST. The dump prints literal
// text raw, so a newline inside a triple-quoted string could otherwise forge
// a node line: the parse runs on a copy with every newline turned into a
// carriage return, which GoogleSQL also treats as a line end (comments end
// at either), keeping byte offsets and tokens identical.
func parseTree(sql string) (*astNode, error) {
	defer collectModuleGarbage()
	options, err := parserOptions()
	if err != nil {
		return nil, err
	}
	output, err := googlesql.ParseScript(strings.ReplaceAll(sql, "\n", "\r"), options,
		&googlesql.ErrorMessageOptions{Mode: googlesql.ErrorMessageModeErrorMessageOneLine, Stability: googlesql.ErrorMessageStabilityProduction})
	if err != nil {
		return nil, invalidQuery(syntaxPosition(err.Error()))
	}
	script, err := output.Script()
	if err != nil {
		return nil, fmt.Errorf("read the parsed script: %w", err)
	}
	dump, err := script.DebugString(maxASTDepth)
	runtime.KeepAlive(output)
	if err != nil {
		return nil, fmt.Errorf("dump the parsed script: %w", err)
	}
	return readTree(dump)
}

// syntaxPosition spells the parser's "[at 1:1]" position as GCP's syntax
// errors do: "... at [1:1]" (recorded: bigquery-api-surface).
func syntaxPosition(message string) string {
	if text, position, ok := strings.Cut(message, " [at "); ok {
		return text + " at [" + position
	}
	return message
}

// maxASTDepth bounds the dump; a deeper script is refused rather than
// truncated.
const maxASTDepth = 512

// readTree rebuilds the node tree from the dump's indentation.
func readTree(dump string) (*astNode, error) {
	var stack []*astNode
	var root *astNode
	for line := range strings.SplitSeq(strings.TrimRight(dump, "\n"), "\n") {
		match := astLine.FindStringSubmatch(line)
		if match == nil {
			return nil, gap("a query construct the authorization parse cannot read")
		}
		depth := len(match[1]) / 2
		if depth > len(stack) || (depth == 0 && root != nil) || depth >= maxASTDepth-1 {
			return nil, gap("a query construct the authorization parse cannot read")
		}
		start, _ := strconv.Atoi(match[4])
		end, _ := strconv.Atoi(match[5])
		node := &astNode{kind: match[2], payload: match[3], start: start, end: end}
		stack = stack[:depth]
		if depth == 0 {
			root = node
		} else {
			parent := stack[depth-1]
			parent.children = append(parent.children, node)
		}
		stack = append(stack, node)
	}
	if root == nil || root.kind != "Script" {
		return nil, gap("a query construct the authorization parse cannot read")
	}
	return root, nil
}

// tablePath is one table reference as written: its unquoted name parts and
// the source span to rewrite. filter is the WHERE clause of the SELECT that
// reads it, or nil; requirePartitionFilter looks there.
type tablePath struct {
	parts      []string
	start, end int
	filter     []*astNode
}

// statementRefs is one statement of a script and the tables it names.
type statementRefs struct {
	kind   string
	flags  string // printed flags, for example "(is_or_replace)" or " TABLE(is_if_exists)"
	target *tablePath
	reads  []tablePath
	// query is the span of a CREATE statement's AS query, or nil.
	query *[2]int
	// targetFilter is the condition of an UPDATE, DELETE, or MERGE: every
	// child but the target and the SET list.
	targetFilter []*astNode
	// columnList reports an INSERT that names its columns.
	columnList bool
}

// selectRefs is one SELECT: the tables its FROM clause reads directly, by
// alias, and the stars of its select list. Stars over an ingestion-time
// table must not expand to its pseudo-column.
type selectRefs struct {
	tables []aliasedPath
	stars  []starRef
}

// aliasedPath is a table a SELECT reads and the lowercased name its columns
// answer to.
type aliasedPath struct {
	start int
	alias string
}

// starRef is one * or alias.* of a select list: alias is "" for *; insert is
// where an EXCEPT list starts; except, when the star has one, is where a name
// joins it (before its closing parenthesis), or -1.
type starRef struct {
	alias          string
	insert, except int
}

// pseudoRef is one reference to _PARTITIONTIME or _PARTITIONDATE: the span
// of its path, the offset of its last part, its name as written (upper
// case), and whether it is a whole select column without an alias, which
// keeps the pseudo-column's name.
type pseudoRef struct {
	start, end, last int
	name             string
	named            bool
}

// parameterRef is one query parameter occurrence: a lowercased name, or a
// 1-based position, and its source span.
type parameterRef struct {
	name       string
	position   int
	start, end int
}

// scriptRefs is a parsed script: its statements in order, every name that
// can head a correlated path (aliases, CTE names, loop variables), its
// SELECTs, and its pseudo-column references.
type scriptRefs struct {
	statements []statementRefs
	rangeVars  map[string]bool
	ctes       map[string]bool
	parameters []parameterRef
	selects    []*selectRefs
	pseudo     []pseudoRef
}

// Node kinds the parse step reads.
const (
	nodeQuery       = "QueryStatement"
	nodeInsert      = "InsertStatement"
	nodeUpdate      = "UpdateStatement"
	nodeDelete      = "DeleteStatement"
	nodeMerge       = "MergeStatement"
	nodeTruncate    = "TruncateStatement"
	nodeCreateTable = "CreateTableStatement"
	nodeCreateView  = "CreateViewStatement"
	nodeDrop        = "DropStatement"
	nodePath        = "PathExpression"
	nodeIdentifier  = "Identifier"
)

// statementMerge is the MERGE statement type; modePositional the positional
// parameter mode.
const (
	statementMerge  = "MERGE"
	statementDelete = "DELETE"
	statementInsert = "INSERT"
	statementUpdate = "UPDATE"
	statementScript = "SCRIPT"
	modePositional  = "POSITIONAL"
)

// Statement kinds the provider runs. A script may contain only these; any
// other statement (EXECUTE IMMEDIATE, CALL, CREATE FUNCTION, ALTER, EXPORT,
// transactions, system variables) is an explicit gap, because its table
// references cannot be resolved here or the engine does not run it.
var targetedStatements = map[string]bool{
	nodeInsert: true, nodeUpdate: true, nodeDelete: true, nodeMerge: true,
	nodeTruncate: true, nodeCreateTable: true, nodeCreateView: true, nodeDrop: true,
}

var scriptStatements = map[string]bool{
	nodeQuery: true, "VariableDeclaration": true, "SingleAssignment": true,
	"IfStatement": true, "WhileStatement": true, "RepeatStatement": true, "ForInStatement": true,
	"BeginEndBlock": true, "BreakStatement": true, "ContinueStatement": true, "ReturnStatement": true,
	"RaiseStatement": true,
}

// Node kinds that read a table by name inside a statement.
var tableReaders = map[string]bool{"TablePathExpression": true, "TableClause": true}

// Node kinds the parse refuses wherever they appear: they read data or
// metadata this provider cannot authorize table by table.
var refusedNodes = map[string]string{
	"CopyDataSource":   "CREATE TABLE ... COPY",
	"CloneDataSource":  "CREATE TABLE ... CLONE",
	"ModelClause":      "ML models",
	"ConnectionClause": "connections",
	"GraphTableQuery":  "graph queries",
}

// scanScript lists the statements of a parsed script and their tables.
func scanScript(root *astNode) (scriptRefs, error) {
	refs := scriptRefs{rangeVars: map[string]bool{}, ctes: map[string]bool{}}
	if err := collectRangeVars(root, refs.rangeVars, refs.ctes); err != nil {
		return refs, err
	}
	for _, list := range root.children {
		if err := refs.scanList(list); err != nil {
			return refs, err
		}
	}
	return refs, nil
}

// scanList reads one StatementList; every child must be an allowed statement.
func (refs *scriptRefs) scanList(list *astNode) error {
	if list.kind != "StatementList" {
		return gap("a query construct the authorization parse cannot read")
	}
	for _, statement := range list.children {
		if err := refs.scanStatement(statement); err != nil {
			return err
		}
	}
	return nil
}

// scanStatement records one statement, its target, and its reads.
func (refs *scriptRefs) scanStatement(statement *astNode) error {
	if !targetedStatements[statement.kind] && !scriptStatements[statement.kind] {
		return gap("statement " + statement.kind)
	}
	entry := statementRefs{kind: statement.kind, flags: statement.payload}
	children := statement.children
	if targetedStatements[statement.kind] {
		target, err := targetOf(statement)
		if err != nil {
			return err
		}
		entry.target = &target
		children = children[1:]
	}
	if err := validateStatement(statement, children); err != nil {
		return err
	}
	entry.query = createQuery(statement.kind, children)
	entry.targetFilter, entry.columnList = targetShape(statement.kind, children)
	position := len(refs.statements)
	refs.statements = append(refs.statements, entry)
	for _, child := range children {
		if err := refs.scanNode(child, scanContext{position: position}); err != nil {
			return err
		}
	}
	return nil
}

// targetShape reads the condition of an UPDATE, DELETE, or MERGE, and
// whether an INSERT names its columns.
func targetShape(kind string, children []*astNode) ([]*astNode, bool) {
	switch kind {
	case nodeUpdate, nodeDelete, nodeMerge:
		var filter []*astNode
		for _, child := range children {
			if child.kind != "UpdateItemList" {
				filter = append(filter, child)
			}
		}
		return filter, false
	case nodeInsert:
		return nil, len(children) > 0 && children[0].kind == nodeColumnList
	}
	return nil, false
}

// scanContext is where scanNode is: the statement's position, the WHERE
// clause of the nearest SELECT, and that SELECT.
type scanContext struct {
	position int
	filter   []*astNode
	query    *selectRefs
}

// createQuery is the span of a CREATE statement's AS query, or nil.
func createQuery(kind string, children []*astNode) *[2]int {
	if !strings.HasPrefix(kind, "Create") {
		return nil
	}
	for _, child := range children {
		if child.kind == "Query" {
			return &[2]int{child.start, child.end}
		}
	}
	return nil
}

// targetOf reads the table a DML or DDL statement names first.
func targetOf(statement *astNode) (tablePath, error) {
	if len(statement.children) == 0 || statement.children[0].kind != nodePath {
		return tablePath{}, gap("a " + statement.kind + " without a table name")
	}
	return pathOf(statement.children[0])
}

// validateStatement refuses statement forms the provider does not run.
func validateStatement(statement *astNode, rest []*astNode) error {
	switch statement.kind {
	case nodeCreateTable:
		if strings.Contains(statement.payload, "is_temp") {
			return gap("temporary tables")
		}
		return createTableClauses(rest)
	case nodeCreateView:
		if strings.Contains(statement.payload, "is_temp") {
			return gap("temporary views")
		}
		return layoutOptions(rest)
	case nodeDrop:
		if !strings.HasPrefix(statement.payload, " TABLE") && !strings.HasPrefix(statement.payload, " VIEW") {
			return gap("DROP" + strings.SplitN(statement.payload, "(", 2)[0])
		}
	}
	return nil
}

// createTableClauses refuses the CREATE TABLE clauses the provider doesn't
// run: LIKE, PARTITION BY, CLUSTER BY, and the layout options.
func createTableClauses(rest []*astNode) error {
	for _, child := range rest {
		switch child.kind {
		case nodePath:
			return gap("CREATE TABLE ... LIKE")
		case "PartitionBy", "ClusterBy":
			return gap("CREATE TABLE ... PARTITION BY and CLUSTER BY; create partitioned and clustered tables with tables.insert")
		}
	}
	return layoutOptions(rest)
}

// layoutOptions refuses the DDL options that set a table's partitioning or
// expiration, which DDL tables don't model; other options are ignored.
func layoutOptions(children []*astNode) error {
	for _, child := range children {
		if child.kind != "OptionsList" {
			continue
		}
		for _, option := range child.children {
			if len(option.children) == 0 || option.children[0].kind != nodeIdentifier {
				continue
			}
			name, err := identifierText(option.children[0].payload)
			if err != nil {
				return err
			}
			switch name = strings.ToLower(name); name {
			case "expiration_timestamp", "partition_expiration_days", "require_partition_filter":
				return gap("DDL option " + name + "; set it with tables.insert or tables.patch")
			}
		}
	}
	return nil
}

// Star node kinds of a select list, and an INSERT's column list.
const (
	nodeColumnList      = "ColumnList"
	nodeStar            = "Star"
	nodeStarModified    = "StarWithModifiers"
	nodeDotStar         = "DotStar"
	nodeDotStarModified = "DotStarWithModifiers"
)

// scanNode walks one subtree: table readers add reads to the statement at
// position, parameters and pseudo-column references are recorded, a SELECT
// records its tables and stars, and nested statement lists add their own
// statements.
func (refs *scriptRefs) scanNode(node *astNode, at scanContext) error {
	if reason, refused := refusedNodes[node.kind]; refused {
		return gap(reason)
	}
	switch node.kind {
	case "StatementList":
		return refs.scanList(node)
	case "ParameterExpr":
		parameter, err := parameterOf(node)
		refs.parameters = append(refs.parameters, parameter)
		return err
	case "SelectColumn":
		if len(node.children) == 1 && node.children[0].kind == nodePath {
			defer refs.namePseudo(node.children[0])
		}
	}
	if err := refs.visit(node, &at); err != nil {
		return err
	}
	for _, child := range node.children {
		if err := refs.scanNode(child, at); err != nil {
			return err
		}
	}
	return nil
}

// visit records what one node adds: a SELECT's scope, a star, a
// pseudo-column reference, or a table read.
func (refs *scriptRefs) visit(node *astNode, at *scanContext) error {
	switch node.kind {
	case "Select":
		at.query, at.filter = &selectRefs{}, whereOf(node)
		refs.selects = append(refs.selects, at.query)
	case nodeStar, nodeStarModified, nodeDotStar, nodeDotStarModified:
		if at.query != nil {
			at.query.stars = append(at.query.stars, starOf(node))
		}
	case nodePath, nodeColumnList:
		return refs.scanPseudo(node)
	}
	if !tableReaders[node.kind] || len(node.children) == 0 || node.children[0].kind != nodePath {
		return nil
	}
	read, err := pathOf(node.children[0])
	if err != nil {
		return err
	}
	read.filter = at.filter
	refs.statements[at.position].reads = append(refs.statements[at.position].reads, read)
	if at.query != nil {
		at.query.tables = append(at.query.tables, aliasedPath{start: read.start, alias: aliasOf(node, read)})
	}
	return nil
}

// whereOf is a SELECT's WHERE clause, or nil.
func whereOf(node *astNode) []*astNode {
	for _, child := range node.children {
		if child.kind == "WhereClause" {
			return []*astNode{child}
		}
	}
	return nil
}

// aliasOf names the alias a table read answers to: its explicit alias, or
// its last name part.
func aliasOf(node *astNode, read tablePath) string {
	for _, child := range node.children {
		if child.kind == "Alias" && len(child.children) > 0 {
			if name, err := identifierText(child.children[0].payload); err == nil {
				return strings.ToLower(name)
			}
		}
	}
	return strings.ToLower(read.parts[len(read.parts)-1])
}

// starOf reads one star of a select list. A star's * is its first byte; an
// alias.*'s follows its path and a dot.
func starOf(node *astNode) starRef {
	star := starRef{insert: node.start + 1, except: -1}
	if node.kind == nodeDotStar || node.kind == nodeDotStarModified {
		path := node.children[0]
		star.alias = lastName(path)
		star.insert = path.end + 2
		if node.kind == nodeDotStar {
			star.insert = node.end
		}
	}
	for _, child := range node.children {
		if child.kind == "StarModifiers" {
			for _, modifier := range child.children {
				if modifier.kind == "StarExceptList" {
					star.except = modifier.end - 1
				}
			}
		}
	}
	return star
}

// lastName is a path's last part, lowercased, or "".
func lastName(path *astNode) string {
	if len(path.children) == 0 {
		return ""
	}
	name, err := identifierText(path.children[len(path.children)-1].payload)
	if err != nil {
		return ""
	}
	return strings.ToLower(name)
}

// scanPseudo records a path, or an INSERT column, naming _PARTITIONTIME or
// _PARTITIONDATE.
func (refs *scriptRefs) scanPseudo(node *astNode) error {
	if node.kind == nodeColumnList {
		for _, column := range node.children {
			name, err := identifierText(column.payload)
			if err != nil {
				return err
			}
			if upper := strings.ToUpper(name); upper == namePartitionTime || upper == namePartitionDate {
				refs.pseudo = append(refs.pseudo, pseudoRef{start: column.start, end: column.end, last: column.start, name: upper})
			}
		}
		return nil
	}
	if len(node.children) == 0 {
		return nil
	}
	last := node.children[len(node.children)-1]
	name, err := identifierText(last.payload)
	if err != nil {
		return nil //nolint:nilerr // pathOf refuses an unreadable table path; other paths are columns
	}
	if upper := strings.ToUpper(name); upper == namePartitionTime || upper == namePartitionDate {
		refs.pseudo = append(refs.pseudo, pseudoRef{start: node.start, end: node.end, last: last.start, name: upper})
	}
	return nil
}

// namePseudo marks a pseudo-column reference that is a whole select column
// without an alias.
func (refs *scriptRefs) namePseudo(path *astNode) {
	for i := range refs.pseudo {
		if refs.pseudo[i].start == path.start && refs.pseudo[i].end == path.end {
			refs.pseudo[i].named = true
		}
	}
}

// parameterOf reads one ParameterExpr: @name, or a positional ? printed as
// its 1-based position.
func parameterOf(node *astNode) (parameterRef, error) {
	parameter := parameterRef{start: node.start, end: node.end}
	if len(node.children) == 1 && node.children[0].kind == nodeIdentifier {
		name, err := identifierText(node.children[0].payload)
		parameter.name = strings.ToLower(name)
		return parameter, err
	}
	position, err := strconv.Atoi(strings.Trim(node.payload, "()"))
	if err != nil || position < 1 {
		return parameter, gap("a query parameter the authorization parse cannot read")
	}
	parameter.position = position
	return parameter, nil
}

// pathOf reads a PathExpression's identifiers. A backtick-quoted identifier
// may hold several dotted parts; one with an escaped character, or a
// wildcard or INFORMATION_SCHEMA path, is refused.
func pathOf(node *astNode) (tablePath, error) {
	path := tablePath{start: node.start, end: node.end}
	for _, child := range node.children {
		if child.kind != nodeIdentifier {
			return path, gap("a table path the authorization parse cannot read")
		}
		name, err := identifierText(child.payload)
		if err != nil {
			return path, err
		}
		for part := range strings.SplitSeq(name, ".") {
			switch {
			case part == "":
				return path, invalidQuery("Invalid table name " + name)
			case strings.Contains(part, "*"):
				return path, gap("wildcard tables")
			case strings.EqualFold(part, "INFORMATION_SCHEMA"):
				return path, gap("INFORMATION_SCHEMA")
			}
			path.parts = append(path.parts, part)
		}
	}
	if len(path.parts) == 0 {
		return path, gap("a table path the authorization parse cannot read")
	}
	return path, nil
}

// errUnreadableIdentifier is an Identifier payload this parse cannot read.
var errUnreadableIdentifier = gap("an identifier the authorization parse cannot read")

// identifierText unquotes an Identifier payload, "(name)" or "(`name`)".
func identifierText(payload string) (string, error) {
	name, opened := strings.CutPrefix(payload, "(")
	name, closed := strings.CutSuffix(name, ")")
	if !opened || !closed || name == "" {
		return "", errUnreadableIdentifier
	}
	if quoted, isQuoted := strings.CutPrefix(name, "`"); isQuoted {
		var ok bool
		if name, ok = strings.CutSuffix(quoted, "`"); !ok {
			return "", errUnreadableIdentifier
		}
	}
	if strings.ContainsAny(name, "`\\") {
		return "", gap("identifiers with escaped characters")
	}
	return name, nil
}

// collectRangeVars gathers every alias, CTE name, implicit table alias, and
// FOR loop variable in the script, lowercased; a path of two or more parts
// headed by one may be a correlated column path rather than a table. CTE
// names are also kept apart: a one-part path names a CTE or a table.
func collectRangeVars(node *astNode, names, ctes map[string]bool) error {
	name, err := rangeVarOf(node)
	if err != nil {
		return err
	}
	if name != "" {
		names[name] = true
		if node.kind == "AliasedQuery" {
			ctes[name] = true
		}
	}
	for _, child := range node.children {
		if err := collectRangeVars(child, names, ctes); err != nil {
			return err
		}
	}
	return nil
}

// rangeVarOf names the range variable one node declares, lowercased: an
// alias, a CTE, a loop variable, or a table path's implicit alias (its last
// part).
func rangeVarOf(node *astNode) (string, error) {
	if len(node.children) == 0 {
		return "", nil
	}
	first := node.children[0]
	switch {
	case (node.kind == "Alias" || node.kind == "AliasedQuery" || node.kind == "ForInStatement") && first.kind == nodeIdentifier:
		name, err := identifierText(first.payload)
		return strings.ToLower(name), err
	case node.kind == "TablePathExpression" && first.kind == nodePath && len(first.children) > 0:
		name, err := identifierText(first.children[len(first.children)-1].payload)
		if err != nil {
			return "", nil //nolint:nilerr // pathOf refuses the unreadable path itself
		}
		parts := strings.Split(name, ".")
		return strings.ToLower(parts[len(parts)-1]), nil
	}
	return "", nil
}
