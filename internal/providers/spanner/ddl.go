package spanner

import (
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The local DDL slice covers CREATE TABLE with plain columns and a primary key,
// and ALTER TABLE ... ADD COLUMN. getDdl renders tables in Spanner's canonical
// form, which the recording pins exactly:
//
//	CREATE TABLE Items (\n  Id STRING(36) NOT NULL,\n  Name STRING(64),\n) PRIMARY KEY(Id)
//
// Every other statement is an explicit UNIMPLEMENTED gap.
var (
	createTablePattern = regexp.MustCompile(`(?is)^\s*CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*PRIMARY\s+KEY\s*\(([^()]*)\)\s*$`)
	addColumnPattern   = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s+ADD\s+COLUMN\s+(.+?)\s*$`)
	columnPattern      = regexp.MustCompile(`(?is)^([A-Za-z_][A-Za-z0-9_]*)\s+(ARRAY\s*<\s*[A-Za-z0-9]+(?:\s*\(\s*(?:[0-9]+|MAX)\s*\))?\s*>|[A-Za-z0-9]+(?:\s*\(\s*(?:[0-9]+|MAX)\s*\))?)(\s+NOT\s+NULL)?$`)
	keyPartPattern     = regexp.MustCompile(`(?is)^([A-Za-z_][A-Za-z0-9_]*)(\s+DESC)?$`)
	whitespacePattern  = regexp.MustCompile(`\s+`)
	createDBPattern    = regexp.MustCompile("(?is)^\\s*CREATE\\s+DATABASE\\s+(`[^`]*`|[^\\s`]+)\\s*$")
)

// scalarTypes are the GoogleSQL column types the canonicalizer accepts.
var scalarTypes = map[string]bool{
	"BOOL": true, "INT64": true, "FLOAT32": true, "FLOAT64": true, "NUMERIC": true,
	"STRING": true, "BYTES": true, "DATE": true, "TIMESTAMP": true, "JSON": true,
}

// ddlColumn is one canonical column definition.
type ddlColumn struct {
	Name       string
	Definition string
}

// ddlTable is one stored table schema.
type ddlTable struct {
	Name       string
	Columns    []ddlColumn
	PrimaryKey []string
}

// render prints the table in Spanner's canonical getDdl form.
func (t ddlTable) render() string {
	var out strings.Builder
	out.WriteString("CREATE TABLE " + t.Name + " (\n")
	for _, column := range t.Columns {
		out.WriteString("  " + column.Name + " " + column.Definition + ",\n")
	}
	out.WriteString(") PRIMARY KEY(" + strings.Join(t.PrimaryKey, ", ") + ")")
	return out.String()
}

// column finds one column by Spanner's case-insensitive name match.
func (t ddlTable) column(name string) bool {
	for _, column := range t.Columns {
		if strings.EqualFold(column.Name, name) {
			return true
		}
	}
	return false
}

// clone copies the table so failed DDL batches never mutate stored schema.
func (t ddlTable) clone() ddlTable {
	t.Columns = append([]ddlColumn(nil), t.Columns...)
	t.PrimaryKey = append([]string(nil), t.PrimaryKey...)
	return t
}

// applyDDL applies statements to a copy of tables and returns the new schema.
func applyDDL(tables []ddlTable, statements []string) ([]ddlTable, error) {
	next := make([]ddlTable, 0, len(tables)+len(statements))
	for _, table := range tables {
		next = append(next, table.clone())
	}
	for _, statement := range statements {
		var err error
		if next, err = applyStatement(next, statement); err != nil {
			return nil, err
		}
	}
	return next, nil
}

// applyStatement applies one supported DDL statement.
func applyStatement(tables []ddlTable, statement string) ([]ddlTable, error) {
	if match := createTablePattern.FindStringSubmatch(statement); match != nil {
		table, err := parseCreateTable(match[1], match[2], match[3])
		if err != nil {
			return nil, err
		}
		if findTable(tables, table.Name) >= 0 {
			return nil, status.Errorf(codes.FailedPrecondition, "Duplicate name in schema: %s.", table.Name)
		}
		return append(tables, table), nil
	}
	if match := addColumnPattern.FindStringSubmatch(statement); match != nil {
		return addColumn(tables, match[1], match[2])
	}
	return nil, status.Errorf(codes.Unimplemented,
		"gcp-local models only CREATE TABLE and ALTER TABLE ... ADD COLUMN DDL; unsupported statement: %s", statement)
}

// parseCreateTable parses the column list and primary key of one table.
func parseCreateTable(name, body, key string) (ddlTable, error) {
	table := ddlTable{Name: name}
	for part := range strings.SplitSeq(body, ",") {
		if strings.TrimSpace(part) == "" {
			continue
		}
		column, err := parseColumn(part)
		if err != nil {
			return ddlTable{}, err
		}
		if table.column(column.Name) {
			return ddlTable{}, status.Errorf(codes.FailedPrecondition, "Duplicate column name %s.%s.", name, column.Name)
		}
		table.Columns = append(table.Columns, column)
	}
	if len(table.Columns) == 0 {
		return ddlTable{}, status.Errorf(codes.InvalidArgument, "Table %s has no columns", name)
	}
	for part := range strings.SplitSeq(key, ",") {
		keyPart, err := parseKeyPart(table, part)
		if err != nil {
			return ddlTable{}, err
		}
		table.PrimaryKey = append(table.PrimaryKey, keyPart)
	}
	return table, nil
}

// parseKeyPart validates one primary-key column reference.
func parseKeyPart(table ddlTable, part string) (string, error) {
	part = strings.TrimSpace(part)
	if part == "" && len(table.PrimaryKey) == 0 {
		return "", status.Errorf(codes.Unimplemented, "gcp-local does not model empty primary keys on %s", table.Name)
	}
	match := keyPartPattern.FindStringSubmatch(part)
	if match == nil {
		return "", status.Errorf(codes.Unimplemented, "gcp-local does not model primary key part %q", part)
	}
	if !table.column(match[1]) {
		return "", status.Errorf(codes.FailedPrecondition, "Table %s references nonexistent key column %s.", table.Name, match[1])
	}
	if match[2] != "" {
		return match[1] + " DESC", nil
	}
	return match[1], nil
}

// parseColumn canonicalizes one `Name TYPE [NOT NULL]` column definition.
func parseColumn(definition string) (ddlColumn, error) {
	definition = strings.TrimSpace(definition)
	match := columnPattern.FindStringSubmatch(definition)
	if match == nil {
		return ddlColumn{}, status.Errorf(codes.Unimplemented, "gcp-local does not model column definition %q", definition)
	}
	columnType := strings.ToUpper(whitespacePattern.ReplaceAllString(match[2], ""))
	base := strings.TrimPrefix(columnType, "ARRAY<")
	if index := strings.IndexAny(base, "(>"); index >= 0 {
		base = base[:index]
	}
	if !scalarTypes[base] {
		return ddlColumn{}, status.Errorf(codes.InvalidArgument, "Unknown column type %s", match[2])
	}
	if match[3] != "" {
		columnType += " NOT NULL"
	}
	return ddlColumn{Name: match[1], Definition: columnType}, nil
}

// addColumn folds ALTER TABLE ... ADD COLUMN into the stored table.
func addColumn(tables []ddlTable, name, definition string) ([]ddlTable, error) {
	index := findTable(tables, name)
	if index < 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "Table not found: %s", name)
	}
	column, err := parseColumn(definition)
	if err != nil {
		return nil, err
	}
	if tables[index].column(column.Name) {
		return nil, status.Errorf(codes.FailedPrecondition, "Duplicate column name %s.%s.", tables[index].Name, column.Name)
	}
	tables[index].Columns = append(tables[index].Columns, column)
	return tables, nil
}

// findTable locates a table by case-insensitive name, or -1.
func findTable(tables []ddlTable, name string) int {
	for i, table := range tables {
		if strings.EqualFold(table.Name, name) {
			return i
		}
	}
	return -1
}

// renderDDL prints every stored table in creation order.
func renderDDL(tables []ddlTable) []string {
	statements := make([]string, 0, len(tables))
	for _, table := range tables {
		statements = append(statements, table.render())
	}
	return statements
}

// databaseIDFromStatement extracts the id from `CREATE DATABASE id`, or "".
func databaseIDFromStatement(statement string) string {
	match := createDBPattern.FindStringSubmatch(statement)
	if match == nil {
		return ""
	}
	return strings.Trim(match[1], "`")
}
