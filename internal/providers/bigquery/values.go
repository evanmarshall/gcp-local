package bigquery

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/goccy/googlesqlite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GoogleSQL type names, and the legacy names the REST schema uses.
const (
	sqlString     = "STRING"
	sqlBytes      = "BYTES"
	sqlInt64      = "INT64"
	sqlFloat64    = "FLOAT64"
	sqlBool       = "BOOL"
	sqlNumeric    = "NUMERIC"
	sqlBignumeric = "BIGNUMERIC"
	sqlDate       = "DATE"
	sqlDatetime   = "DATETIME"
	sqlTime       = "TIME"
	sqlTimestamp  = "TIMESTAMP"
	sqlGeography  = "GEOGRAPHY"
	sqlJSON       = "JSON"
	sqlInterval   = "INTERVAL"
	sqlStruct     = "STRUCT"
	sqlArray      = "ARRAY"
	sqlRange      = "RANGE"
	restInteger   = "INTEGER"
	restFloat     = "FLOAT"
	restBoolean   = "BOOLEAN"
	restRecord    = "RECORD"
	modeRepeated  = "REPEATED"
	modeRequired  = "REQUIRED"
	modeNullable  = "NULLABLE"
	keyType       = "type"
	keyName       = "name"
	keyMode       = "mode"
)

// sqlTypeName folds a REST or legacy type name to its GoogleSQL name.
func sqlTypeName(name string) string {
	switch upper := strings.ToUpper(name); upper {
	case restInteger:
		return sqlInt64
	case restFloat:
		return sqlFloat64
	case restBoolean:
		return sqlBool
	case restRecord:
		return sqlStruct
	default:
		return upper
	}
}

// engineType is a GoogleSQL type as the engine reports it for a column.
type engineType struct {
	Name        string             `json:"name"`
	Kind        googlesql.TypeKind `json:"kind"`
	ElementType *engineType        `json:"elementType"`
	FieldTypes  []struct {
		Name string      `json:"name"`
		Type *engineType `json:"type"`
	} `json:"fieldTypes"`
}

// engineColumn is one result column.
type engineColumn struct {
	name string
	kind *engineType
}

func parseEngineType(raw string) (*engineType, error) {
	parsed := &engineType{}
	if err := json.Unmarshal([]byte(raw), parsed); err != nil {
		return nil, fmt.Errorf("read an engine column type: %w", err)
	}
	return parsed, nil
}

// restNames are the REST schema names of GoogleSQL types.
var restNames = map[googlesql.TypeKind]string{
	googlesql.TypeKindTypeInt64: restInteger, googlesql.TypeKindTypeDouble: restFloat, googlesql.TypeKindTypeBool: restBoolean,
	googlesql.TypeKindTypeString: sqlString, googlesql.TypeKindTypeBytes: sqlBytes, googlesql.TypeKindTypeDate: sqlDate,
	googlesql.TypeKindTypeTimestamp: sqlTimestamp, googlesql.TypeKindTypeTime: sqlTime, googlesql.TypeKindTypeDatetime: sqlDatetime,
	googlesql.TypeKindTypeGeography: sqlGeography, googlesql.TypeKindTypeNumeric: sqlNumeric,
	googlesql.TypeKindTypeBignumeric: sqlBignumeric, googlesql.TypeKindTypeJson: sqlJSON,
	googlesql.TypeKindTypeInterval: sqlInterval, googlesql.TypeKindTypeStruct: restRecord,
}

// schemaField renders one engine column as a REST schema field: every
// result column is NULLABLE, and an array is its element type REPEATED.
func schemaField(name string, kind *engineType) map[string]any {
	mode := modeNullable
	if kind.Kind == googlesql.TypeKindTypeArray && kind.ElementType != nil {
		kind, mode = kind.ElementType, modeRepeated
	}
	typeName, ok := restNames[kind.Kind]
	if !ok {
		typeName = kind.Name
	}
	field := map[string]any{keyName: name, keyType: typeName, keyMode: mode}
	if kind.Kind == googlesql.TypeKindTypeStruct {
		nested := make([]any, 0, len(kind.FieldTypes))
		for _, member := range kind.FieldTypes {
			nested = append(nested, schemaField(member.Name, member.Type))
		}
		field[keyFields] = nested
	}
	return field
}

// tableField renders one column of a table a DDL statement created: a table
// schema omits the NULLABLE mode (recorded: bigquery-sql-engine), and the
// engine does not report NOT NULL, so no field is REQUIRED.
func tableField(name string, kind *engineType) map[string]any {
	field := schemaField(name, kind)
	stripNullable(field)
	return field
}

// stripNullable removes NULLABLE modes from a field and its members.
func stripNullable(field map[string]any) {
	if field[keyMode] == modeNullable {
		delete(field, keyMode)
	}
	members, _ := field[keyFields].([]any)
	for _, member := range members {
		if nested, ok := member.(map[string]any); ok {
			stripNullable(nested)
		}
	}
}

// resultFields renders a result's schema fields.
func resultFields(columns []engineColumn) []map[string]any {
	fields := make([]map[string]any, 0, len(columns))
	for _, column := range columns {
		fields = append(fields, schemaField(column.name, column.kind))
	}
	return fields
}

// columnPattern is the BigQuery column-name alphabet the engine accepts.
var columnPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,299}$`)

// columnTypes are the GoogleSQL column types a REST schema may name.
var columnTypes = map[string]bool{
	sqlString: true, sqlBytes: true, sqlInt64: true, sqlFloat64: true, sqlNumeric: true, sqlBignumeric: true,
	sqlBool: true, sqlTimestamp: true, sqlDate: true, sqlTime: true, sqlDatetime: true, sqlGeography: true,
	sqlJSON: true, sqlInterval: true,
}

// fieldName and fieldType read one schema field's name and GoogleSQL type.
func fieldName(field map[string]any) string { name, _ := field[keyName].(string); return name }
func fieldType(field map[string]any) string { return sqlTypeName(fmt.Sprint(field[keyType])) }

// columnDefinitions renders a REST schema as engine column definitions.
func columnDefinitions(fields []map[string]any) (string, error) {
	definitions := make([]string, 0, len(fields))
	for _, field := range fields {
		definition, err := columnType(field)
		if err != nil {
			return "", err
		}
		if field[keyMode] == modeRequired {
			definition += " NOT NULL"
		}
		definitions = append(definitions, "`"+fieldName(field)+"` "+definition)
	}
	if len(definitions) == 0 {
		return "", provider.Invalid("A table requires a schema with fields")
	}
	return strings.Join(definitions, ", "), nil
}

// columnType renders one field's GoogleSQL type.
func columnType(field map[string]any) (string, error) {
	name := fieldName(field)
	if !columnPattern.MatchString(name) {
		return "", provider.Invalid("Invalid field name \"" + name + "\"")
	}
	definition := fieldType(field)
	switch {
	case definition == sqlStruct:
		var err error
		if definition, err = structType(field); err != nil {
			return "", err
		}
	case !columnTypes[definition]:
		return "", gap("column type " + definition)
	}
	switch mode := fmt.Sprint(field[keyMode]); mode {
	case modeRepeated:
		return sqlArray + "<" + definition + ">", nil
	case "", "<nil>", modeNullable, modeRequired:
		return definition, nil
	default:
		return "", provider.Invalid("Invalid field mode " + mode)
	}
}

// structType renders a RECORD field's STRUCT type.
func structType(field map[string]any) (string, error) {
	members := schemaFields(map[string]any{keyFields: field[keyFields]})
	parts := make([]string, 0, len(members))
	for _, member := range members {
		memberType, err := columnType(member)
		if err != nil {
			return "", err
		}
		parts = append(parts, "`"+fieldName(member)+"` "+memberType)
	}
	if len(parts) == 0 {
		return "", provider.Invalid("Field " + fieldName(field) + " is type RECORD but has no schema")
	}
	return sqlStruct + "<" + strings.Join(parts, ", ") + ">", nil
}

// typingCalls give a value bound as text its declared type, as an insert
// placeholder or around a query parameter; other scalar types bind natively.
var typingCalls = map[string]string{
	sqlDate: "CAST(%s AS DATE)", sqlDatetime: "CAST(%s AS DATETIME)", sqlTime: "CAST(%s AS TIME)",
	sqlTimestamp: "CAST(%s AS TIMESTAMP)", sqlNumeric: "CAST(%s AS NUMERIC)", sqlBignumeric: "CAST(%s AS BIGNUMERIC)",
	sqlInterval: "CAST(%s AS INTERVAL)", sqlBytes: "FROM_BASE64(%s)", sqlJSON: "PARSE_JSON(%s)",
	sqlGeography: "ST_GEOGFROMTEXT(%s)",
}

// placeholder is the insert expression that types one bound value.
// TIMESTAMP values bind natively as time.Time.
func placeholder(field map[string]any) string {
	kind := fieldType(field)
	if wrap, ok := typingCalls[kind]; ok && field[keyMode] != modeRepeated && kind != sqlTimestamp {
		return strings.ReplaceAll(wrap, "%s", "?")
	}
	return "?"
}

// badValueError is a row value that does not fit its column.
type badValueError struct{ field, reason string }

func (e badValueError) Error() string {
	return "Cannot convert value of field " + e.field + ": " + e.reason
}

// insertValue converts one insertAll JSON value for its column. RECORD
// columns are a coverage gap: the engine binds structs from maps, whose
// field order is not stable.
func insertValue(field map[string]any, value any) (any, error) {
	name, kind := fieldName(field), fieldType(field)
	if kind == sqlStruct {
		return nil, gap("insertAll into RECORD columns")
	}
	if field[keyMode] != modeRepeated {
		return scalarValue(name, kind, value)
	}
	if value == nil {
		return []any{}, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, badValueError{name, "a REPEATED field takes an array"}
	}
	switch kind {
	case sqlInt64:
		return typedSlice[int64](name, kind, items)
	case sqlFloat64:
		return typedSlice[float64](name, kind, items)
	case sqlBool:
		return typedSlice[bool](name, kind, items)
	case sqlString:
		return typedSlice[string](name, kind, items)
	default:
		return nil, gap("insertAll into REPEATED " + kind + " columns")
	}
}

// typedSlice binds an array as a typed slice, which the engine types as an
// ARRAY of that element.
func typedSlice[T any](name, kind string, items []any) ([]T, error) {
	out := make([]T, 0, len(items))
	for _, item := range items {
		converted, err := scalarValue(name, kind, item)
		typed, ok := converted.(T)
		if err != nil || !ok {
			return nil, badValueError{name, "an array element is not a valid " + kind}
		}
		out = append(out, typed)
	}
	return out, nil
}

// scalarConverters convert one JSON scalar the way the streaming API accepts
// it: numbers and booleans may also arrive as strings. Other types arrive as
// text, which the insert placeholder casts.
var scalarConverters = map[string]func(any) (any, bool){
	sqlInt64:      toInt64,
	sqlFloat64:    toFloat64,
	sqlBool:       toBool,
	sqlTimestamp:  toTimestamp,
	sqlJSON:       toJSONText,
	sqlNumeric:    toNumericText,
	sqlBignumeric: toNumericText,
}

// scalarValue converts one JSON value for a scalar column; nil is NULL.
func scalarValue(name, kind string, value any) (any, error) {
	if value == nil {
		return nil, nil //nolint:nilnil // a nil value is SQL NULL, not a missing result
	}
	convert, ok := scalarConverters[kind]
	if !ok {
		convert = toText
	}
	converted, ok := convert(value)
	if !ok {
		return nil, badValueError{name, "not a valid " + kind}
	}
	return converted, nil
}

func toInt64(value any) (any, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), typed == math.Trunc(typed) && math.Abs(typed) < 1<<63
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	}
	return nil, false
}

func toFloat64(value any) (any, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case string:
		parsed, err := strconv.ParseFloat(typed, 64)
		return parsed, err == nil
	}
	return nil, false
}

func toBool(value any) (any, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(typed)
		return parsed, err == nil
	}
	return nil, false
}

// toTimestamp reads seconds since the epoch or timestamp text.
func toTimestamp(value any) (any, bool) {
	switch typed := value.(type) {
	case float64:
		return time.UnixMicro(int64(math.Round(typed * 1e6))).UTC(), true
	case string:
		parsed, err := googlesqlite.TimeFromTimestampValue(typed)
		return parsed.UTC(), err == nil
	}
	return nil, false
}

// toJSONText reads JSON text, or encodes any other JSON value.
func toJSONText(value any) (any, bool) {
	if text, ok := value.(string); ok {
		return text, true
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err == nil
}

func toNumericText(value any) (any, bool) {
	if number, ok := value.(float64); ok {
		return strconv.FormatFloat(number, 'f', -1, 64), true
	}
	return toText(value)
}

func toText(value any) (any, bool) {
	text, ok := value.(string)
	return text, ok
}

// cellOf renders one engine value in the REST cell encoding: scalars are
// strings, arrays are lists of {"v"} cells, and structs are {"f"} rows.
// TIMESTAMP cells are float seconds, or integer microseconds when the
// caller asked for formatOptions.useInt64Timestamp.
func cellOf(value any, kind *engineType, int64Timestamp bool) any {
	if kind.Kind == googlesql.TypeKindTypeArray {
		items, _ := value.([]any)
		cells := make([]any, 0, len(items))
		for _, item := range items {
			cells = append(cells, map[string]any{"v": cellOf(item, kind.ElementType, int64Timestamp)})
		}
		return cells
	}
	if kind.Kind == googlesql.TypeKindTypeStruct {
		return structCell(value, kind, int64Timestamp)
	}
	return scalarCell(value, kind, int64Timestamp)
}

// scalarCell renders one scalar value as its REST string.
func scalarCell(value any, kind *engineType, int64Timestamp bool) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return floatCell(typed)
	case bool:
		return strconv.FormatBool(typed)
	case string:
		if kind.Kind == googlesql.TypeKindTypeTimestamp {
			return timestampCell(typed, int64Timestamp)
		}
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

// structCell renders a STRUCT, which the engine returns positionally.
func structCell(value any, kind *engineType, int64Timestamp bool) any {
	if value == nil {
		return nil
	}
	members, _ := value.([]any)
	cells := make([]any, 0, len(members))
	for i, member := range members {
		if i < len(kind.FieldTypes) {
			cells = append(cells, map[string]any{"v": cellOf(member, kind.FieldTypes[i].Type, int64Timestamp)})
		}
	}
	return map[string]any{"f": cells}
}

// floatCell renders a FLOAT64 cell the way the REST surface does, which is
// Java's Double.toString: "15.0" (recorded: bigquery-sql-engine), decimal
// for magnitudes in [1e-3, 1e7), and "1.5E10" otherwise. Non-finite values
// use BigQuery's names.
func floatCell(value float64) string {
	switch magnitude := math.Abs(value); {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	case magnitude == 0 || (magnitude >= 1e-3 && magnitude < 1e7):
		text := strconv.FormatFloat(value, 'f', -1, 64)
		if !strings.Contains(text, ".") {
			text += ".0"
		}
		return text
	default:
		return scientific(value)
	}
}

// scientific renders d.dddE<exponent> with at least one fraction digit.
func scientific(value float64) string {
	mantissa, exponent, _ := strings.Cut(strconv.FormatFloat(value, 'E', -1, 64), "E")
	if !strings.Contains(mantissa, ".") {
		mantissa += ".0"
	}
	sign := ""
	if strings.HasPrefix(exponent, "-") {
		sign = "-"
	}
	digits := strings.TrimLeft(strings.TrimLeft(exponent, "+-"), "0")
	if digits == "" {
		digits = "0"
	}
	return mantissa + "E" + sign + digits
}

// timestampCell renders a TIMESTAMP cell from the engine's canonical text.
// The float form follows the REST encoding, for example "1.4084520955E9".
func timestampCell(text string, int64Timestamp bool) string {
	moment, err := googlesqlite.TimeFromTimestampValue(text)
	if err != nil {
		return text
	}
	micros := moment.UnixMicro()
	if int64Timestamp {
		return strconv.FormatInt(micros, 10)
	}
	return scientific(float64(micros) / 1e6)
}

// cellRows renders result rows for the REST surface.
func cellRows(columns []engineColumn, rows [][]any, int64Timestamp bool) []any {
	out := make([]any, 0, len(rows))
	for _, row := range rows {
		cells := make([]any, 0, len(columns))
		for i, column := range columns {
			cells = append(cells, map[string]any{"v": cellOf(row[i], column.kind, int64Timestamp)})
		}
		out = append(out, map[string]any{"f": cells})
	}
	return out
}

// engineLimit matches the engine's own refusals of valid GoogleSQL, as
// opposed to errors GoogleSQL itself raises.
var engineLimit = regexp.MustCompile(`(?i)not supported|unsupported|not implemented|must be a single-table reference|supports \w+ only|only supports`)

// engineError maps an engine failure to the REST error a client sees: a
// missing table is 404, a statement the engine does not run is an explicit
// coverage gap, and every other analysis or runtime failure is an invalid
// query (400), which clients do not retry.
func engineError(err error) error {
	message := strings.TrimPrefix(err.Error(), "failed to analyze: ")
	switch {
	case strings.Contains(message, "Table not found: "):
		_, name, _ := strings.Cut(message, "Table not found: ")
		name, _, _ = strings.Cut(name, " [at ")
		name, _, _ = strings.Cut(name, ";")
		return status.Error(codes.NotFound, "Not found: Table "+name)
	case engineLimit.MatchString(message):
		return gap("the engine does not run this query: " + message)
	default:
		return invalidQuery(message)
	}
}

// invalidQuery is a 400 for a query GoogleSQL refuses.
func invalidQuery(message string) error {
	return status.Error(codes.InvalidArgument, message)
}
