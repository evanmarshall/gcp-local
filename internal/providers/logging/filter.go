package logging

import (
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// This file implements a subset of the Logging query language: comparisons
// on named fields, bare or quoted global text terms, AND (explicit or
// implicit), OR, NOT or a leading "-", and parentheses. Unsupported fields
// and operators are rejected, never ignored.

// Field names the filter and ordering read.
const (
	fieldTimestamp = "timestamp"
	fieldLogName   = "logName"
	fieldSeverity  = "severity"
	fieldReceive   = "receiveTimestamp"
)

// view is one stored entry seen by the filter.
type view struct {
	project string
	stored  storedEntry
}

// Node kinds.
const (
	nodeAll = iota
	nodeAnd
	nodeOr
	nodeNot
	nodeText
	nodeCompare
)

// node is one parsed filter expression.
type node struct {
	kind        int
	left, right *node
	text        string
	compare     comparison
}

// matches evaluates the expression against one entry.
func (n *node) matches(v view) bool {
	switch n.kind {
	case nodeAnd:
		return n.left.matches(v) && n.right.matches(v)
	case nodeOr:
		return n.left.matches(v) || n.right.matches(v)
	case nodeNot:
		return !n.left.matches(v)
	case nodeText:
		return textMatches(v, n.text)
	case nodeCompare:
		return n.compare.matches(v)
	default:
		return true
	}
}

// textMatches is a global restriction: a case-insensitive substring of any
// text field (payload, labels, resource labels, log name, insert ID).
func textMatches(v view, text string) bool {
	needle := strings.ToLower(text)
	for _, value := range searchable(v) {
		if strings.Contains(strings.ToLower(value), needle) {
			return true
		}
	}
	return false
}

// comparison is one field comparison.
type comparison struct {
	field, op, value string
	pattern          *regexp.Regexp
}

// token is one lexical filter element.
type token struct {
	kind, text string
}

// Token kinds.
const (
	tokenWord   = "word"
	tokenString = "string"
	tokenOp     = "op"
	tokenOpen   = "("
	tokenClose  = ")"
)

// operators lists comparison operators longest-first.
var operators = []string{">=", "<=", "!=", "=~", "!~", "=", ">", "<", ":"}

// invalidFilter reports a filter syntax error.
func invalidFilter(message string) error {
	return status.Error(codes.InvalidArgument, "Invalid filter: "+message)
}

// lexString reads a quoted string starting after its opening quote.
func lexString(runes []rune, i int) (string, int, error) {
	var text strings.Builder
	for i < len(runes) {
		switch runes[i] {
		case '\\':
			if i+1 < len(runes) {
				text.WriteRune(runes[i+1])
			}
			i += 2
		case '"':
			return text.String(), i + 1, nil
		default:
			text.WriteRune(runes[i])
			i++
		}
	}
	return "", i, invalidFilter("unterminated string")
}

// wordRune reports whether a rune continues a bare word.
func wordRune(runes []rune, i int) bool {
	r := runes[i]
	return !unicode.IsSpace(r) && r != '(' && r != ')' && r != '"' && operatorAt(runes[i:]) == ""
}

// lexBare reads an operator or a bare word.
func lexBare(runes []rune, i int) (token, int) {
	if op := operatorAt(runes[i:]); op != "" {
		return token{kind: tokenOp, text: op}, i + len(op)
	}
	start := i
	for i < len(runes) && wordRune(runes, i) {
		i++
	}
	return token{kind: tokenWord, text: string(runes[start:i])}, i
}

// lex splits a filter into tokens.
func lex(filter string) ([]token, error) {
	var tokens []token
	runes := []rune(filter)
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '(' || r == ')':
			tokens = append(tokens, token{kind: string(r), text: string(r)})
			i++
		case r == '"':
			text, next, err := lexString(runes, i+1)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokenString, text: text})
			i = next
		default:
			var next token
			next, i = lexBare(runes, i)
			tokens = append(tokens, next)
		}
	}
	return tokens, nil
}

// operatorAt returns the comparison operator starting the input, if any.
func operatorAt(input []rune) string {
	head := string(input[:min(len(input), 2)])
	for _, op := range operators {
		if strings.HasPrefix(head, op) {
			return op
		}
	}
	return ""
}

// parser is a recursive-descent parser over the token list.
type parser struct {
	tokens []token
	at     int
}

// parseFilter compiles one filter; an empty filter matches everything.
func parseFilter(filter string) (*node, error) {
	if strings.TrimSpace(filter) == "" {
		return &node{kind: nodeAll}, nil
	}
	tokens, err := lex(filter)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	expression, err := p.or()
	if err != nil {
		return nil, err
	}
	if p.at != len(p.tokens) {
		return nil, invalidFilter("unexpected " + strconv.Quote(p.tokens[p.at].text))
	}
	return expression, nil
}

func (p *parser) peek() (token, bool) {
	if p.at >= len(p.tokens) {
		return token{}, false
	}
	return p.tokens[p.at], true
}

func (p *parser) keyword(word string) bool {
	next, ok := p.peek()
	return ok && next.kind == tokenWord && next.text == word
}

func (p *parser) or() (*node, error) {
	left, err := p.and()
	if err != nil {
		return nil, err
	}
	for p.keyword("OR") {
		p.at++
		right, err := p.and()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nodeOr, left: left, right: right}
	}
	return left, nil
}

func (p *parser) and() (*node, error) {
	left, err := p.unary()
	if err != nil {
		return nil, err
	}
	for {
		next, ok := p.peek()
		if !ok || next.kind == tokenClose || p.keyword("OR") {
			return left, nil
		}
		if p.keyword("AND") {
			p.at++
		}
		right, err := p.unary()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nodeAnd, left: left, right: right}
	}
}

func (p *parser) unary() (*node, error) {
	next, ok := p.peek()
	if !ok {
		return nil, invalidFilter("unexpected end of filter")
	}
	negated := p.keyword("NOT")
	if negated {
		p.at++
	} else if next.kind == tokenWord && len(next.text) > 1 && strings.HasPrefix(next.text, "-") {
		negated = true
		p.tokens[p.at].text = next.text[1:]
	}
	if !negated {
		return p.primary()
	}
	inner, err := p.unary()
	if err != nil {
		return nil, err
	}
	return &node{kind: nodeNot, left: inner}, nil
}

func (p *parser) primary() (*node, error) {
	next, _ := p.peek()
	p.at++
	switch next.kind {
	case tokenOpen:
		inner, err := p.or()
		if err != nil {
			return nil, err
		}
		if closing, ok := p.peek(); !ok || closing.kind != tokenClose {
			return nil, invalidFilter("missing closing parenthesis")
		}
		p.at++
		return inner, nil
	case tokenString:
		return &node{kind: nodeText, text: next.text}, nil
	case tokenWord:
		return p.comparison(next.text)
	default:
		return nil, invalidFilter("unexpected " + strconv.Quote(next.text))
	}
}

// comparison parses "field op value", or a bare word as a text term.
func (p *parser) comparison(field string) (*node, error) {
	op, ok := p.peek()
	if !ok || op.kind != tokenOp {
		return &node{kind: nodeText, text: field}, nil
	}
	p.at++
	value, ok := p.peek()
	if !ok || (value.kind != tokenWord && value.kind != tokenString) {
		return nil, invalidFilter("comparison " + strconv.Quote(field+op.text) + " has no value")
	}
	p.at++
	compare, err := newComparison(field, op.text, value.text)
	if err != nil {
		return nil, err
	}
	return &node{kind: nodeCompare, compare: compare}, nil
}

// present pairs a value with whether the entry carries it.
func present(value string) (string, bool) { return value, value != "" }

// stamp renders a timestamp field.
func stamp(moment interface{ AsTime() time.Time }, set bool) (string, bool) {
	return moment.AsTime().Format(time.RFC3339Nano), set
}

// scalarReaders read the supported top-level comparison fields as text,
// under both the JSON and proto field spellings.
var scalarReaders = map[string]func(view) (string, bool){
	fieldLogName:  func(v view) (string, bool) { return logName(v.project, v.stored.logID), true },
	fieldSeverity: func(v view) (string, bool) { return v.stored.entry.GetSeverity().String(), true },
	"insertId":    func(v view) (string, bool) { return present(v.stored.entry.GetInsertId()) },
	"trace":       func(v view) (string, bool) { return present(v.stored.entry.GetTrace()) },
	"spanId":      func(v view) (string, bool) { return present(v.stored.entry.GetSpanId()) },
	"textPayload": func(v view) (string, bool) {
		_, ok := v.stored.entry.GetPayload().(*loggingpb.LogEntry_TextPayload)
		return v.stored.entry.GetTextPayload(), ok
	},
	"resource.type": func(v view) (string, bool) {
		return v.stored.entry.GetResource().GetType(), v.stored.entry.GetResource() != nil
	},
	fieldTimestamp: func(v view) (string, bool) {
		return stamp(v.stored.entry.GetTimestamp(), v.stored.entry.GetTimestamp() != nil)
	},
	fieldReceive: func(v view) (string, bool) {
		return stamp(v.stored.entry.GetReceiveTimestamp(), v.stored.entry.GetReceiveTimestamp() != nil)
	},
}

// snakeFields maps proto field spellings to their JSON names.
var snakeFields = map[string]string{
	"log_name": fieldLogName, "insert_id": "insertId", "span_id": "spanId",
	"text_payload": "textPayload", "receive_timestamp": fieldReceive,
}

// scalarReader finds the reader of a top-level field in either spelling.
func scalarReader(field string) (func(view) (string, bool), bool) {
	if name, ok := snakeFields[field]; ok {
		field = name
	}
	reader, ok := scalarReaders[field]
	return reader, ok
}

// mapPrefixes are the supported nested comparison fields.
var mapPrefixes = []string{"resource.labels.", "labels.", "jsonPayload.", "json_payload."}

// supportedField reports whether a comparison field is modeled.
func supportedField(field string) bool {
	if _, ok := scalarReader(field); ok {
		return true
	}
	for _, prefix := range mapPrefixes {
		if strings.HasPrefix(field, prefix) && len(field) > len(prefix) {
			return true
		}
	}
	return false
}

// newComparison validates one comparison before evaluation.
func newComparison(field, op, value string) (comparison, error) {
	if !supportedField(field) {
		return comparison{}, status.Errorf(codes.InvalidArgument, "Invalid filter: field %q is not supported locally", field)
	}
	compare := comparison{field: field, op: op, value: value}
	switch {
	case op == "=~" || op == "!~":
		pattern, err := regexp.Compile(value)
		if err != nil {
			return comparison{}, invalidFilter("invalid regular expression")
		}
		compare.pattern = pattern
	case field == fieldSeverity:
		if _, ok := severityRank(value); !ok {
			return comparison{}, status.Errorf(codes.InvalidArgument, "Invalid filter: unknown severity %q", value)
		}
	case isTimeField(field) && op != ":":
		if _, err := parseTime(value); err != nil {
			return comparison{}, status.Errorf(codes.InvalidArgument, "Invalid filter: invalid timestamp %q", value)
		}
	}
	return compare, nil
}

func isTimeField(field string) bool {
	return field == fieldTimestamp || field == fieldReceive || field == "receive_timestamp"
}

// parseTime reads an RFC 3339 timestamp, a numeric-offset timestamp, or a
// date. The Python client's default 24-hour filter writes offsets as +0000.
func parseTime(value string) (time.Time, error) {
	var err error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999-0700", "2006-01-02"} {
		var parsed time.Time
		if parsed, err = time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, err //nolint:wrapcheck // callers render their own error
}

// severityRank reads a severity name or number.
func severityRank(value string) (int32, bool) {
	if rank, ok := ltype.LogSeverity_value[strings.ToUpper(value)]; ok {
		return rank, true
	}
	number, err := strconv.ParseInt(value, 10, 32)
	return int32(number), err == nil
}

// matches evaluates the comparison against one entry.
func (c comparison) matches(v view) bool {
	entry := v.stored.entry
	switch {
	case c.field == fieldSeverity && c.pattern == nil:
		rank, _ := severityRank(c.value)
		return ordered(int64(entry.GetSeverity()), int64(rank), c.op)
	case isTimeField(c.field) && c.op != ":" && c.pattern == nil:
		moment := entry.GetTimestamp()
		if c.field != fieldTimestamp {
			moment = entry.GetReceiveTimestamp()
		}
		bound, _ := parseTime(c.value)
		return ordered(moment.AsTime().UnixNano(), bound.UnixNano(), c.op)
	}
	actual, present := fieldValue(v, c.field)
	if c.op == ":" && c.value == "*" {
		return present
	}
	return present && c.matchesText(v, actual)
}

// matchesPattern applies the regular-expression and has operators.
func (c comparison) matchesPattern(actual string) (bool, bool) {
	switch c.op {
	case "=~":
		return c.pattern.MatchString(actual), true
	case "!~":
		return !c.pattern.MatchString(actual), true
	case ":":
		return strings.Contains(strings.ToLower(actual), strings.ToLower(c.value)), true
	default:
		return false, false
	}
}

// matchesLogName accepts the encoded resource name or the decoded log ID
// form in a log name equality.
func (c comparison) matchesLogName(v view, actual string) (bool, bool) {
	if (c.field != fieldLogName && c.field != "log_name") || (c.op != "=" && c.op != "!=") {
		return false, false
	}
	decoded := "projects/" + v.project + "/logs/" + v.stored.logID
	equal := actual == c.value || decoded == c.value
	return equal == (c.op == "="), true
}

// matchesText compares a present text value.
func (c comparison) matchesText(v view, actual string) bool {
	if matched, decided := c.matchesPattern(actual); decided {
		return matched
	}
	if matched, decided := c.matchesLogName(v, actual); decided {
		return matched
	}
	left, leftErr := strconv.ParseFloat(actual, 64)
	right, rightErr := strconv.ParseFloat(c.value, 64)
	if leftErr == nil && rightErr == nil && c.op != "=" && c.op != "!=" {
		return orderedFloat(left, right, c.op)
	}
	return orderedString(actual, c.value, c.op)
}

// ordered applies a comparison operator to two integers.
func ordered(left, right int64, op string) bool {
	switch op {
	case "=", ":":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	}
	return false
}

func orderedFloat(left, right float64, op string) bool {
	switch op {
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	}
	return false
}

func orderedString(left, right, op string) bool {
	switch op {
	case "=":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	}
	return false
}

// fieldValue reads one comparable field as text.
func fieldValue(v view, field string) (string, bool) {
	if reader, ok := scalarReader(field); ok {
		return reader(v)
	}
	entry := v.stored.entry
	if key, ok := strings.CutPrefix(field, "resource.labels."); ok {
		value, found := entry.GetResource().GetLabels()[key]
		return value, found
	}
	if key, ok := strings.CutPrefix(field, "labels."); ok {
		value, found := entry.GetLabels()[key]
		return value, found
	}
	path, ok := strings.CutPrefix(field, "jsonPayload.")
	if !ok {
		path, _ = strings.CutPrefix(field, "json_payload.")
	}
	return jsonField(entry.GetJsonPayload(), strings.Split(path, "."))
}

// jsonField walks a structured payload to one scalar value.
func jsonField(payload *structpb.Struct, path []string) (string, bool) {
	if payload == nil {
		return "", false
	}
	value, ok := payload.GetFields()[path[0]]
	if !ok {
		return "", false
	}
	if len(path) > 1 {
		return jsonField(value.GetStructValue(), path[1:])
	}
	switch kind := value.GetKind().(type) {
	case *structpb.Value_StringValue:
		return kind.StringValue, true
	case *structpb.Value_NumberValue:
		return strconv.FormatFloat(kind.NumberValue, 'f', -1, 64), true
	case *structpb.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue), true
	case *structpb.Value_NullValue:
		return "", false
	default:
		raw, err := value.MarshalJSON()
		return string(raw), err == nil
	}
}

// walkJSON appends every string and number in a structured value.
func walkJSON(value *structpb.Value, values []string) []string {
	switch kind := value.GetKind().(type) {
	case *structpb.Value_StringValue:
		values = append(values, kind.StringValue)
	case *structpb.Value_NumberValue:
		values = append(values, strconv.FormatFloat(kind.NumberValue, 'f', -1, 64))
	case *structpb.Value_StructValue:
		for _, child := range kind.StructValue.GetFields() {
			values = walkJSON(child, values)
		}
	case *structpb.Value_ListValue:
		for _, child := range kind.ListValue.GetValues() {
			values = walkJSON(child, values)
		}
	}
	return values
}

// searchable lists the text a global restriction searches.
func searchable(v view) []string {
	entry := v.stored.entry
	values := []string{logName(v.project, v.stored.logID), entry.GetInsertId(), entry.GetTextPayload(), entry.GetResource().GetType()}
	for _, label := range entry.GetLabels() {
		values = append(values, label)
	}
	for _, label := range entry.GetResource().GetLabels() {
		values = append(values, label)
	}
	if payload := entry.GetJsonPayload(); payload != nil {
		values = walkJSON(structpb.NewStructValue(payload), values)
	}
	return values
}
