package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
)

// timePartitioning is a table's time-unit partitioning on Field, or
// ingestion-time partitioning when Field is "": the engine then keeps the
// rows' partition time in a hidden _PARTITIONTIME column. ExpirationMs is
// the partition expiration, 0 for none.
type timePartitioning struct {
	Type         string
	Field        string
	ExpirationMs int64
}

// rangePartitioning is a table's integer-range partitioning.
type rangePartitioning struct {
	Field                string
	Start, End, Interval int64
}

// pseudoColumn is the engine column that holds an ingestion-time table's
// partition time. It is always backtick-quoted, so the engine's own
// _PARTITIONTIME rewrite (googlesqlite v0.3.1, which substitutes
// CURRENT_TIMESTAMP()) leaves it alone.
const pseudoColumn = "`_PARTITIONTIME`"

// Pseudo-column names as queries spell them.
const (
	namePartitionTime = "_PARTITIONTIME"
	namePartitionDate = "_PARTITIONDATE"
)

// partitionDay is the daily partition type, the default.
const partitionDay = "DAY"

// partitionTypes are the documented TimePartitioning.type values.
var partitionTypes = []string{partitionDay, "HOUR", "MONTH", "YEAR"}

// timeColumnTypes are the column types time-unit partitioning takes.
var timeColumnTypes = []string{sqlTimestamp, sqlDate, sqlDatetime}

// maxClusteringFields is BigQuery's clustering limit.
const maxClusteringFields = 4

// specFrozen is GCP's refusal to change a table's partitioning (recorded).
const specFrozen = "Cannot change partitioning/clustering spec for a partitioned/clustered table."

// errInt64 refuses a value that is not a decimal int64.
var errInt64 = errors.New("invalid int64 value")

// optionalInt is an int64 request field that GCP sends as a decimal string
// or a number: Set when the body names it, Null when it is null.
type optionalInt struct {
	Set, Null bool
	Value     int64
}

func (o *optionalInt) UnmarshalJSON(raw []byte) error {
	o.Set = true
	if string(raw) == jsonNull {
		o.Null = true
		return nil
	}
	value, err := strconv.ParseInt(strings.Trim(string(raw), `"`), 10, 64)
	if err != nil {
		return fmt.Errorf("%w %s", errInt64, raw)
	}
	o.Value = value
	return nil
}

// present reports a non-null value.
func (o optionalInt) present() bool { return o.Set && !o.Null }

// timePartitioningBody is TimePartitioning as sent. requirePartitionFilter
// here is the deprecated spelling of the table's own field.
type timePartitioningBody struct {
	Type                   string      `json:"type"`
	Field                  string      `json:"field"`
	ExpirationMs           optionalInt `json:"expirationMs"`
	RequirePartitionFilter *bool       `json:"requirePartitionFilter"`
}

// rangePartitioningBody is RangePartitioning as sent.
type rangePartitioningBody struct {
	Field string `json:"field"`
	Range struct {
		Start    optionalInt `json:"start"`
		End      optionalInt `json:"end"`
		Interval optionalInt `json:"interval"`
	} `json:"range"`
}

// clusteringBody is Clustering as sent; Set when the body names it, and nil
// Fields (null, {}, or an empty list) removes clustering.
type clusteringBody struct {
	Set    bool
	Fields []string
}

func (c *clusteringBody) UnmarshalJSON(raw []byte) error {
	c.Set = true
	if string(raw) == jsonNull {
		return nil
	}
	var body struct {
		Fields []string `json:"fields"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return fmt.Errorf("invalid clustering: %w", err)
	}
	c.Fields = body.Fields
	return nil
}

// layout is a table's partitioning, clustering, filter requirement, and
// expiration, validated against its schema.
type layout struct {
	time       *timePartitioning
	ranged     *rangePartitioning
	clustering []string
	filter     *bool
	expiration time.Time
}

// columnOf finds a top-level schema column by name; GCP compares column
// names without case.
func columnOf(fields []map[string]any, name string) map[string]any {
	for _, field := range fields {
		if strings.EqualFold(fieldName(field), name) {
			return field
		}
	}
	return nil
}

// newLayout validates a tables.insert body's layout, with GCP's messages
// (recorded: bigquery-partitioning-expiration). A body with both kinds of
// partitioning keeps only the range, as GCP does (recorded).
func newLayout(body tableBody, fields []map[string]any, now time.Time) (layout, error) {
	var result layout
	switch {
	case body.RangePartitioning != nil:
		ranged, err := rangeLayout(*body.RangePartitioning, fields)
		if err != nil {
			return result, err
		}
		result.ranged = ranged
	case body.TimePartitioning != nil:
		partitioning, err := timeLayout(*body.TimePartitioning, fields)
		if err != nil {
			return result, err
		}
		result.time = partitioning
	}
	clustering, err := clusteringLayout(body.Clustering.Fields, fields)
	if err != nil {
		return result, err
	}
	result.clustering = clustering
	result.filter = body.requireFilter()
	if body.ExpirationTime.present() {
		if result.expiration, err = expirationOf(body.ExpirationTime.Value, now); err != nil {
			return result, err
		}
	}
	return result, nil
}

// requireFilter reads requirePartitionFilter, or its deprecated spelling
// inside timePartitioning.
func (body tableBody) requireFilter() *bool {
	if body.RequirePartitionFilter == nil && body.TimePartitioning != nil {
		return body.TimePartitioning.RequirePartitionFilter
	}
	return body.RequirePartitionFilter
}

// timeLayout validates time-unit or ingestion-time partitioning. A missing
// type is DAY, as the client libraries default it (not recorded).
func timeLayout(body timePartitioningBody, fields []map[string]any) (*timePartitioning, error) {
	kind := cmpType(strings.ToUpper(body.Type))
	if !slices.Contains(partitionTypes, kind) {
		return nil, failure(codes.InvalidArgument, reasonInvalid, "Invalid value for timePartitioning.type: "+body.Type+" is not a valid value")
	}
	partitioning := &timePartitioning{Type: kind}
	if body.Field != "" {
		column := columnOf(fields, body.Field)
		if column == nil {
			return nil, failure(codes.InvalidArgument, reasonInvalid, "The field specified for partitioning cannot be found in the schema.")
		}
		if kind := fieldType(column); !slices.Contains(timeColumnTypes, kind) {
			return nil, failure(codes.InvalidArgument, reasonInvalid,
				"The field specified for time partitioning can only be of type TIMESTAMP, DATE or DATETIME. The type found is: "+kind+".")
		}
		partitioning.Field = fieldName(column)
	}
	if body.ExpirationMs.present() {
		if body.ExpirationMs.Value <= 0 {
			// Not recorded; GCP documents only a positive expiration.
			return nil, failure(codes.InvalidArgument, reasonInvalid, "Partition expiration must be positive")
		}
		partitioning.ExpirationMs = body.ExpirationMs.Value
	}
	return partitioning, nil
}

// rangeLayout validates integer-range partitioning.
func rangeLayout(body rangePartitioningBody, fields []map[string]any) (*rangePartitioning, error) {
	column := columnOf(fields, body.Field)
	if column == nil {
		return nil, failure(codes.InvalidArgument, reasonInvalid, "The field specified for partitioning cannot be found in the schema.")
	}
	if kind := fieldType(column); kind != sqlInt64 {
		return nil, failure(codes.InvalidArgument, reasonInvalid,
			"The field specified for integer range partitioning can only be of type INT64. The type found is: "+kind+".")
	}
	bounds := body.Range
	if bounds.Interval.Value <= 0 {
		return nil, failure(codes.InvalidArgument, reasonInvalid, "Interval must be larger than 0 for range partition.")
	}
	if bounds.End.Value <= bounds.Start.Value {
		// Not recorded.
		return nil, failure(codes.InvalidArgument, reasonInvalid, "End must be larger than start for range partition.")
	}
	return &rangePartitioning{Field: fieldName(column), Start: bounds.Start.Value, End: bounds.End.Value, Interval: bounds.Interval.Value}, nil
}

// clusteringLayout validates clustering columns: each once, each in the
// schema, at most four (recorded, in this order).
func clusteringLayout(names []string, fields []map[string]any) ([]string, error) {
	var clustering []string
	for _, name := range names {
		if slices.ContainsFunc(clustering, func(seen string) bool { return strings.EqualFold(seen, name) }) {
			return nil, failure(codes.InvalidArgument, reasonInvalid, "Field can only appear once in clustering fields. Invalid field: "+name+".")
		}
		column := columnOf(fields, name)
		if column == nil {
			return nil, failure(codes.InvalidArgument, reasonInvalid,
				"The field specified for clustering cannot be found in the schema. Invalid field: "+name+".")
		}
		clustering = append(clustering, fieldName(column))
	}
	if len(clustering) > maxClusteringFields {
		return nil, failure(codes.InvalidArgument, reasonInvalid,
			fmt.Sprintf("%d clustering fields specified, exceeding the limit of %d.", len(clustering), maxClusteringFields))
	}
	return clustering, nil
}

// expirationOf reads an expirationTime in milliseconds; GCP refuses one less
// than a second away (recorded).
func expirationOf(millis int64, now time.Time) (time.Time, error) {
	expiration := time.UnixMilli(millis)
	if expiration.Before(now.Add(time.Second)) {
		return time.Time{}, failure(codes.InvalidArgument, reasonInvalid, "Expiration time must be at least 1 seconds in the future")
	}
	return expiration, nil
}

// applyDefaults gives a new table its dataset's default expirations: a
// time-partitioned table the default partition expiration, and any other
// table, or a time-partitioned one when the dataset sets no partition
// default, the default table expiration (recorded: a partitioned table
// under both defaults gets no expirationTime).
func (l *layout) applyDefaults(dataset *datasetRecord, created time.Time) {
	partitioned := l.time != nil && dataset.DefaultPartitionExpirationMs > 0
	if partitioned && l.time.ExpirationMs == 0 {
		l.time.ExpirationMs = dataset.DefaultPartitionExpirationMs
	}
	if l.expiration.IsZero() && dataset.DefaultTableExpirationMs > 0 && !partitioned {
		l.expiration = created.Add(time.Duration(dataset.DefaultTableExpirationMs) * time.Millisecond)
	}
}

// store copies a layout onto a table record.
func (l *layout) store(record *tableRecord) {
	record.TimePartitioning, record.RangePartitioning = l.time, l.ranged
	record.Clustering, record.RequirePartitionFilter, record.Expiration = l.clustering, l.filter, l.expiration
}

// patchLayout validates a tables.patch body's layout changes against the
// record: clustering, the filter requirement, partition expiration, and
// expirationTime change; the partitioning itself cannot (recorded).
func patchLayout(body tableBody, record *tableRecord, now time.Time) (layout, error) {
	result := layout{time: record.TimePartitioning, ranged: record.RangePartitioning, clustering: record.Clustering,
		filter: record.RequirePartitionFilter, expiration: record.Expiration}
	fields := schemaFields(record.Schema)
	var err error
	if result.time, err = patchTime(body.TimePartitioning, record.TimePartitioning, fields); err != nil {
		return result, err
	}
	if err := patchRange(body.RangePartitioning, record.RangePartitioning, fields); err != nil {
		return result, err
	}
	if body.Clustering.Set {
		if result.clustering, err = clusteringLayout(body.Clustering.Fields, fields); err != nil {
			return result, err
		}
	}
	if filter := body.requireFilter(); filter != nil {
		result.filter = filter
	}
	if expiration := body.ExpirationTime; expiration.Set {
		result.expiration = time.Time{}
		if expiration.present() {
			result.expiration, err = expirationOf(expiration.Value, now)
		}
	}
	return result, err
}

// patchTime validates a patch's time partitioning: only its expiration may
// change.
func patchTime(sent *timePartitioningBody, current *timePartitioning, fields []map[string]any) (*timePartitioning, error) {
	if sent == nil {
		return current, nil
	}
	if current == nil || !strings.EqualFold(cmpType(sent.Type), current.Type) || !strings.EqualFold(sent.Field, current.Field) {
		return nil, failure(codes.InvalidArgument, reasonInvalid, specFrozen)
	}
	changed, err := timeLayout(*sent, fields)
	if err != nil {
		return nil, err
	}
	if !sent.ExpirationMs.Set {
		changed.ExpirationMs = current.ExpirationMs
	}
	return changed, nil
}

// patchRange accepts a patch's range partitioning only unchanged.
func patchRange(sent *rangePartitioningBody, current *rangePartitioning, fields []map[string]any) error {
	if sent == nil {
		return nil
	}
	changed, err := rangeLayout(*sent, fields)
	if err != nil {
		return err
	}
	if current == nil || *changed != *current {
		return failure(codes.InvalidArgument, reasonInvalid, specFrozen)
	}
	return nil
}

// cmpType is a sent partitioning type, DAY when it is empty.
func cmpType(kind string) string {
	if kind == "" {
		return partitionDay
	}
	return kind
}

// layoutWire adds a table's layout to its document. echoFilter repeats
// requirePartitionFilter inside timePartitioning, as GCP's tables.get and
// tables.patch do and tables.insert and tables.list do not (recorded).
func layoutWire(document map[string]any, record *tableRecord, echoFilter bool) {
	if partitioning := record.TimePartitioning; partitioning != nil {
		document["timePartitioning"] = timeWire(partitioning, echoFilter && record.RequirePartitionFilter != nil && *record.RequirePartitionFilter)
	}
	if partitioning := record.RangePartitioning; partitioning != nil {
		document["rangePartitioning"] = map[string]any{"field": partitioning.Field, "range": map[string]any{
			"start": strconv.FormatInt(partitioning.Start, 10), "end": strconv.FormatInt(partitioning.End, 10),
			"interval": strconv.FormatInt(partitioning.Interval, 10),
		}}
	}
	if len(record.Clustering) > 0 {
		document["clustering"] = map[string]any{keyFields: slices.Clone(record.Clustering)}
	}
	if record.RequirePartitionFilter != nil {
		document["requirePartitionFilter"] = *record.RequirePartitionFilter
	}
	if !record.Expiration.IsZero() {
		document["expirationTime"] = strconv.FormatInt(record.Expiration.UnixMilli(), 10)
	}
}

// timeWire renders TimePartitioning, with requirePartitionFilter when
// filter is set.
func timeWire(partitioning *timePartitioning, filter bool) map[string]any {
	wire := map[string]any{keyType: partitioning.Type}
	if partitioning.Field != "" {
		wire["field"] = partitioning.Field
	}
	if partitioning.ExpirationMs > 0 {
		wire["expirationMs"] = strconv.FormatInt(partitioning.ExpirationMs, 10)
	}
	if filter {
		wire["requirePartitionFilter"] = true
	}
	return wire
}

// partitionColumn names the column a table is partitioned on, "" for an
// ingestion-time or unpartitioned table.
func (t *tableRecord) partitionColumn() string {
	switch {
	case t.RangePartitioning != nil:
		return t.RangePartitioning.Field
	case t.TimePartitioning != nil:
		return t.TimePartitioning.Field
	}
	return ""
}

// ingestionTime reports an ingestion-time partitioned table.
func (t *tableRecord) ingestionTime() bool {
	return t.TimePartitioning != nil && t.TimePartitioning.Field == ""
}

// partitionDefinition is the documented PartitioningDefinition GCP reports
// for a column-partitioned table (its presence is recorded).
func partitionDefinition(document map[string]any, record *tableRecord) {
	if column := record.partitionColumn(); column != "" {
		document["partitionDefinition"] = map[string]any{"partitionedColumn": []any{map[string]any{"field": column}}}
	}
}

// pseudoDefinition is the hidden column of an ingestion-time table: each
// row's partition time, the insert time truncated to the partition unit, as
// GCP assigns it to rows written by DML (recorded). Streamed rows get it too;
// GCP leaves them NULL while they sit in the streaming buffer.
func pseudoDefinition(partitioning *timePartitioning) string {
	return pseudoColumn + " TIMESTAMP DEFAULT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), " + partitioning.Type + ")"
}

// requiredFilterColumns are the columns a query must filter on to read a
// table with requirePartitionFilter, as GCP's refusal names them.
func (t *tableRecord) requiredFilterColumns() []string {
	if column := t.partitionColumn(); column != "" {
		return []string{column}
	}
	// Not recorded: GCP's documented pseudo-columns of an ingestion-time table.
	return []string{"_PARTITION_LOAD_TIME", namePartitionDate, namePartitionTime}
}

// filterRequired is GCP's refusal of a query that reads a table with
// requirePartitionFilter without filtering on its partitioning (recorded).
func filterRequired(record *tableRecord) error {
	columns := make([]string, 0, 3)
	for _, column := range record.requiredFilterColumns() {
		columns = append(columns, "'"+column+"'")
	}
	return invalidQuery(fmt.Sprintf("Cannot query over table '%s.%s' without a filter over column(s) %s that can be used for partition elimination",
		record.Dataset, record.ID, strings.Join(columns, ", ")))
}

// partitionStart is the engine expression for a row's partition start, as a
// TIMESTAMP, for a table whose partitions expire.
func partitionStart(record *tableRecord, fields []map[string]any) string {
	partitioning := record.TimePartitioning
	if partitioning.Field == "" {
		return pseudoColumn
	}
	column := "`" + partitioning.Field + "`"
	switch fieldType(columnOf(fields, partitioning.Field)) {
	case sqlDate:
		return "TIMESTAMP(DATE_TRUNC(" + column + ", " + partitioning.Type + "))"
	case sqlDatetime:
		return "TIMESTAMP(DATETIME_TRUNC(" + column + ", " + partitioning.Type + "))"
	default:
		return "TIMESTAMP_TRUNC(" + column + ", " + partitioning.Type + ")"
	}
}

// expirePartitions deletes the rows of each table's expired partitions: a
// partition expires its expirationMs after its start, in UTC. GCP stops
// reading an expired partition at once, even one a statement just wrote
// (recorded), and deletes it later; the provider deletes it before the next
// read. Rows without a partition (NULL) never expire. The caller holds the
// engine gate.
func (p *Provider) expirePartitions(ctx context.Context, engine *engineBackend, scope state.Scope, tables []tableName) error {
	now := p.clock()
	for _, name := range tables {
		record := p.tableCopy(scope, name.dataset, name.table)
		if record == nil || record.TimePartitioning == nil || record.TimePartitioning.ExpirationMs == 0 {
			continue
		}
		cutoff := now.Add(-time.Duration(record.TimePartitioning.ExpirationMs) * time.Millisecond).UnixMicro()
		statement := "DELETE FROM " + canonicalPath(scope.Project, name.dataset, name.table) + " WHERE " +
			partitionStart(record, schemaFields(record.Schema)) + " <= TIMESTAMP_MICROS(" + strconv.FormatInt(cutoff, 10) + ")"
		if _, err := engine.exec(ctx, statement, nil); err != nil {
			return err
		}
	}
	return nil
}

// expired reports a table past its expirationTime; GCP deletes it then.
func (t *tableRecord) expired(now time.Time) bool {
	return !t.Expiration.IsZero() && !now.Before(t.Expiration)
}

// dropExpired drops every table of the scope past its expirationTime, from
// the engine and then its metadata. Reads already treat such a table as
// gone; this frees it. The caller holds the engine gate.
func (p *Provider) dropExpired(ctx context.Context, engine *engineBackend, scope state.Scope) error {
	now := p.clock()
	var expired []*tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.tables {
			if record.expired(now) {
				expired = append(expired, record.clone())
			}
		}
		return nil
	})
	return p.dropTables(ctx, engine, scope, expired)
}
