package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CommitSchema checks a new revision against the latest one. The rules and
// messages below are the ones GCP returned in the pubsub-schemas recording
// and in probes of the sandbox on 2026-10-12; a change neither covers is a
// coverage gap rather than a guess. The emulator checks nothing.

// incompatible renders GCP's FAILED_PRECONDITION for an incompatible revision.
func incompatible(name, previous, reason string) error {
	return status.Errorf(codes.FailedPrecondition, `Compatibility checking failed to commit a schema revision for (schema=%q). (reason="Revision is incompatible with previous revision: %s. Failed with error: %s). Refer to https://cloud.google.com/pubsub/docs/schemas for more information.`, name, previous, reason)
}

// checkCompatible compares a revision to commit with the latest revision.
func checkCompatible(ctx context.Context, name string, latest, next *pb.Schema) error {
	if latest.GetType() != next.GetType() {
		return incompatible(name, latest.GetRevisionId(), "Types do not match")
	}
	var reason string
	var err error
	if next.GetType() == pb.Schema_PROTOCOL_BUFFER {
		reason, err = protoCompatibility(ctx, latest.GetDefinition(), next.GetDefinition())
	} else {
		reason, err = avroCompatibility(latest.GetDefinition(), next.GetDefinition())
	}
	if err != nil || reason == "" {
		return err
	}
	return incompatible(name, latest.GetRevisionId(), reason)
}

// avroCompatibility compares two Avro records field by field: a record keeps
// its name (or lists the old one as an alias), a field present in only one
// revision has a default, and a shared field keeps its primitive type.
func avroCompatibility(previous, next string) (string, error) {
	before, beforeOK := avroRecord(previous)
	after, afterOK := avroRecord(next)
	if !beforeOK || !afterOK {
		if canonicalJSON(previous) == canonicalJSON(next) && canonicalJSON(next) != "" {
			return "", nil
		}
		return "", gap("compatibility of Avro revisions that are not records")
	}
	if before.fullName() != after.fullName() && !slices.Contains(after.Aliases, before.Name) && !slices.Contains(after.Aliases, before.fullName()) {
		return "Type name " + before.fullName() + " does not match or has no alias for " + after.fullName(), nil
	}
	return avroFieldReason(before, after)
}

// avroFieldReason compares the fields of two records with the same name.
func avroFieldReason(before, after avroRecordSchema) (string, error) {
	for _, field := range after.Fields {
		if reason, err := avroFieldChange(before, field); reason != "" || err != nil {
			return reason, err
		}
	}
	for _, field := range before.Fields {
		if _, found := after.field(field.Name); !found && !field.hasDefault {
			return "Presence of non-optional field " + field.Name + " is inconsistent.", nil
		}
	}
	return "", nil
}

// avroFieldChange checks one field of the new record against the old record.
func avroFieldChange(before avroRecordSchema, field avroFieldSchema) (string, error) {
	old, found := before.field(field.Name)
	switch {
	case !found && !field.hasDefault:
		return "Presence of non-optional field " + field.Name + " is inconsistent.", nil
	case !found || reflect.DeepEqual(old.Type, field.Type):
		return "", nil
	case avroPrimitive(old.Type) && avroPrimitive(field.Type):
		return "Field " + field.Name + " has type mismatch: Types do not match.", nil
	}
	return "", gap("compatibility of changed nested Avro types")
}

// avroRecordSchema is the part of an Avro record the compatibility rules read.
type avroRecordSchema struct {
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Aliases   []string          `json:"aliases"`
	Fields    []avroFieldSchema `json:"fields"`
}

// avroFieldSchema is one record field. A declared default, even null, makes
// the field optional.
type avroFieldSchema struct {
	Name       string
	Type       any
	hasDefault bool
}

// UnmarshalJSON records whether the field declares a default.
func (f *avroFieldSchema) UnmarshalJSON(raw []byte) error {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return fmt.Errorf("decode an Avro field: %w", err)
	}
	if err := json.Unmarshal(members["name"], &f.Name); err != nil {
		return fmt.Errorf("decode an Avro field name: %w", err)
	}
	if err := json.Unmarshal(members["type"], &f.Type); err != nil {
		return fmt.Errorf("decode an Avro field type: %w", err)
	}
	_, f.hasDefault = members["default"]
	return nil
}

// fullName renders the record name as GCP's messages do (".Event" without a namespace).
func (r avroRecordSchema) fullName() string {
	if strings.Contains(r.Name, ".") {
		return r.Name
	}
	return r.Namespace + "." + r.Name
}

func (r avroRecordSchema) field(name string) (avroFieldSchema, bool) {
	for _, field := range r.Fields {
		if field.Name == name {
			return field, true
		}
	}
	return avroFieldSchema{}, false
}

// avroRecord decodes a top-level Avro record definition.
func avroRecord(definition string) (avroRecordSchema, bool) {
	var record avroRecordSchema
	if json.Unmarshal([]byte(definition), &record) != nil || record.Type != "record" || record.Name == "" {
		return avroRecordSchema{}, false
	}
	return record, true
}

// avroPrimitive reports a primitive type written as a bare name.
func avroPrimitive(value any) bool {
	name, ok := value.(string)
	return ok && slices.Contains([]string{"null", "boolean", "int", "long", "float", "double", "bytes", "string"}, name)
}

// canonicalJSON re-encodes JSON so formatting does not affect comparison.
func canonicalJSON(definition string) string {
	var value any
	if json.Unmarshal([]byte(definition), &value) != nil {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// protoCompatibility compares the top-level messages of two Protocol Buffer
// definitions. Renaming the message and removing optional fields are allowed.
func protoCompatibility(ctx context.Context, previous, next string) (string, error) {
	before, err := compileProtoSchema(ctx, previous)
	if err != nil {
		return "", err
	}
	after, err := compileProtoSchema(ctx, next)
	if err != nil {
		return "", err
	}
	return compareMessages(before, after)
}

// compareMessages checks each field of the new message against the old one.
func compareMessages(before, after protoreflect.MessageDescriptor) (string, error) {
	for i := range after.Fields().Len() {
		field := after.Fields().Get(i)
		old := before.Fields().ByNumber(field.Number())
		if renamed := before.Fields().ByName(field.Name()); renamed != nil && renamed.Number() != field.Number() {
			return fmt.Sprintf("%s with tag %d has the same name as %s with tag %d", label(field), field.Number(), label(renamed), renamed.Number()), nil
		}
		if old == nil && field.Cardinality() == protoreflect.Required {
			return "", gap("compatibility of an added required Protocol Buffer field")
		}
		if old == nil {
			continue
		}
		if reason, err := compareFields(old, field); reason != "" || err != nil {
			return reason, err
		}
	}
	return "", removedRequired(before, after)
}

// removedRequired refuses a revision that drops a required field.
func removedRequired(before, after protoreflect.MessageDescriptor) error {
	for i := range before.Fields().Len() {
		if old := before.Fields().Get(i); old.Cardinality() == protoreflect.Required && after.Fields().ByNumber(old.Number()) == nil {
			return gap("compatibility of a removed required Protocol Buffer field")
		}
	}
	return nil
}

// compareFields checks one field kept at the same tag.
func compareFields(old, field protoreflect.FieldDescriptor) (string, error) {
	if old.Name() != field.Name() {
		return fmt.Sprintf("%s has the same tag (%d) but different name than %s", label(field), field.Number(), label(old)), nil
	}
	if reason, err := compareCardinality(old, field); reason != "" || err != nil {
		return reason, err
	}
	return compareKinds(old, field)
}

// compareCardinality checks repeated and required labels.
func compareCardinality(old, field protoreflect.FieldDescriptor) (string, error) {
	repeated, wasRepeated := field.Cardinality() == protoreflect.Repeated, old.Cardinality() == protoreflect.Repeated
	required, wasRequired := field.Cardinality() == protoreflect.Required, old.Cardinality() == protoreflect.Required
	switch {
	case repeated && !wasRepeated:
		return label(field) + " is repeated but " + label(old) + " is not", nil
	case required && !wasRequired:
		return label(field) + " is a required field but " + label(old) + " is not", nil
	case wasRepeated && !repeated:
		return "", gap("compatibility of a repeated Protocol Buffer field made singular")
	case wasRequired && !required:
		return "", gap("compatibility of a required Protocol Buffer field made optional")
	}
	return "", nil
}

// compareKinds checks a field's type. Nested messages are compared
// recursively; scalar types must match exactly.
func compareKinds(old, field protoreflect.FieldDescriptor) (string, error) {
	if old.Kind() == protoreflect.MessageKind && field.Kind() == protoreflect.MessageKind {
		return compareMessages(old.Message(), field.Message())
	}
	if old.Kind() == protoreflect.EnumKind && field.Kind() == protoreflect.EnumKind && sameEnum(old.Enum(), field.Enum()) {
		return "", nil
	}
	if !scalar(old) || !scalar(field) {
		return "", gap("compatibility of changed Protocol Buffer enum or message fields")
	}
	if old.Kind() != field.Kind() {
		return fmt.Sprintf("%s of type %s is not compatible with %s of type %s", label(field), field.Kind(), label(old), old.Kind()), nil
	}
	return "", nil
}

// scalar reports a field that is neither a message, a group, nor an enum.
func scalar(field protoreflect.FieldDescriptor) bool {
	kind := field.Kind()
	return kind != protoreflect.MessageKind && kind != protoreflect.GroupKind && kind != protoreflect.EnumKind
}

// sameEnum reports enums with identical value names and numbers.
func sameEnum(a, b protoreflect.EnumDescriptor) bool {
	if a.Values().Len() != b.Values().Len() {
		return false
	}
	for i := range a.Values().Len() {
		value := a.Values().Get(i)
		if other := b.Values().ByName(value.Name()); other == nil || other.Number() != value.Number() {
			return false
		}
	}
	return true
}

// label names a field as GCP's messages do: Message.field.
func label(field protoreflect.FieldDescriptor) string {
	return string(field.ContainingMessage().Name()) + "." + string(field.Name())
}
