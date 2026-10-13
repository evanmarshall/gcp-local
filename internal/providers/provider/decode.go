package provider

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// DecodeProto strictly decodes a REST request body into message the way
// GCP's JSON transcoder does. root is the proto name of the request field the
// body binds ("job" for a body: "job" route, "" for body: "*"); it prefixes
// every reported path.
//
//   - A name the message schema doesn't have is INVALID_ARGUMENT
//     `Invalid JSON payload received. Unknown name "x" at 'job.pubsub_target': Cannot find field.`
//     with a BadRequest violation on the enclosing path.
//   - A value of the wrong JSON type is INVALID_ARGUMENT
//     `Invalid value at 'job.retry_config.retry_count' (TYPE_INT32), "many"`
//     with a BadRequest violation on its path.
//   - A schema field the provider doesn't model is named in unmodeled by its
//     dotted JSON path ("appEngineHttpTarget") and refused with gap, naming
//     it, unless its value is null or the type's default.
//
// An empty body decodes as {}. Output-only fields decode normally; the
// provider overwrites them, as GCP ignores them on input.
func DecodeProto(body []byte, root string, message proto.Message, gap func(string) error, unmodeled ...string) error {
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	walk := protoWalk{gap: gap, unmodeled: names(unmodeled)}
	if err := walk.message(body, message.ProtoReflect(), root, ""); err != nil {
		return err
	}
	if err := protojson.Unmarshal(body, message); err != nil {
		return FieldViolation(root, "Invalid JSON payload received. "+err.Error())
	}
	return nil
}

// DecodePolicy strictly decodes a setIamPolicy body, a
// google.iam.v1.SetIamPolicyRequest, and returns its policy. Audit configs
// and an updateMask naming anything but bindings and etag (the default
// mask) are refused with gap.
func DecodePolicy(body []byte, gap func(string) error) (iam.Policy, error) {
	var request iampb.SetIamPolicyRequest
	if err := DecodeProto(body, "", &request, gap, "policy.auditConfigs"); err != nil {
		return iam.Policy{}, err
	}
	for _, path := range request.GetUpdateMask().GetPaths() {
		if path != "bindings" && path != "etag" {
			return iam.Policy{}, gap("setIamPolicy updateMask " + path + " is not modeled")
		}
	}
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return envelope.Policy, nil
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return iam.Policy{}, FieldViolation("policy", "Invalid JSON payload received. "+err.Error())
	}
	return envelope.Policy, nil
}

// Unmodeled lists, for DecodeProto, the dotted JSON paths of message's input
// fields that modeled doesn't cover: every field not marked OUTPUT_ONLY
// (google.api.field_behavior) that is neither a modeled path nor the parent
// message of one. A modeled message path covers its whole subtree. Deriving
// the list from the descriptor makes a field a later proto adds a gap, never
// silently accepted.
func Unmodeled(message proto.Message, modeled ...string) []string {
	covered := names(modeled)
	parents := map[string]bool{}
	for _, path := range modeled {
		for i := strings.LastIndex(path, "."); i > 0; i = strings.LastIndex(path[:i], ".") {
			parents[path[:i]] = true
		}
	}
	var paths []string
	var walk func(protoreflect.MessageDescriptor, string)
	walk = func(descriptor protoreflect.MessageDescriptor, prefix string) {
		fields := descriptor.Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			path := join(prefix, field.JSONName())
			switch {
			case covered[path] || outputOnly(field):
			case parents[path] && field.Message() != nil && !field.IsMap():
				walk(field.Message(), path)
			default:
				paths = append(paths, path)
			}
		}
	}
	walk(message.ProtoReflect().Descriptor(), "")
	return paths
}

// outputOnly reports a field GCP's schema marks OUTPUT_ONLY.
func outputOnly(field protoreflect.FieldDescriptor) bool {
	behaviors, _ := proto.GetExtension(field.Options(), annotations.E_FieldBehavior).([]annotations.FieldBehavior)
	return slices.Contains(behaviors, annotations.FieldBehavior_OUTPUT_ONLY)
}

// protoWalk checks a JSON document against a message descriptor before
// protojson decodes it, so every refusal names its path.
type protoWalk struct {
	gap       func(string) error
	unmodeled map[string]bool
}

// message checks one JSON object against a message. path is the proto path
// GCP reports; jsonPath is the dotted JSON path unmodeled names use.
func (w protoWalk) message(raw json.RawMessage, message protoreflect.Message, path, jsonPath string) error {
	descriptor := message.Descriptor()
	typeURL := "type.googleapis.com/" + string(descriptor.FullName())
	if wellKnown(descriptor) {
		// Durations, timestamps, field masks, structs, and wrappers have
		// their own JSON forms; protojson knows them.
		if protojson.Unmarshal(raw, message.Interface()) == nil {
			return nil
		}
		if kind, ok := wrapped(descriptor); ok {
			// GCP names a wrapper's value field and omits the value:
			// "Invalid value at 'plaintext_crc32c.value' (TYPE_INT64), "
			// (recording cloudkms-symmetric-key-lifecycle, Int64Value).
			inner := join(path, "value")
			return FieldViolation(inner, fmt.Sprintf("Invalid value at '%s' (%s), ", inner, kind))
		}
		return mistyped(path, typeURL, raw)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return mistyped(path, typeURL, raw)
	}
	for _, key := range sortedKeys(fields) {
		if err := w.member(key, fields[key], message, path, jsonPath); err != nil {
			return err
		}
	}
	return nil
}

// member checks one key of a message object.
func (w protoWalk) member(key string, raw json.RawMessage, message protoreflect.Message, path, jsonPath string) error {
	fields := message.Descriptor().Fields()
	field := fields.ByJSONName(key)
	if field == nil {
		field = fields.ByName(protoreflect.Name(key))
	}
	if field == nil {
		return unknownName(key, path)
	}
	childJSON := join(jsonPath, field.JSONName())
	if w.unmodeled[childJSON] && !emptyJSON(raw) {
		return w.gap(childJSON + " is not modeled")
	}
	if isNull(raw) {
		return nil
	}
	if listForSingular(field, raw) {
		// Recorded: serviceaccount-lifecycle-authorization "mistyped-field".
		return refusedName(key, path, "Proto field is not repeating, cannot start list.")
	}
	childPath := join(path, string(field.Name()))
	switch {
	case field.IsList():
		fresh := func() protoreflect.Message { return message.NewField(field).List().NewElement().Message() }
		return eachItem(raw, childPath, kindName(field), func(item json.RawMessage, itemPath string) error {
			return w.single(item, field, fresh, itemPath, childJSON)
		})
	case field.IsMap():
		fresh := func() protoreflect.Message { return message.NewField(field).Map().NewValue().Message() }
		return eachEntry(raw, childPath, kindName(field), func(value json.RawMessage, valuePath string) error {
			return w.single(value, field.MapValue(), fresh, valuePath, childJSON)
		})
	default:
		return w.single(raw, field, func() protoreflect.Message { return message.NewField(field).Message() }, childPath, childJSON)
	}
}

// listForSingular reports a JSON array for a field that isn't repeated, a
// map, or a well-known type with its own JSON form (Value, ListValue).
func listForSingular(field protoreflect.FieldDescriptor, raw json.RawMessage) bool {
	if field.IsList() || field.IsMap() || field.Message() != nil && wellKnown(field.Message()) {
		return false
	}
	return bytes.HasPrefix(bytes.TrimSpace(raw), []byte("["))
}

// eachItem checks a JSON array item by item; items are addressed path[i].
func eachItem(raw json.RawMessage, path, kind string, check func(json.RawMessage, string) error) error {
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return mistyped(path, kind, raw)
	}
	for i, item := range items {
		if err := check(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

// eachEntry checks a JSON object's values; entries are addressed path[key].
func eachEntry(raw json.RawMessage, path, kind string, check func(json.RawMessage, string) error) error {
	var entries map[string]json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		return mistyped(path, kind, raw)
	}
	for _, key := range sortedKeys(entries) {
		if err := check(entries[key], path+"["+key+"]"); err != nil {
			return err
		}
	}
	return nil
}

// single checks one value of field's kind; fresh builds an empty message of
// the field's message type.
func (w protoWalk) single(raw json.RawMessage, field protoreflect.FieldDescriptor, fresh func() protoreflect.Message, path, jsonPath string) error {
	if isNull(raw) {
		return nil
	}
	if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
		return w.message(raw, fresh(), path, jsonPath)
	}
	if !scalarFits(raw, field) {
		kind := kindName(field)
		if field.Kind() == protoreflect.EnumKind {
			kind = "type.googleapis.com/" + string(field.Enum().FullName())
		}
		return mistyped(path, kind, raw)
	}
	return nil
}

// scalarFits reports whether a JSON value is a valid proto JSON value of a
// scalar field: integers and floats as numbers or decimal strings, enums by
// name or number.
func scalarFits(raw json.RawMessage, field protoreflect.FieldDescriptor) bool {
	text := strings.Trim(string(raw), `"`)
	switch field.Kind() {
	case protoreflect.StringKind, protoreflect.BytesKind:
		return textFits(raw, field.Kind())
	case protoreflect.BoolKind:
		return isBool(raw)
	case protoreflect.EnumKind:
		return enumFits(raw, field.Enum())
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		_, err := strconv.ParseFloat(text, 64)
		return err == nil || text == "NaN" || text == "Infinity" || text == "-Infinity"
	default:
		_, signed := strconv.ParseInt(text, 10, 64)
		_, unsigned := strconv.ParseUint(text, 10, 64)
		return signed == nil || unsigned == nil
	}
}

// textFits reports a JSON string, which for bytes must be standard or
// URL-safe base64, padded or not, as proto JSON accepts.
func textFits(raw json.RawMessage, kind protoreflect.Kind) bool {
	if raw[0] != '"' {
		return false
	}
	if kind != protoreflect.BytesKind {
		return true
	}
	text := strings.Trim(string(raw), `"`)
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		if _, err := encoding.DecodeString(text); err == nil {
			return true
		}
	}
	return false
}

// enumFits reports an enum value given by name or number.
func enumFits(raw json.RawMessage, enum protoreflect.EnumDescriptor) bool {
	if raw[0] == '"' {
		return enum.Values().ByName(protoreflect.Name(strings.Trim(string(raw), `"`))) != nil
	}
	_, err := strconv.ParseInt(string(raw), 10, 32)
	return err == nil
}

// kindName spells a scalar kind the way GCP's errors do ("TYPE_INT32").
func kindName(field protoreflect.FieldDescriptor) string {
	if field.Kind() == protoreflect.MessageKind {
		return "type.googleapis.com/" + string(field.Message().FullName())
	}
	return "TYPE_" + strings.ToUpper(field.Kind().String())
}

// wrapped reports a google.protobuf wrapper type (Int64Value, StringValue,
// ...) and its value field's kind name.
func wrapped(descriptor protoreflect.MessageDescriptor) (string, bool) {
	if !strings.HasSuffix(string(descriptor.Name()), "Value") || descriptor.Fields().Len() != 1 {
		return "", false
	}
	value := descriptor.Fields().ByName("value")
	if value == nil || value.Kind() == protoreflect.MessageKind || value.IsList() {
		return "", false
	}
	return kindName(value), true
}

// wellKnown reports a google.protobuf type with a special JSON form.
func wellKnown(descriptor protoreflect.MessageDescriptor) bool {
	return descriptor.ParentFile().Package() == "google.protobuf"
}

// Fields names the schema fields a DecodeJSON struct lacks, by dotted JSON
// path ("settings.backupConfiguration"): Unmodeled ones are refused with the
// gap when set to a non-default value; OutputOnly ones are accepted and
// dropped, as GCP ignores output-only fields on input. List every field of
// the method's discovery schema that the struct doesn't have.
type Fields struct {
	Unmodeled, OutputOnly []string
	// IgnoreUnknown drops names the schema doesn't have instead of refusing
	// them, as Cloud SQL Admin v1beta4 does (recorded,
	// cloudsql-postgres-instance-admin). Schema fields stay strict.
	IgnoreUnknown bool
}

// DecodeJSON strictly decodes a JSON object request body into into, a
// pointer to a struct whose json tags are the fields the provider models.
// root is the request field the body binds ("instance"), as in DecodeProto.
//
//   - A field in fields.Unmodeled is refused with gap, naming its path, unless
//     its value is null or the type's default ("", 0, false, {}, []), which
//     proto3 treats as unset. A field in fields.OutputOnly is dropped.
//   - Any other name the struct doesn't have is GCP's 400 for an unknown
//     name, with a BadRequest violation, or dropped with IgnoreUnknown.
//   - A value of the wrong JSON type is INVALID_ARGUMENT with a BadRequest
//     violation on its JSON path.
//
// json.RawMessage and interface fields accept any value. An empty body
// decodes as {}. Prefer DecodeProto when a proto message describes the body.
func DecodeJSON(body []byte, root string, into any, gap func(string) error, fields Fields) error {
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	walk := jsonWalk{gap: gap, unmodeled: names(fields.Unmodeled), output: names(fields.OutputOnly), ignore: fields.IgnoreUnknown}
	if err := walk.value(body, reflect.TypeOf(into).Elem(), root, ""); err != nil {
		return err
	}
	if err := json.Unmarshal(body, into); err != nil {
		return FieldViolation(root, "Invalid JSON payload received. "+err.Error())
	}
	return nil
}

// jsonWalk checks a JSON document against a Go type before json decodes it.
type jsonWalk struct {
	gap               func(string) error
	unmodeled, output map[string]bool
	ignore            bool
}

// Types that decode JSON values themselves.
var (
	rawMessage  = reflect.TypeFor[json.RawMessage]()
	number      = reflect.TypeFor[json.Number]()
	unmarshaler = reflect.TypeFor[json.Unmarshaler]()
)

// value checks one JSON value against a Go type. A type with its own JSON
// decoding reports its own errors. path is the reported path (root
// included); jsonPath is the dotted JSON path Fields names.
func (w jsonWalk) value(raw json.RawMessage, kind reflect.Type, path, jsonPath string) error {
	if isNull(raw) || decodesItself(kind) {
		return nil
	}
	switch kind.Kind() {
	case reflect.Pointer:
		return w.value(raw, kind.Elem(), path, jsonPath)
	case reflect.Struct:
		return w.object(raw, kind, path, jsonPath)
	case reflect.Slice, reflect.Array:
		return eachItem(raw, path, "TYPE_LIST", func(item json.RawMessage, itemPath string) error {
			return w.value(item, kind.Elem(), itemPath, jsonPath)
		})
	case reflect.Map:
		return eachEntry(raw, path, "TYPE_MAP", func(value json.RawMessage, valuePath string) error {
			return w.value(value, kind.Elem(), valuePath, jsonPath)
		})
	default:
		if !goScalarFits(raw, kind) {
			return mistyped(path, "TYPE_"+strings.ToUpper(kind.Kind().String()), raw)
		}
		return nil
	}
}

// decodesItself reports a type that accepts any JSON value or decodes its own.
func decodesItself(kind reflect.Type) bool {
	return kind == rawMessage || kind.Kind() == reflect.Interface || reflect.PointerTo(kind).Implements(unmarshaler)
}

// goScalarFits reports whether a JSON value decodes into a scalar Go type.
func goScalarFits(raw json.RawMessage, kind reflect.Type) bool {
	switch {
	case kind == number:
		return json.Unmarshal(raw, new(json.Number)) == nil
	case kind.Kind() == reflect.String:
		return raw[0] == '"'
	case kind.Kind() == reflect.Bool:
		return isBool(raw)
	default:
		return json.Unmarshal(raw, reflect.New(kind).Interface()) == nil
	}
}

// object checks a JSON object against a struct's json-tagged fields.
func (w jsonWalk) object(raw json.RawMessage, kind reflect.Type, path, jsonPath string) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return mistyped(path, "TYPE_MESSAGE", raw)
	}
	known := jsonFields(kind)
	for _, key := range sortedKeys(fields) {
		if err := w.member(key, fields[key], known, path, jsonPath); err != nil {
			return err
		}
	}
	return nil
}

// member checks one key of a JSON object against the struct's fields.
func (w jsonWalk) member(key string, raw json.RawMessage, known map[string]reflect.StructField, path, jsonPath string) error {
	child := join(jsonPath, key)
	field, found := known[key]
	switch {
	case found && !tagged(field, "string"): // a ",string" number reports its own errors
		return w.value(raw, field.Type, join(path, key), child)
	case found, w.output[child], w.ignore && !w.unmodeled[child]:
		return nil
	case w.unmodeled[child]:
		if !emptyJSON(raw) {
			return w.gap(child + " is not modeled")
		}
		return nil
	default:
		return unknownName(key, path)
	}
}

// jsonFields maps each JSON name to its struct field, as encoding/json
// resolves them (embedded structs promote their fields).
func jsonFields(kind reflect.Type) map[string]reflect.StructField {
	fields := map[string]reflect.StructField{}
	for _, field := range reflect.VisibleFields(kind) {
		if !field.IsExported() || field.Anonymous && field.Tag.Get("json") == "" {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		switch name {
		case "-":
			continue
		case "":
			name = field.Name
		}
		if _, taken := fields[name]; !taken || len(field.Index) == 1 {
			fields[name] = field
		}
	}
	return fields
}

// tagged reports a json tag option.
func tagged(field reflect.StructField, option string) bool {
	_, options, _ := strings.Cut(field.Tag.Get("json"), ",")
	for item := range strings.SplitSeq(options, ",") {
		if item == option {
			return true
		}
	}
	return false
}

// unknownName is GCP's refusal of a name the schema doesn't have.
func unknownName(key, path string) error {
	return refusedName(key, path, "Cannot find field.")
}

// refusedName is GCP's refusal of one member of the object at path.
func refusedName(key, path, reason string) error {
	description := fmt.Sprintf("Invalid JSON payload received. Unknown name %q", key)
	if path != "" {
		description += fmt.Sprintf(" at '%s'", path)
	}
	return FieldViolation(path, description+": "+reason)
}

// mistyped is GCP's refusal of a value of the wrong type.
func mistyped(path, kind string, raw json.RawMessage) error {
	return FieldViolation(path, fmt.Sprintf("Invalid value at '%s' (%s), %s", path, kind, bytes.TrimSpace(raw)))
}

// emptyJSON reports a value that leaves a field at its default.
func emptyJSON(raw json.RawMessage) bool {
	switch string(bytes.TrimSpace(raw)) {
	case "null", `""`, "{}", "[]", "0", "false":
		return true
	default:
		return false
	}
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func isBool(raw json.RawMessage) bool { return string(raw) == "true" || string(raw) == "false" }

// join appends a dotted path segment.
func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// names builds a set.
func names(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

// sortedKeys orders an object's keys so the first refusal is deterministic.
func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// MaskPaths resolves an update mask (google.protobuf.FieldMask, comma
// separated) to field paths of update's message, accepting JSON or proto
// names. A top-level field in output may not be named. Only message fields
// may have children. An empty mask selects every field update sets, output
// fields excepted.
func MaskPaths(mask string, update proto.Message, output map[protoreflect.Name]bool) ([][]protoreflect.FieldDescriptor, error) {
	descriptor := update.ProtoReflect().Descriptor()
	var paths [][]protoreflect.FieldDescriptor
	if strings.TrimSpace(mask) == "" {
		update.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			if !output[field.Name()] {
				paths = append(paths, []protoreflect.FieldDescriptor{field})
			}
			return true
		})
		return paths, nil
	}
	for path := range strings.SplitSeq(mask, ",") {
		resolved, err := maskPath(descriptor, strings.TrimSpace(path), output)
		if err != nil {
			return nil, err
		}
		paths = append(paths, resolved)
	}
	return paths, nil
}

// maskPath resolves one dotted mask path.
func maskPath(descriptor protoreflect.MessageDescriptor, path string, output map[protoreflect.Name]bool) ([]protoreflect.FieldDescriptor, error) {
	var fields []protoreflect.FieldDescriptor
	for i, segment := range strings.Split(path, ".") {
		if descriptor == nil {
			return nil, Invalid("Invalid update mask path " + path)
		}
		field := descriptor.Fields().ByName(protoreflect.Name(segment))
		if field == nil {
			field = descriptor.Fields().ByJSONName(segment)
		}
		if field == nil || (i == 0 && output[field.Name()]) {
			return nil, Invalid("Invalid update mask path " + path)
		}
		fields = append(fields, field)
		descriptor = field.Message()
		if field.IsMap() || field.IsList() {
			descriptor = nil
		}
	}
	return fields, nil
}

// ApplyMask copies each masked field from update onto a copy of current; a
// field the update leaves unset is cleared.
func ApplyMask[M proto.Message](current, update M, paths [][]protoreflect.FieldDescriptor) M {
	merged := proto.CloneOf(current)
	for _, path := range paths {
		target, source := merged.ProtoReflect(), update.ProtoReflect()
		for _, field := range path[:len(path)-1] {
			target = target.Mutable(field).Message()
			source = source.Get(field).Message()
		}
		leaf := path[len(path)-1]
		if source.Has(leaf) {
			target.Set(leaf, source.Get(leaf))
		} else {
			target.Clear(leaf)
		}
	}
	return merged
}
