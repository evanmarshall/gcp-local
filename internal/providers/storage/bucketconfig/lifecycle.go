package bucketconfig

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Lifecycle actions (buckets resource, lifecycle.rule[].action.type).
const (
	ActionDelete          = "Delete"
	ActionSetStorageClass = "SetStorageClass"
	ActionAbortUpload     = "AbortIncompleteMultipartUpload"
)

// Rule is one lifecycle rule, rendered as GCP stores it: only the fields the
// request set (recorded: storage-lifecycle-rules "rules-set").
type Rule struct {
	Action    Action    `json:"action"`
	Condition Condition `json:"condition"`
}

// Action is a rule's action; StorageClass is SetStorageClass's destination.
type Action struct {
	StorageClass string `json:"storageClass,omitempty"`
	Type         string `json:"type,omitempty"`
}

// Condition is a rule's conditions; a rule applies when all of them hold.
// Pointers keep a zero value (age 0, isLive false) apart from an unset one.
type Condition struct {
	Age                     *int64   `json:"age,omitempty"`
	CreatedBefore           string   `json:"createdBefore,omitempty"`
	CustomTimeBefore        string   `json:"customTimeBefore,omitempty"`
	DaysSinceCustomTime     *int64   `json:"daysSinceCustomTime,omitempty"`
	DaysSinceNoncurrentTime *int64   `json:"daysSinceNoncurrentTime,omitempty"`
	IsLive                  *bool    `json:"isLive,omitempty"`
	MatchesPrefix           []string `json:"matchesPrefix,omitempty"`
	MatchesStorageClass     []string `json:"matchesStorageClass,omitempty"`
	MatchesSuffix           []string `json:"matchesSuffix,omitempty"`
	NoncurrentTimeBefore    string   `json:"noncurrentTimeBefore,omitempty"`
	NumNewerVersions        *int64   `json:"numNewerVersions,omitempty"`
}

// storageClasses are the classes GCP names in lifecycle rules; the legacy
// ones (MULTI_REGIONAL, REGIONAL, DURABLE_REDUCED_AVAILABILITY) depend on the
// bucket's location type.
var storageClasses = []string{ClassStandard, "NEARLINE", "COLDLINE", "ARCHIVE", "MULTI_REGIONAL", "REGIONAL", "DURABLE_REDUCED_AVAILABILITY"}

// ClassStandard is the default storage class.
const ClassStandard = "STANDARD"

// coldness orders the current classes for SetStorageClass precedence: the
// action to the class with the lowest at-rest price wins
// (storage/docs/lifecycle#setstorageclass-cost).
var coldness = map[string]int{ClassStandard: 0, "NEARLINE": 1, "COLDLINE": 2, "ARCHIVE": 3}

// dateLayout is the date format of createdBefore and the other *Before
// conditions.
const dateLayout = "2006-01-02"

// ParseLifecycle reads a bucket's lifecycle field. It returns no rules for
// null or an empty rule list, which GCP stores as no lifecycle (recorded:
// "rules-emptied", "rules-cleared"). location is the bucket's location and
// project its project number, which the MULTI_REGIONAL refusal names.
//
// GCP ignores names a rule does not define: a condition holding only an
// unknown name is "must have a condition" (recorded: "unknown-condition"), so
// unknown names are dropped rather than refused.
func ParseLifecycle(raw json.RawMessage, location, project string) ([]Rule, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	var lifecycle struct {
		Rule []json.RawMessage `json:"rule"`
	}
	if json.Unmarshal(raw, &lifecycle) != nil {
		return nil, GapError("lifecycle must be an object with a rule list")
	}
	rules := make([]Rule, 0, len(lifecycle.Rule))
	for _, item := range lifecycle.Rule {
		var rule Rule
		if json.Unmarshal(item, &rule) != nil {
			return nil, GapError("lifecycle rule values of another JSON type")
		}
		rule.Condition = rule.Condition.compacted()
		if err := rule.validate(location, project); err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	if len(rules) == 0 {
		return nil, nil
	}
	return rules, nil
}

// validate applies GCP's recorded checks to one rule, in the order GCP
// reports them (recorded: storage-lifecycle-rules).
func (r Rule) validate(location, project string) error {
	switch r.Action.Type {
	case "":
		return invalid("Lifecycle rules must have an action.")
	case ActionDelete, ActionSetStorageClass, ActionAbortUpload:
	default:
		return invalid("Invalid value for: %s is not a valid value", r.Action.Type)
	}
	if r.Condition.empty() {
		return invalid("Lifecycle rules must have a condition.")
	}
	if err := r.validateAction(location, project); err != nil {
		return err
	}
	return r.Condition.validate()
}

// validateAction checks a SetStorageClass destination and the conditions an
// AbortIncompleteMultipartUpload rule may carry.
func (r Rule) validateAction(location, project string) error {
	switch r.Action.Type {
	case ActionSetStorageClass:
		class := r.Action.StorageClass
		switch {
		case class == "":
			return invalid("Lifecycle storage class rules must specify the destination storage class.")
		case !slices.Contains(storageClasses, class):
			return invalid("Unknown storage class '%s' is not a valid option for SET_STORAGE_CLASS actions.", class)
		case class == "MULTI_REGIONAL" && strings.Contains(location, "-"):
			return invalid("Project %s may not create storageClass MULTI_REGIONAL buckets with locationConstraint %s.", project, strings.ToUpper(location))
		case coldness[class] == 0 && class != ClassStandard:
			return GapError("lifecycle SetStorageClass to " + class)
		}
	case ActionAbortUpload:
		c := r.Condition
		c.Age, c.MatchesPrefix, c.MatchesSuffix = nil, nil, nil
		if !c.empty() {
			return invalid("Lifecycle age_days, matches_prefix, and matches_suffix conditions are the only ones allowed for a ABORT_INCOMPLETE_MULTIPART_UPLOAD rule.")
		}
	}
	return nil
}

// compacted drops empty lists, which set no condition.
func (c Condition) compacted() Condition {
	for _, list := range []*[]string{&c.MatchesPrefix, &c.MatchesStorageClass, &c.MatchesSuffix} {
		if len(*list) == 0 {
			*list = nil
		}
	}
	return c
}

func (c Condition) empty() bool { return reflect.ValueOf(c).IsZero() }

// validate checks the condition values.
func (c Condition) validate() error {
	if err := c.validateCounts(); err != nil {
		return err
	}
	return c.validateValues()
}

// validateCounts refuses negative counts. Negative age and numNewerVersions
// have recorded messages; the other negative day counts are gaps.
func (c Condition) validateCounts() error {
	if c.Age != nil && *c.Age < 0 {
		return invalid("Lifecycle Age Condition must be greater than or equal to 0, but was %d", *c.Age)
	}
	if c.NumNewerVersions != nil && *c.NumNewerVersions < 0 {
		return invalid("Lifecycle num_newer_versions Condition must be greater than or equal to 0, but was %d", *c.NumNewerVersions)
	}
	for name, days := range map[string]*int64{"daysSinceCustomTime": c.DaysSinceCustomTime, "daysSinceNoncurrentTime": c.DaysSinceNoncurrentTime} {
		if days != nil && *days < 0 {
			return GapError("a negative lifecycle " + name)
		}
	}
	return nil
}

// validateValues refuses a malformed date or an unknown storage class.
func (c Condition) validateValues() error {
	for _, date := range []string{c.CreatedBefore, c.CustomTimeBefore, c.NoncurrentTimeBefore} {
		if _, err := time.Parse(dateLayout, date); date != "" && err != nil {
			return invalid("Invalid value for: Invalid format: %q", date)
		}
	}
	for _, class := range c.MatchesStorageClass {
		if !slices.Contains(storageClasses, class) {
			return invalid("Invalid storage class %q", class)
		}
	}
	return nil
}

// Object is what lifecycle conditions read about one object generation.
// Noncurrent is when it became noncurrent (its timeDeleted), zero while live.
type Object struct {
	Name, StorageClass              string
	Created, CustomTime, Noncurrent time.Time
	// NewerVersions counts the generations of the name newer than this one,
	// the live one included.
	NewerVersions int
}

// Apply returns the action the rules take on an object at now. Delete takes
// precedence over SetStorageClass, and among SetStorageClass actions the
// coldest destination wins (storage/docs/lifecycle#behavior). A
// SetStorageClass to the object's own class is no action.
// AbortIncompleteMultipartUpload rules apply to XML API multipart uploads,
// not objects.
func Apply(rules []Rule, object Object, now time.Time) (Action, bool) {
	var chosen Action
	found := false
	for _, rule := range rules {
		if rule.Action.Type == ActionAbortUpload || !rule.Condition.holds(object, now) {
			continue
		}
		switch {
		case rule.Action.Type == ActionDelete:
			return rule.Action, true
		case rule.Action.StorageClass == object.StorageClass:
		case !found || coldness[rule.Action.StorageClass] > coldness[chosen.StorageClass]:
			chosen, found = rule.Action, true
		}
	}
	return chosen, found
}

// holds reports whether every set condition holds for the object at now.
func (c Condition) holds(object Object, now time.Time) bool {
	return c.timesHold(object, now) && c.matchesHold(object)
}

// timesHold checks the conditions on the object's times. A condition on a
// time the object lacks (customTime, the noncurrent time of a live object)
// does not hold.
func (c Condition) timesHold(object Object, now time.Time) bool {
	return elapsed(c.Age, object.Created, now) &&
		before(c.CreatedBefore, object.Created) &&
		before(c.CustomTimeBefore, object.CustomTime) &&
		elapsed(c.DaysSinceCustomTime, object.CustomTime, now) &&
		elapsed(c.DaysSinceNoncurrentTime, object.Noncurrent, now) &&
		before(c.NoncurrentTimeBefore, object.Noncurrent)
}

// matchesHold checks liveness, storage class, name, and newer versions.
func (c Condition) matchesHold(object Object) bool {
	return (c.IsLive == nil || *c.IsLive == object.Noncurrent.IsZero()) &&
		(c.MatchesStorageClass == nil || slices.Contains(c.MatchesStorageClass, object.StorageClass)) &&
		(c.MatchesPrefix == nil || slices.ContainsFunc(c.MatchesPrefix, func(p string) bool { return strings.HasPrefix(object.Name, p) })) &&
		(c.MatchesSuffix == nil || slices.ContainsFunc(c.MatchesSuffix, func(s string) bool { return strings.HasSuffix(object.Name, s) })) &&
		(c.NumNewerVersions == nil || int64(object.NewerVersions) >= *c.NumNewerVersions)
}

// elapsed reports whether count days have passed since moment; an unset
// count holds, an unset moment does not.
func elapsed(count *int64, moment, now time.Time) bool {
	return count == nil || !moment.IsZero() && !now.Before(moment.Add(time.Duration(*count)*24*time.Hour))
}

// before reports whether moment is before midnight UTC of the date; an unset
// date holds, an unset moment does not.
func before(text string, moment time.Time) bool {
	return text == "" || !moment.IsZero() && moment.Before(date(text))
}

// date is midnight UTC of a validated condition date.
func date(text string) time.Time {
	parsed, _ := time.Parse(dateLayout, text)
	return parsed
}
