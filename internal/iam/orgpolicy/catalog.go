// Package orgpolicy models Organization Policy v2 for the constraints the
// local providers enforce: the constraint catalog, policy validation, the
// hierarchy merge that getEffectivePolicy reports, and rule evaluation. It
// holds no state: the IAM engine stores policies and walks the hierarchy.
package orgpolicy

import (
	_ "embed"
	"encoding/json"
	"maps"
	"strings"
)

// catalogJSON is GCP's definition of every enforced constraint, read with
// constraints.list on 2026-10-13, in the order it returned them.
//
//go:embed constraints.json
var catalogJSON []byte

// Constraint is one constraint the emulator enforces.
type Constraint struct {
	// Name is the constraint ID without its prefix: "gcp.resourceLocations".
	Name string
	// List is true for a list constraint, false for a boolean one.
	List bool
	// definition is the constraints.list entry without its name.
	definition map[string]any
}

// catalog is the parsed constraint catalog, in GCP's order.
var catalog = loadCatalog()

// loadCatalog parses the embedded catalog. A malformed file is a build
// defect, so it panics at startup rather than enforcing a partial catalog.
func loadCatalog() []Constraint {
	var file struct {
		Constraints []map[string]any `json:"constraints"`
	}
	if err := json.Unmarshal(catalogJSON, &file); err != nil || len(file.Constraints) == 0 {
		panic("orgpolicy: invalid constraints.json")
	}
	constraints := make([]Constraint, 0, len(file.Constraints))
	for _, entry := range file.Constraints {
		name, _ := entry["name"].(string)
		id, ok := strings.CutPrefix(name, prefix)
		if !ok {
			panic("orgpolicy: constraint without the constraints/ prefix")
		}
		_, list := entry["listConstraint"]
		definition := maps.Clone(entry)
		delete(definition, "name")
		constraints = append(constraints, Constraint{Name: id, List: list, definition: definition})
	}
	return constraints
}

// prefix starts every constraint reference ("constraints/gcp.resourceLocations").
const prefix = "constraints/"

// Catalog returns the enforced constraints in GCP's list order.
func Catalog() []Constraint { return append([]Constraint(nil), catalog...) }

// Lookup finds an enforced constraint by ID.
func Lookup(name string) (Constraint, bool) {
	for _, constraint := range catalog {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return Constraint{}, false
}

// Reference is the constraint's "constraints/NAME" form.
func (c Constraint) Reference() string { return prefix + c.Name }

// Description is the constraint's description, which some refusals
// quote.
func (c Constraint) Description() string {
	description, _ := c.definition["description"].(string)
	return description
}

// Wire renders the constraint as constraints.list returns it under parent
// ("projects/p"): GCP echoes the parent as the request named it.
func (c Constraint) Wire(parent string) map[string]any {
	body := maps.Clone(c.definition)
	body["name"] = parent + "/constraints/" + c.Name
	return body
}
