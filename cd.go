package brickhouse

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Operation is a pseudo-enumerator for indicating operation type.
type Operation uint

// Addition, Modification, Deletion ...
const (
	Addition Operation = iota
	Modification
	Deletion
)

// Change is a flexible structure that represents a single atomic change to a row or cell.
type Change struct {
	ID        string
	Timestamp time.Time
	Operation Operation
	Old       interface{}
	New       interface{}
}

// Map a Change object to a map primitive.
func (c Change) Map() (string, map[string]interface{}) {
	out := make(map[string]interface{})

	out["timestamp"] = c.Timestamp.Format(time.RFC3339)
	out["operation"] = fmt.Sprintf("%d", c.Operation)

	if c.Old != nil {
		if reflect.TypeOf(c.Old).Kind() == reflect.Map {
			cleaned := make([]string, 0)
			for _, v := range c.Old.(map[string]interface{}) {
				cleaned = append(cleaned, strings.ReplaceAll(v.(string), "'", "''"))
			}

			out["old"] = strings.Join(cleaned, ",")
		} else if reflect.TypeOf(c.Old).Kind() == reflect.String {
			out["old"] = c.Old.(string)
		} else {
			out["old"] = "INVALID TYPE"
		}
	} else {
		out["old"] = ""
	}

	if c.New != nil {
		if reflect.TypeOf(c.New).Kind() == reflect.Map {
			cleaned := make([]string, 0)
			for _, v := range c.New.(map[string]interface{}) {
				cleaned = append(cleaned, strings.ReplaceAll(v.(string), "'", "''"))
			}

			out["new"] = strings.Join(cleaned, ",")
		} else if reflect.TypeOf(c.New).Kind() == reflect.String {
			out["new"] = c.New.(string)
		} else {
			out["new"] = "INVALID TYPE"
		}
	} else {
		out["new"] = ""
	}

	return c.ID, out
}

// Compare two maps recursively and return the changes between them.
//
// This algorithm is designed to first evaluate the top-level records, then the individual cells therein.
// Values of type `string` are terminal and will cancel recursion.
// If values are `interface{}` (i.e. base map[string]interface{} types), recursion will continue.
//
// The `args` variadic parameter is used for passing an optional ID value for nested comparisons.
// Top-level comparisons do not need any ID, so no value needs to be explicitly passed by the user.
func Compare(old map[string]interface{}, new map[string]interface{}, args ...string) []Change {
	changes := make([]Change, 0)

	for key, value := range old {
		id := ""
		if len(args) > 0 {
			id = args[0] + "." + key
		} else {
			id = key
		}

		if _, in := new[key]; !in {
			change := Change{
				ID:        id,
				Timestamp: time.Now(),
				Operation: Deletion,
				Old:       value,
				New:       nil,
			}
			changes = append(changes, change)
			continue
		}

		_, isOldValueTerminal := value.(string)
		_, isNewValueTerminal := new[key].(string)
		if isOldValueTerminal && isNewValueTerminal && value != new[key] {
			change := Change{
				ID:        id,
				Timestamp: time.Now(),
				Operation: Modification,
				Old:       value,
				New:       new[key],
			}
			changes = append(changes, change)
			break
		} else if !reflect.DeepEqual(value, new[key]) {
			subchanges := Compare(value.(map[string]interface{}), new[key].(map[string]interface{}), key)
			changes = append(changes, subchanges...)
		}
	}

	for key, value := range new {
		id := ""
		if len(args) > 0 {
			id = args[0] + "." + key
		} else {
			id = key
		}

		if _, in := old[key]; !in {
			change := Change{
				ID:        id,
				Timestamp: time.Now(),
				Operation: Addition,
				Old:       nil,
				New:       value,
			}
			changes = append(changes, change)
		}
	}

	return changes
}
