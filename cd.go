package bricks

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
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
	Keychain  string
	Timestamp time.Time
	Operation Operation
	Old       interface{}
	New       interface{}
}

// ByTimestamp is a native sort.Interface implementation for sorting by timestamp.
type ByTimestamp []Change

func (t ByTimestamp) Len() int           { return len(t) }
func (t ByTimestamp) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t ByTimestamp) Less(i, j int) bool { return t[i].Timestamp.After(t[j].Timestamp) }

// Map a Change object to a map primitive and return it with an ID.
func (c Change) Map() (string, map[string]interface{}) {
	out := map[string]interface{}{}

	out["keychain"] = c.Keychain
	out["timestamp"] = c.Timestamp.Format(time.RFC3339Nano)
	out["operation"] = fmt.Sprintf("%d", c.Operation)

	if c.Old != nil {
		if reflect.TypeOf(c.Old).Kind() == reflect.Map {
			dump, _ := json.Marshal(c.Old.(map[string]interface{}))
			out["old"] = string(dump)
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
			dump, _ := json.Marshal(c.New.(map[string]interface{}))
			out["new"] = string(dump)
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
	var changes []Change

	for key, value := range old {
		var keychain string
		if len(args) > 0 {
			keychain = args[0] + ":" + key
		} else {
			keychain = key
		}

		if _, in := new[key]; !in {
			id, _ := uuid.NewUUID()
			change := Change{
				ID:        id.String(),
				Keychain:  keychain,
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
			id, _ := uuid.NewUUID()
			change := Change{
				ID:        id.String(),
				Keychain:  keychain,
				Timestamp: time.Now(),
				Operation: Modification,
				Old:       value,
				New:       new[key],
			}
			changes = append(changes, change)
			break
		} else if !reflect.DeepEqual(value, new[key]) {
			ov := value.(map[string]interface{})
			nv := new[key].(map[string]interface{})

			subchanges := Compare(ov, nv, key)
			changes = append(changes, subchanges...)
		}
	}

	for key, value := range new {
		var keychain string
		if len(args) > 0 {
			keychain = args[0] + ":" + key
		} else {
			keychain = key
		}

		if _, in := old[key]; !in {
			id, _ := uuid.NewUUID()
			change := Change{
				ID:        id.String(),
				Keychain:  keychain,
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
