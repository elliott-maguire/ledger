package brickhouse

import (
	"reflect"
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
	Command   string
	Old       interface{}
	New       interface{}
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
	id := ""
	if len(args) > 0 {
		id = args[0]
	}

	matched := make(map[string]bool)
	changes := make([]Change, 0)

	for oK, oV := range old {
		if _, in := matched[oK]; in {
			continue
		}

		for nK, nV := range new {
			if _, in := matched[nK]; in {
				continue
			}

			if oK == nK {
				matched[oK] = true

				_, isOVTerminal := oV.(string)
				_, isNVTerminal := nV.(string)
				if isOVTerminal && isNVTerminal && oV != nV {
					change := Change{
						ID:        id,
						Timestamp: time.Now(),
						Operation: Modification,
						Command:   "",
						Old:       oV,
						New:       nV,
					}
					changes = append(changes, change)
					break
				} else if reflect.DeepEqual(oV, nV) {
					continue
				} else {
					subchanges := Compare(oV.(map[string]interface{}), nV.(map[string]interface{}), oK)
					changes = append(changes, subchanges...)
				}
			}
		}
	}

	for k, v := range old {
		if _, in := matched[k]; !in {
			change := Change{
				ID:        id,
				Timestamp: time.Now(),
				Operation: Deletion,
				Command:   "temp",
				Old:       v,
				New:       nil,
			}
			changes = append(changes, change)
		}
	}

	for k, v := range new {
		if _, in := matched[k]; !in {
			change := Change{
				ID:        id,
				Timestamp: time.Now(),
				Operation: Addition,
				Command:   "temp",
				Old:       nil,
				New:       v,
			}
			changes = append(changes, change)
		}
	}

	return changes
}
