package core

import (
	"reflect"
)

// OperationType abstracts the available operation types and gives a namespace for them.
type OperationType int

// Adddition, Modification, and Deletion are the three available op types.
const (
	Addition OperationType = iota
	Modification
	Deletion
)

// Change represents a single linked change in a Store's records.
type Change struct {
	Operation OperationType
	Current   []string
	Incoming  []string
}

// GetChanges runs through all the find functions and returns a complete set of changes
// for the given current and incoming records.
func GetChanges(current map[string][]string, incoming map[string][]string) *[]Change {
	var changes []Change

	for key, incomingRecord := range incoming {
		if _, exists := current[key]; !exists {
			changes = append(changes, Change{
				Operation: Addition,
				Current:   nil,
				Incoming:  incomingRecord,
			})
		}
	}

	for key, incomingRecord := range incoming {
		if currentRecord, exists := current[key]; exists && !reflect.DeepEqual(incomingRecord, currentRecord) {
			changes = append(changes, Change{
				Operation: Modification,
				Current:   currentRecord,
				Incoming:  incomingRecord,
			})
		}
	}

	for key, currentRecord := range current {
		if _, exists := incoming[key]; !exists {
			changes = append(changes, Change{
				Operation: Deletion,
				Current:   currentRecord,
				Incoming:  nil,
			})
		}
	}

	return &changes
}
