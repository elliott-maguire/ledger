package util

import (
	"reflect"
	"time"
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
	ID        string
	Timestamp string
	Operation OperationType
	Previous  []string
	Next      []string
}

// GetChanges runs through all the find functions and returns a complete set of changes
// for the given current and incoming records.
func GetChanges(current map[string][]string, incoming map[string][]string) []Change {
	var changes []Change
	timestamp := time.Now().Format(time.RFC3339)

	for key, incomingRecord := range incoming {
		if _, exists := current[key]; !exists {
			change := Change{
				ID:        key,
				Timestamp: timestamp,
				Operation: Addition,
				Previous:  nil,
				Next:      incomingRecord,
			}
			changes = append(changes, change)
		}
	}

	for key, incomingRecord := range incoming {
		if currentRecord, exists := current[key]; exists && !reflect.DeepEqual(incomingRecord, currentRecord) {
			change := Change{
				ID:        key,
				Timestamp: timestamp,
				Operation: Modification,
				Previous:  currentRecord,
				Next:      incomingRecord,
			}
			changes = append(changes, change)
		}
	}

	for key, currentRecord := range current {
		if _, exists := incoming[key]; !exists {
			change := Change{
				ID:        key,
				Timestamp: timestamp,
				Operation: Deletion,
				Previous:  currentRecord,
				Next:      nil,
			}
			changes = append(changes, change)
		}
	}

	return changes
}
