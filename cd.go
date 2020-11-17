package brickhouse

import "time"

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

// Compare two datasets and return the changes between them.
func Compare(old Dataset, new Dataset) *[]Change
