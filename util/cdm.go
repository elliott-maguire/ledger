package util

// Change holds metadata for a single-point atomic change.
type Change struct {
	ID        string
	Key       string
	Timestamp string
	Previous  string
	Next      string
}

// DetectChanges compares two datasets row by row, cell by cell, and produces an
// array of all the changes between the two.
func DetectChanges(current []map[string]string, incoming []map[string]string) []Change
