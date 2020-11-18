package brickhouse

import "github.com/jmoiron/sqlx"

// Table is a pseudo-enumerator for indicating which table to write/read to/from.
type Table uint

// Live, Archive, Changes ...
const (
	Live Table = iota
	Archive
	Changes
)

// Ensure the given store exists on the map[string]interface{}base and is accessible.
func Ensure(db *sqlx.DB, label string) error {
	return nil
}

// Read from the indicated table in the labeled store.
func Read(db *sqlx.DB, label string, table Table) (*[][]string, error) {
	return nil, nil
}

// Write to the indicated table in the labeled store.
func Write(db *sqlx.DB, label string, table Table, data map[string]interface{}, drop bool) error {
	return nil
}
