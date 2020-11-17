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

// Ensure the given store exists on the database and is accessible.
func Ensure(db *sqlx.DB, label string) error

// Read from the indicated table in the labeled store.
func Read(db *sqlx.DB, label string, table Table) (*[][]string, error)

// Write to the indicated table in the labeled store.
func Write(db *sqlx.DB, label string, table Table, dataset Dataset, drop bool) error
