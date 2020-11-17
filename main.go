package brickhouse

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Update runs several smaller operations to add new data to the warehouse.
//   1. Ensures a schema exists with the given label
//   2. Evaluates existing data against incoming data for changes
//   3. If changes exist, the incoming data replaces the existing data on the live table
//   4. Changes are written to the history table
func Update(db *sqlx.DB, label string, incoming map[string]map[string]string) error {
	return nil
}

// Restore runs several smaller operations to restore a previous version of data.
//   1. Ensures a schema exists with the given label
//   2. Retrieves the data in its current state
//   3. Retrieves the data's history
//   4. Iterates through the history and applies reversions until the indicated time is reached
//   5. Replaces the existing data on the archive table, returns a pointer to the raw data
func Restore(db *sqlx.DB, label string, to time.Time) (*[][]string, error) {
	return nil, nil
}
