package brickhouse

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Update the store with new map[string]interface{}, register changes.
func Update(db *sqlx.DB, label string, data *map[string]interface{}) (int, error) {
	return 0, nil
}

// Restore the `archive` table to a given time.
func Restore(db *sqlx.DB, label string, time time.Time) (*[][]string, error) {
	return nil, nil
}
