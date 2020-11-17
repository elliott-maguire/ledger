package brickhouse

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Dataset is a readability alias for a cumbersome nested string map structure.
type Dataset map[string]map[string]string

// Update the store with new data, register changes.
func Update(db *sqlx.DB, label string, dataset *Dataset) (int, error)

// Restore the `archive` table to a given time.
func Restore(db *sqlx.DB, label string, time time.Time) (*[][]string, error)
