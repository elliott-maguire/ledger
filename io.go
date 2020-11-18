package brickhouse

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// Update the store with new map[string]interface{}, register changes.
func Update(db *sqlx.DB, label string, data *map[string]interface{}) error {
	old, err := Read(db, label, Live)
	if err != nil {
		return err
	}

	changes := Compare(*old, *data)
	mappedChanges := make(map[string]interface{})
	for _, change := range changes {
		id, record := change.Map()
		mappedChanges[id] = record
	}
	if err := Write(db, label, Changes, &mappedChanges, false); err != nil {
		return err
	}

	if err := Write(db, label, Live, data, true); err != nil {
		return err
	}

	return nil
}

// Restore the `archive` table to a given time.
func Restore(db *sqlx.DB, label string, time time.Time) (*[][]string, error) {
	return nil, nil
}
