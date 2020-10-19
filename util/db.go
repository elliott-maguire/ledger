package util

import "github.com/jmoiron/sqlx"

// EnsureSchema creates a schema with the given label if it does not exist.
func EnsureSchema(db *sqlx.DB, label string) error
