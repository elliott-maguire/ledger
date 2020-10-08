package brickhouse

import "github.com/jmoiron/sqlx"

// Source is the interface that must be written to for sources to be warehoused.
type Source interface {
	GetSchema() string
	GetDB() (*sqlx.DB, error)
	GetSchedule() string
	GetData() (fields []string, data map[string][]string, err error)
}
