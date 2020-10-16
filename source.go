package brickhouse

import "github.com/jmoiron/sqlx"

// Source is what allows users to define their own data sources to be synced.
type Source interface {
	GetName() string
	GetDB() (*sqlx.DB, error)
	GetSchedule() string
	GetFields() []string
	GetData() (data map[string][]string, err error)
}
