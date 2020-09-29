package core

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // postgres driver
)

// WriteStore takes a database URI and schema name to write to the database.
func WriteStore(uri string, schema string) error {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	return nil
}

// WriteRecords takes a schema name and an array of Record instances
// and writes them to the corresponding schema.
func WriteRecords(uri string, schema string, fields []string, records map[string][]string) error {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	createTable := BuildCreateRecordsTableQuery(schema, fields)
	if _, err := tx.Exec(createTable); err != nil {
		return err
	}
	truncateTable := "TRUNCATE " + schema + ".records"
	if _, err := tx.Exec(truncateTable); err != nil {
		return err
	}
	insertRecords := BuildInsertRecordsQuery(schema, fields, records)
	if _, err := tx.Exec(insertRecords); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// WriteChanges takes a schema name and an array of Change instances
// and writes them to the corresponding schema.
func WriteChanges(uri string, schema string, changes []Change) error {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	createTable := BuildCreateChangesTableQuery(schema)
	if _, err := tx.Exec(createTable); err != nil {
		return err
	}
	insertChanges := BuildInsertChangesQuery(schema, changes)
	if _, err := tx.Exec(insertChanges); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
