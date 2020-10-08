package brickhouse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgres driver
)

// TableDirective allows the user to point the WriteRecords method to either the live table
// or the archive table when writing a given record map.
type TableDirective uint

// ...
const (
	Live TableDirective = iota
	Archive
)

// WriteStore takes a database URI and schema name to write to the database.
func WriteStore(db *sqlx.DB, schema string) error {
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	return nil
}

// WriteRecords takes a schema name and a record map and writes them to the corresponding schema.
func WriteRecords(db *sqlx.DB, schema string, directive TableDirective, fields []string, records map[string][]string) error {
	var table string
	if directive == 0 {
		table = "live"
	} else {
		table = "archive"
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	createTable := BuildCreateRecordsTableQuery(schema, table, fields)
	if _, err := tx.Exec(createTable); err != nil {
		return err
	}
	truncateTable := "TRUNCATE " + schema + "." + table
	if _, err := tx.Exec(truncateTable); err != nil {
		return err
	}

	var insertRecord string
	for key, record := range records {
		insertRecord = BuildInsertRecordQuery(schema, table, fields, key, record)
		if _, err := tx.Exec(insertRecord); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// WriteChanges takes a schema name and an array of Change instances
// and writes them to the corresponding schema.
func WriteChanges(db *sqlx.DB, schema string, changes []Change) error {
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

// ReadRecords pulls all of the records out of the records table
// in a given store.
func ReadRecords(db *sqlx.DB, schema string, directive TableDirective) (map[string][]string, error) {
	var table string
	if directive == 0 {
		table = "live"
	} else {
		table = "archive"
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s", schema, table)
	rows, err := db.Queryx(query)
	if err != nil {
		return nil, err
	}

	records := map[string][]string{}
	for rows.Next() {
		dest, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}

		record := make([]string, len(dest))
		for i, v := range dest {
			record[i] = v.(string)
		}

		key := record[0]
		values := append(record[:0], record[1:]...)
		records[key] = values
	}

	return records, nil
}

// ReadChanges pulls all of the changes out of the changes table
// in a given store.
func ReadChanges(db *sqlx.DB, schema string) ([]Change, error) {
	query := fmt.Sprintf("SELECT * FROM %s.changes", schema)
	rows, err := db.Queryx(query)
	if err != nil {
		return nil, err
	}

	var changes []Change
	for rows.Next() {
		dest, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}

		row := make([]string, len(dest))
		for i, v := range dest {
			row[i] = v.(string)
		}

		id := row[0]
		timestamp := row[1]

		var operation OperationType
		opCode, err := strconv.Atoi(row[2])
		if err != nil {
			return nil, err
		}
		switch opCode {
		case 0:
			operation = Addition
		case 1:
			operation = Modification
		case 2:
			operation = Deletion
		}

		current := strings.Split(row[3], ",")
		incoming := strings.Split(row[4], ",")

		change := Change{
			ID:        id,
			Timestamp: timestamp,
			Operation: operation,
			Previous:  current,
			Next:      incoming,
		}
		changes = append(changes, change)
	}

	return changes, nil
}
