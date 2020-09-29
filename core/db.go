package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgres driver
)

// WriteStore takes a database URI and schema name to write to the database.
func WriteStore(db *sqlx.DB, schema string) error {
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)
	if _, err := db.Exec(query); err != nil {
		return err
	}

	return nil
}

// WriteRecords takes a schema name and an array of Record instances
// and writes them to the corresponding schema.
func WriteRecords(db *sqlx.DB, schema string, fields []string, records map[string][]string) error {
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
func ReadRecords(db *sqlx.DB, schema string) (map[string][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.records", schema)
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

// ReadArchive returns a copy of the given schema's records at the given time.
// The timestamp must be in the RFC3339 format, as that is the format used for stamping
// change sets as they are stored.
func ReadArchive(db *sqlx.DB, schema string, timestamp string) (map[string][]string, error) {
	records, err := ReadRecords(db, schema)
	if err != nil {
		return nil, err
	}
	changes, err := ReadChanges(db, schema)
	if err != nil {
		return nil, err
	}

	for i := range changes {
		change := changes[i]
		if change.Timestamp == timestamp {
			switch change.Operation {
			case Addition:
				delete(records, change.ID)
			case Modification:
				records[change.ID] = change.Previous
			case Deletion:
				records[change.ID] = change.Previous
			}
		}
	}

	return records, nil
}
