package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// ReadRecords pulls all of the records out of the records table
// in a given store.
func ReadRecords(uri string, schema string) (map[string][]string, error) {
	db, err := sqlx.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

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
func ReadChanges(uri string, schema string) ([]Change, error) {
	db, err := sqlx.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

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
func ReadArchive(uri string, schema string, timestamp string) (map[string][]string, error) {
	records, err := ReadRecords(uri, schema)
	if err != nil {
		return nil, err
	}
	changes, err := ReadChanges(uri, schema)
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
