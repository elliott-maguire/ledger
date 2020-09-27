package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// ReadRecords pulls all of the records out of the records table
// in a given store.
func ReadRecords(uri string, schema string) (*map[string][]string, error) {
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

	return &records, nil
}

// ReadChanges pulls all of the changes out of the changes table
// in a given store.
func ReadChanges(uri string, schema string) (*[]Change, error) {
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

		record := make([]string, len(dest))
		for i, v := range dest {
			record[i] = v.(string)
		}

		var operation OperationType
		opCode, err := strconv.Atoi(record[0])
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

		current := strings.Split(record[1], ",")
		incoming := strings.Split(record[2], ",")

		change := Change{
			Operation: operation,
			Current:   current,
			Incoming:  incoming,
		}
		changes = append(changes, change)
	}

	return &changes, nil
}
