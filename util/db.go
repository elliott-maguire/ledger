package util

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// WriteData takes a set of data and writes it to the indicated table.
func WriteData(db *sqlx.DB, schema string, table string, incoming []map[string]string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q := fmt.Sprintf(CreateSchema, schema)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	q = fmt.Sprintf(DropTable, schema, table)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	fields := make([]string, 0)
	for k := range incoming[0] {
		fields = append(fields, k)
	}
	fieldSet := BuildFieldSet(fields)

	q = fmt.Sprintf(CreateRecordsTable, schema, table, fieldSet)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	var values []string
	var valueSet string
	for _, row := range incoming {
		for _, v := range row {
			values = append(values, v)
		}

		valueSet = BuildValueSet(values)
		q = fmt.Sprintf(InsertRecord, schema, table, strings.Join(fields, ","), valueSet)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// ReadData returns a raw two-dimensional representation of the
// contens of the indicated table.
func ReadData(db *sqlx.DB, schema string, table string) (*[][]string, error) {
	q := fmt.Sprintf(SelectRecords, schema, table)
	rows, err := db.Queryx(q)
	if err != nil {
		return nil, err
	}
	data := [][]string{}
	for rows.Next() {
		dest, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}

		row := make([]string, len(dest))
		for i, v := range dest {
			row[i] = v.(string)
		}

		data = append(data, row)
	}

	return &data, nil
}
