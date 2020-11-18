package brickhouse

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
)

var reField = regexp.MustCompile("[^0-9A-Za-z_]")
var reValue = regexp.MustCompile("['\r\n\t]")

// Table is a pseudo-enumerator for indicating which table to write/read to/from.
type Table string

// Live, Archive, Changes ...
const (
	Live    Table = "live"
	Archive Table = "archive"
	Changes Table = "changes"
)

// Ensure the given store exists on the database and is accessible.
func Ensure(db *sqlx.DB, label string, fields ...string) error {
	var q string

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", label)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	if len(fields) > 0 {
		base := "CREATE TABLE IF NOT EXISTS %s.%s (%s)"

		createFieldset := func(fields []string) string {
			cleaned := make([]string, len(fields))
			for i, f := range fields {
				cleaned[i] = fmt.Sprintf("%s VARCHAR", reField.ReplaceAllString(f, ""))
			}

			return strings.Join(cleaned, ",")
		}

		fieldset := createFieldset(append(fields, "brickhouse_id"))
		q = fmt.Sprintf(base, label, Live, fieldset)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
		q = fmt.Sprintf(base, label, Archive, fieldset)
		if _, err := tx.Exec(q); err != nil {
			return err
		}

		changeFields := []string{"id", "timestamp", "operation", "old", "new"}
		fieldset = createFieldset(changeFields)
		if _, err := tx.Exec(fmt.Sprintf(base, label, Changes, fieldset)); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Read from the indicated table in the labeled store.
func Read(db *sqlx.DB, label string, table Table) (*map[string]interface{}, error) {
	var q string

	if err := Ensure(db, label); err != nil {
		return nil, err
	}

	q = fmt.Sprintf("SELECT * FROM %s.%s", label, table)
	rows, err := db.Queryx(q)
	if err != nil {
		return nil, err
	}

	data := make(map[string]interface{})
	record := make(map[string]interface{})
	for rows.Next() {
		if err := rows.MapScan(record); err != nil {
			return nil, err
		}

		if id, in := record["brickhouse_id"]; in {
			delete(record, "brickhouse_id")
			data[id.(string)] = record
		}
	}

	return &data, nil
}

// Write to the indicated table in the labeled store.
func Write(db *sqlx.DB, label string, table Table, data map[string]interface{}, args ...bool) error {
	var q string

	drop := false
	if len(args) > 0 {
		drop = args[0]
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if drop {
		q = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", label, table)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	} else {
		q = fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s", label, table)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	ensured := false
	for id, record := range data {
		fields := make([]string, 0)
		values := make([]string, 0)

		for k, v := range record.(map[string]interface{}) {
			fields = append(fields, k)
			values = append(values, fmt.Sprintf("'%s'", reValue.ReplaceAllString(v.(string), "")))
		}

		if !ensured {
			if err := Ensure(db, label, fields...); err != nil {
				return err
			}
		}

		fieldset := strings.Join(fields, ",")
		valueset := strings.Join(values, ",")

		q = fmt.Sprintf("INSERT INTO %s.%s (brickhouse_id,%s) VALUES ('%s',%s)", label, table, fieldset, id, valueset)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
