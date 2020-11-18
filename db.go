package brickhouse

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var reField = regexp.MustCompile("[^0-9A-Za-z_]")
var reValue = regexp.MustCompile("['\r\n\t]")

func createFieldset(fields []string) string {
	cleaned := make([]string, len(fields))
	for i, f := range fields {
		cleaned[i] = fmt.Sprintf("%s VARCHAR", reField.ReplaceAllString(f, ""))
	}

	return strings.Join(cleaned, ",")
}

// Table is a pseudo-enumerator for indicating which table to write/read to/from.
type Table string

// Live, Archive, Changes ...
const (
	Live    Table = "live"
	Archive Table = "archive"
	Changes Table = "changes"
)

// Ensure the given store exists on the database and is accessible.
func Ensure(db *sqlx.DB, label string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", label)
	if _, err := tx.Exec(createSchema); err != nil {
		return err
	}

	createChangesTable := "CREATE TABLE IF NOT EXISTS %s.%s (%s)"
	fieldset := createFieldset([]string{"brickhouse_id", "timestamp", "operation", "old", "new"})
	if _, err := tx.Exec(fmt.Sprintf(createChangesTable, label, Changes, fieldset)); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Read from the indicated table in the labeled store.
func Read(db *sqlx.DB, label string, table Table) (*map[string]interface{}, error) {
	if err := Ensure(db, label); err != nil {
		return nil, err
	}

	selectAll := fmt.Sprintf("SELECT * FROM %s.%s", label, table)
	rows, err := db.Queryx(selectAll)
	if err != nil {
		if _, is := err.(*pq.Error); is && err.(*pq.Error).Code == "42P01" {
			out := make(map[string]interface{})
			return &out, nil
		}

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
func Write(db *sqlx.DB, label string, table Table, data *map[string]interface{}, args ...bool) error {
	if err := Ensure(db, label); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var fields []string
	var values []string
	insertions := make([]string, 0)
	for id, record := range *data {
		fields = make([]string, 0)
		values = make([]string, 0)

		for k, v := range record.(map[string]interface{}) {
			fields = append(fields, k)
			values = append(values, fmt.Sprintf("'%s'", reValue.ReplaceAllString(v.(string), "")))
		}

		fieldset := strings.Join(fields, ",")
		valueset := strings.Join(values, ",")

		insertion := fmt.Sprintf(
			"INSERT INTO %s.%s (brickhouse_id,%s) VALUES ('%s',%s)",
			label, table, fieldset, id, valueset,
		)
		insertions = append(insertions, insertion)
	}

	fieldset := createFieldset(append(fields, "brickhouse_id"))
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)", label, table, fieldset)
	if _, err := tx.Exec(createTable); err != nil {
		return err
	}

	drop := true
	if len(args) > 0 {
		drop = args[0]
	}
	if drop {
		dropTable := fmt.Sprintf("DROP TABLE %s.%s", label, table)
		if _, err := tx.Exec(dropTable); err != nil {
			return err
		}
		if _, err := tx.Exec(createTable); err != nil {
			return err
		}
	}

	for _, insertion := range insertions {
		if _, err := tx.Exec(insertion); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
