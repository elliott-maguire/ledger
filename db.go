package bricks

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

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
	Changes Table = "changes"
)

// Read from the indicated table in the labeled store.
func Read(db *sqlx.DB, label string, table Table) (map[string]interface{}, error) {
	rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM %s_%s", label, table))
	if err != nil {
		if _, is := err.(*pq.Error); is && err.(*pq.Error).Code == "42P01" {
			return map[string]interface{}{}, nil
		}

		return nil, err
	}

	// Scan rows into maps, extract record ID, add to data map
	data := map[string]interface{}{}
	for rows.Next() {
		record := map[string]interface{}{}
		if err := rows.MapScan(record); err != nil {
			return nil, err
		}

		if id, in := record["bricks_id"]; in {
			delete(record, "bricks_id")
			data[id.(string)] = record
		}
	}

	return data, nil
}

// Write to the indicated table in the labeled store.
func Write(db *sqlx.DB, label string, table Table, data map[string]interface{}) error {
	if table != Changes {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE %s_%s", label, table))
		if _, is := err.(*pq.Error); is && err.(*pq.Error).Code != "42P01" && err != nil {
			return err
		}
	}

	fields := make([]string, 0)
	i := 0
	for _, v := range data {
		if i > 0 {
			break
		}

		for k := range v.(map[string]interface{}) {
			fields = append(fields, fmt.Sprintf("%s VARCHAR", k))
		}

		i++
	}

	if _, err := db.Exec(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_%s (bricks_id VARCHAR,%s)", label, table, strings.Join(fields, ",")),
	); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for id, record := range data {
		fields := make([]string, 0)
		values := make([]string, 0)

		for k, v := range record.(map[string]interface{}) {
			fields = append(fields, k)
			values = append(values, fmt.Sprintf("'%s'", v))
		}

		if _, err := tx.Exec(
			fmt.Sprintf(
				"INSERT INTO %s_%s (bricks_id,%s) VALUES ('%s',%s)",
				label, table, strings.Join(fields, ","), id, strings.Join(values, ","),
			),
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
