package brickhouse

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	createRecordsQuery = "CREATE TABLE IF NOT EXISTS %s.%s (id VARCHAR,%s)"
	insertRecordsQuery = "INSERT INTO %s.%s (id,%s) VALUES (%s)"
	createChangesQuery = `
		CREATE TABLE IF NOT EXISTS %s.changes (
			id VARCHAR, 
			timestamp VARCHAR, 
			operation VARCHAR, 
			current VARCHAR, 
			incoming VARCHAR
		)
	`
	insertChangesQuery = "INSERT INTO %s.changes (id,timestamp,operation,current,incoming) VALUES %s"
)

// BuildCreateRecordsTableQuery smashes together a schema and list of fields
// to build the query used to create the records table for a Store.
func BuildCreateRecordsTableQuery(schema string, table string, fields []string) string {
	re := regexp.MustCompile("[^0-9A-Za-z_]")

	var values []string
	for _, field := range fields {
		values = append(values, re.ReplaceAllString(field, "")+" VARCHAR")
	}

	query := fmt.Sprintf(createRecordsQuery, schema, table, strings.Join(values, ","))

	return strings.ToValidUTF8(query, "")
}

// BuildInsertRecordQuery compiles all the value sets in a set of records
// into a single query.
func BuildInsertRecordQuery(schema string, table string, fields []string, key string, record []string) string {
	re := regexp.MustCompile("[^0-9A-Za-z_]")

	var safeFields []string
	for _, field := range fields {
		safeFields = append(safeFields, re.ReplaceAllString(field, ""))
	}
	fieldsClause := strings.Join(safeFields, ",")

	var safeValues []string
	for _, value := range record {
		safeValues = append(safeValues, strings.ReplaceAll(value, "'", ""))
	}
	valuesClause := fmt.Sprintf("'%s','%s'", key, strings.Join(safeValues, "','"))

	query := fmt.Sprintf(insertRecordsQuery, schema, table, fieldsClause, valuesClause)

	return strings.ToValidUTF8(query, "")
}

// BuildCreateChangesTableQuery is another convenient hideaway so that we
// don't have to look at SQL in other functions.
func BuildCreateChangesTableQuery(schema string) string {
	query := fmt.Sprintf(createChangesQuery, schema)

	return strings.ToValidUTF8(query, "")
}

// BuildInsertChangesQuery is yet another convenience function for hiding away
// unique query building.
func BuildInsertChangesQuery(schema string, changes []Change) string {
	var values []string
	for _, change := range changes {
		cleanedPrevious := make([]string, len(change.Previous))
		for i, cell := range change.Previous {
			cleanedPrevious[i] = strings.ReplaceAll(cell, "'", "''")
		}

		cleanedNext := make([]string, len(change.Next))
		for i, cell := range change.Next {
			cleanedNext[i] = strings.ReplaceAll(cell, "'", "''")
		}

		values = append(
			values,
			fmt.Sprintf(
				"('%s','%s','%d','%s','%s')",
				change.ID,
				change.Timestamp,
				change.Operation,
				strings.Join(cleanedPrevious, ","),
				strings.Join(cleanedNext, ","),
			),
		)
	}

	query := fmt.Sprintf(insertChangesQuery, schema, strings.Join(values, ","))

	return strings.ToValidUTF8(query, "")
}
