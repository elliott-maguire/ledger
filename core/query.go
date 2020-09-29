package core

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	re = regexp.MustCompile("[^0-9A-Za-z_]")

	createRecordsQuery = "CREATE TABLE IF NOT EXISTS %s.records (id VARCHAR,%s)"
	insertRecordsQuery = "INSERT INTO %s.records (id,%s) VALUES %s"
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
func BuildCreateRecordsTableQuery(schema string, fields []string) string {
	var values []string
	for _, field := range fields {
		values = append(values, re.ReplaceAllString(field, "")+" VARCHAR")
	}

	query := fmt.Sprintf(createRecordsQuery, schema, strings.Join(values, ","))

	return strings.ToValidUTF8(query, "")
}

// BuildInsertRecordsQuery compiles all the value sets in a set of records
// into a single query.
func BuildInsertRecordsQuery(schema string, fields []string, records map[string][]string) string {
	var safeFields []string
	for _, field := range fields {
		safeFields = append(safeFields, re.ReplaceAllString(field, ""))
	}

	var values []string
	for key, record := range records {
		var cleaned []string
		for _, cell := range record {
			cleaned = append(cleaned, re.ReplaceAllString(cell, ""))
		}

		values = append(
			values,
			fmt.Sprintf("('%s','%s')", key, strings.Join(cleaned, "','")),
		)
	}

	query := fmt.Sprintf(insertRecordsQuery, schema, strings.Join(safeFields, ","), strings.Join(values, ","))
	fmt.Println(query)

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
			cleanedPrevious[i] = re.ReplaceAllString(cell, "")
		}

		cleanedNext := make([]string, len(change.Next))
		for i, cell := range change.Next {
			cleanedNext[i] = re.ReplaceAllString(cell, "")
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
