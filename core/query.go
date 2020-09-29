package core

import (
	"fmt"
	"regexp"
	"strings"
)

var re = regexp.MustCompile("[^0-9A-Za-z_]")

// BuildCreateRecordsTableQuery smashes together a schema and list of fields
// to build the query used to create the records table for a Store.
func BuildCreateRecordsTableQuery(schema string, fields []string) string {
	command := "CREATE TABLE IF NOT EXISTS"
	target := schema + ".records"

	var values []string
	for _, field := range fields {
		values = append(values, re.ReplaceAllString(field, "")+" VARCHAR")
	}

	query := fmt.Sprintf("%s %s (id VARCHAR,%s)", command, target, strings.Join(values, ","))

	return strings.ToValidUTF8(query, "")
}

// BuildInsertRecordsQuery compiles all the value sets in a set of records
// into a single query.
func BuildInsertRecordsQuery(schema string, fields []string, records map[string][]string) string {
	command := "INSERT INTO"
	target := schema + ".records"

	var safeFields []string
	for _, field := range fields {
		safeFields = append(safeFields, re.ReplaceAllString(field, ""))
	}

	var values []string
	for key, record := range records {
		values = append(
			values,
			fmt.Sprintf("('%s','%s')", key, strings.Join(record, "','")),
		)
	}

	query := fmt.Sprintf(
		"%s %s (id,%s) VALUES %s",
		command, target, strings.Join(safeFields, ","), strings.Join(values, ","),
	)

	return strings.ToValidUTF8(query, "")
}

// BuildCreateChangesTableQuery is another convenient hideaway so that we
// don't have to look at SQL in other functions.
func BuildCreateChangesTableQuery(schema string) string {
	command := "CREATE TABLE IF NOT EXISTS"
	target := schema + ".changes"

	values := "(id VARCHAR, timestamp VARCHAR, operation VARCHAR, current VARCHAR, incoming VARCHAR)"

	query := fmt.Sprintf("%s %s %s", command, target, values)

	return strings.ToValidUTF8(query, "")
}

// BuildInsertChangesQuery is yet another convenience function for hiding away
// unique query building.
func BuildInsertChangesQuery(schema string, changes []Change) string {
	command := "INSERT INTO"
	target := schema + ".changes"

	var values []string
	for _, change := range changes {
		values = append(
			values,
			fmt.Sprintf(
				"('%s','%s','%d','%s','%s')",
				change.ID,
				change.Timestamp,
				change.Operation,
				strings.Join(change.Previous, ","),
				strings.Join(change.Next, ","),
			),
		)
	}

	query := fmt.Sprintf(
		"%s %s (id,timestamp,operation,current,incoming) VALUES %s",
		command, target, strings.Join(values, ","),
	)

	return strings.ToValidUTF8(query, "")
}
