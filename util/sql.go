package util

import (
	"fmt"
	"regexp"
	"strings"
)

var reField = regexp.MustCompile("[^0-9A-Za-z_]")
var reValue = regexp.MustCompile("['\r\n\t]")

// BuildFieldSet constructs a field set string from an array of field names.
func BuildFieldSet(fields []string) string {
	cleaned := make([]string, len(fields))
	for i, f := range fields {
		cleaned[i] = fmt.Sprintf("%s VARCHAR", reField.ReplaceAllString(f, ""))
	}

	return strings.Join(cleaned, ",")
}

// BuildValueSet constructs a value set string from an array of values.
func BuildValueSet(values []string) string {
	cleaned := make([]string, len(values))
	for i, v := range values {
		cleaned[i] = fmt.Sprintf("'%s'", reValue.ReplaceAllString(v, ""))
	}

	return strings.Join(cleaned, ",")
}

// CreateSchema takes schema
var CreateSchema = "CREATE SCHEMA IF NOT EXISTS %s"

// CreateRecordsTable takes schema, then table, then field definitions
var CreateRecordsTable = "CREATE TABLE IF NOT EXISTS %s.%s (id VARCHAR,%s)"

// CreateChangesTable takes schema
var CreateChangesTable = `
CREATE TABLE IF NOT EXISTS %s.changes (
	id VARCHAR, 
	timestamp VARCHAR, 
	operation VARCHAR, 
	previous VARCHAR, 
	next VARCHAR
)
`

// InsertRecord takes schema, then table, then a field set, then value sets
var InsertRecord = "INSERT INTO %s.%s (id,%s) VALUES (%s)"

// InsertChange takes schema, then value sets
var InsertChange = "INSERT INTO %s.changes (id,timestamp,operation,previous,next) VALUES (%s)"

// SelectRecords takes a schema, then table
var SelectRecords = "SELECT * FROM %s.%s"

// TruncateRecords takes schema, then table
var TruncateRecords = "TRUNCATE %s.%s"
