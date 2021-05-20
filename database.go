package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var reField = regexp.MustCompile("[^0-9A-Za-z_]")
var reValue = regexp.MustCompile("['\r\n\t]")

// Table is a pseudo-enumerator for indicating which table to write/read to/from.
type Table string

// Live, Archive, Changes ...
const (
	Cache   Table = "cache"
	Live    Table = "live"
	Changes Table = "changes"
)

// Update the store with new map[string]interface{}, register changes.
func Update(db *sqlx.DB, label string, input map[string]interface{}) error {
	cleanedInput := make(map[string]interface{})
	for id, incomingRecord := range input {
		outRecord := make(map[string]interface{})
		for inKey, inValue := range incomingRecord.(map[string]interface{}) {
			outKey := strings.ToLower(reField.ReplaceAllString(inKey, ""))
			outValue := reValue.ReplaceAllString(inValue.(string), "")

			if outKey == "user" {
				outKey = "user_"
			}

			outRecord[outKey] = outValue
		}
		cleanedInput[id] = outRecord
	}

	lastInput, err := Read(db, label, Cache)
	if err != nil {
		return err
	}

	changes := Compare(lastInput, cleanedInput)
	if len(changes) > 0 {
		mappedChanges := make(map[string]interface{})
		for _, change := range changes {
			id, mapped := change.ToMap()
			mappedChanges[id] = mapped
		}

		if err := WriteTyped(db, label, Changes, mappedChanges); err != nil {
			return err
		}

		if err := WriteTyped(db, label, Live, cleanedInput); err != nil {
			return err
		}
	}

	if err := WriteUntyped(db, label, cleanedInput); err != nil {
		return err
	}

	return nil
}

// Restore the `archive` table to a given time.
func Restore(db *sqlx.DB, label string, target time.Time) (map[string]interface{}, error) {
	lastInput, err := Read(db, label, Cache)
	if err != nil {
		return nil, err
	}

	rawChanges, err := Read(db, label, Changes)
	if err != nil {
		return nil, err
	}
	var changes []Change
	for k, v := range rawChanges {
		var change Change
		change.FromMap(k, v.(map[string]interface{}))
		changes = append(changes, change)
	}
	sort.Sort(ByTimestamp(changes))

	for _, change := range changes {
		if change.Timestamp.Before(target) {
			break
		}

		keychain := strings.Split(change.Keychain, "@")
		if len(keychain) == 1 {
			switch change.Operation {
			case Addition:
				delete(lastInput, keychain[0])
			case Deletion:
				raw, _ := change.Old.(string)

				var record map[string]interface{}
				json.Unmarshal([]byte(raw), &record)

				lastInput[keychain[0]] = record
			}
		} else if len(keychain) == 2 {
			_, isMap := lastInput[keychain[0]].(map[string]interface{})
			if !isMap {
				return nil, errors.New("record wasn't a map")
			}

			switch change.Operation {
			case Addition:
				delete(lastInput[keychain[0]].(map[string]interface{}), keychain[1])
			case Modification:
				lastInput[keychain[0]].(map[string]interface{})[keychain[1]] = change.Old
			case Deletion:
				lastInput[keychain[0]].(map[string]interface{})[keychain[1]] = change.Old
			}
		}
	}

	return lastInput, nil
}

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

		if id, in := record["saleshouse_id"]; in {
			delete(record, "saleshouse_id")
			data[id.(string)] = record
		}
	}

	return data, nil
}

// WriteUntyped writes raw string data to the cache table.
func WriteUntyped(db *sqlx.DB, label string, data map[string]interface{}) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s_cache", label))
	if _, is := err.(*pq.Error); is && err.(*pq.Error).Code != "42P01" && err != nil {
		return err
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
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_cache (saleshouse_id VARCHAR,%s)", label, strings.Join(fields, ",")),
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
				"INSERT INTO %s_cache (saleshouse_id,%s) VALUES ('%s',%s)",
				label, strings.Join(fields, ","), id, strings.Join(values, ","),
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

// WriteTyped writes type-inferred data to the indicated table in the labeled store.
func WriteTyped(DB *sqlx.DB, label string, table Table, data map[string]interface{}) error {
	var q string

	var fieldTypes map[string]SupportedType
	var fieldDefinitionClause string
	if table != Changes {
		data = Recast(data)

		q = fmt.Sprintf("DROP TABLE %s_%s", label, table)
		_, err := DB.Exec(q)
		if _, is := err.(*pq.Error); is && err.(*pq.Error).Code != "42P01" && err != nil {
			return err
		}

		fieldTypes = GetFieldTypes(data)
	} else {
		fieldTypes = map[string]SupportedType{
			"keychain":  String,
			"timestamp": String,
			"operation": String,
			"old":       String,
			"new":       String,
		}
	}

	fieldDefinitionClause = CreateFieldDefinitionClause(fieldTypes)

	q = fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_%s (saleshouse_id VARCHAR,%s)",
		label, table, fieldDefinitionClause,
	)
	if _, err := DB.Exec(q); err != nil {
		return err
	}

	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for id, record := range data {
		fields := make([]string, 0)
		values := make([]string, 0)

		for field, value := range record.(map[string]interface{}) {
			fields = append(fields, field)
			fieldType := fieldTypes[field]
			switch fieldType {
			case Int:
				if _, is := value.(string); is {
					value = 0
				}
				values = append(values, fmt.Sprintf("%d", value))
			case Float:
				if _, is := value.(string); is {
					value = 0.0
				}
				values = append(values, fmt.Sprintf("%f", value))
			case Bool:
				if _, is := value.(string); is {
					value = false
				}
				values = append(values, fmt.Sprintf("%t", value))
			case Date:
				if _, is := value.(string); is {
					values = append(values, "null")
				} else {
					values = append(values, fmt.Sprintf("'%s'", value.(time.Time).Format("1/2/2006")))
				}
			default:
				values = append(values, fmt.Sprintf("'%s'", value))
			}
		}

		q = fmt.Sprintf(
			"INSERT INTO %s_%s (saleshouse_id,%s) VALUES ('%s',%s)",
			label, table, strings.Join(fields, ","), id, strings.Join(values, ","),
		)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
