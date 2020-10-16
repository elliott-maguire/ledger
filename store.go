package brickhouse

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sr-revops/brickhouse/util"
)

type table uint

// live, archive are table directive constants for the internal record read/write methods.
const (
	live table = iota
	archive
)

// Store is a metadata struct for the data being collected and managed from an implemented Source.
type Store struct {
	Name   string
	DB     *sqlx.DB
	Fields []string
}

// Ensure creates the schema and necessary tables if they don't already exist,
// i.e. `live`, `archive`, `changes`, and `meta`.
//
// `live` is for the current state of the data, in its most up-to-date form.
// `archive` is for a past snapshot of the data as constructed by `Retrieve`.
// `changes` is for the complete record of changes that happens to the data.
// `meta` is for the store's metadata.
func (s Store) Ensure() error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q := fmt.Sprintf(util.CreateSchema, s.Name)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	// Create `live` and `archive` tables
	fieldSet := util.BuildFieldSet(s.Fields)
	q = fmt.Sprintf(util.CreateRecordsTable, s.Name, "live", fieldSet)
	if _, err := tx.Exec(q); err != nil {
		return err
	}
	q = fmt.Sprintf(util.CreateRecordsTable, s.Name, "archive", fieldSet)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	// Create `changes` table
	q = fmt.Sprintf(util.CreateChangesTable, s.Name)
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Update replaces the records in the `live` table with the incoming records,
// finds all changes, and writes them to the `changes` table.
func (s Store) Update(incoming map[string][]string) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Pull current data to memory before truncating
	q := fmt.Sprintf(util.SelectRecords, s.Name, "live")
	rows, err := s.DB.Queryx(q)
	if err != nil {
		return err
	}
	current := map[string][]string{}
	for rows.Next() {
		dest, err := rows.SliceScan()
		if err != nil {
			return err
		}

		record := make([]string, len(dest))
		for i, v := range dest {
			record[i] = v.(string)
		}

		key := record[0]
		values := append(record[:0], record[1:]...)
		current[key] = values
	}

	// Truncate live table as it is
	q = fmt.Sprintf(util.TruncateRecords, s.Name, "live")
	if _, err := tx.Exec(q); err != nil {
		return err
	}

	// Repopulate live with incoming records
	for key, cells := range incoming {
		valueSet := util.BuildValueSet(append([]string{key}, cells...))

		q := fmt.Sprintf(util.InsertRecord, s.Name, "live", strings.Join(s.Fields, ","), valueSet)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	// Calculate and add changes to the changes table
	changes := util.GetChanges(current, incoming)
	for _, c := range changes {
		cleanedPrevious := make([]string, len(c.Previous))
		for i, cell := range c.Previous {
			cleanedPrevious[i] = strings.ReplaceAll(cell, "'", "''")
		}

		cleanedNext := make([]string, len(c.Next))
		for i, cell := range c.Next {
			cleanedNext[i] = strings.ReplaceAll(cell, "'", "''")
		}

		valueSet := fmt.Sprintf(
			"'%s','%s','%d','%s','%s'",
			c.ID,
			c.Timestamp,
			c.Operation,
			strings.Join(cleanedPrevious, ","),
			strings.Join(cleanedNext, ","),
		)

		q = fmt.Sprintf(util.InsertChange, s.Name, valueSet)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Retrieve uses the instructions in the `changes` table to reconstruct the state
// of the records at the given time `when`, to the closest degree. It also automatically
// updates the `archive` table with the retrieved records.
func (s Store) Retrieve(when time.Time) (*map[string][]string, error) {
	q := fmt.Sprintf(util.SelectRecords, s.Name, "live")
	rows, err := s.DB.Queryx(q)
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

	q = fmt.Sprintf(util.SelectRecords, s.Name, "changes")
	rows, err = s.DB.Queryx(q)
	if err != nil {
		return nil, err
	}
	var changes []util.Change
	for rows.Next() {
		dest, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}

		row := make([]string, len(dest))
		for i, v := range dest {
			row[i] = v.(string)
		}

		id := row[0]
		timestamp := row[1]

		var operation util.OperationType
		opCode, err := strconv.Atoi(row[2])
		if err != nil {
			return nil, err
		}
		switch opCode {
		case 0:
			operation = util.Addition
		case 1:
			operation = util.Modification
		case 2:
			operation = util.Deletion
		}

		current := strings.Split(row[3], ",")
		incoming := strings.Split(row[4], ",")

		change := util.Change{
			ID:        id,
			Timestamp: timestamp,
			Operation: operation,
			Previous:  current,
			Next:      incoming,
		}
		changes = append(changes, change)
	}

	counts := make(map[string]int, 0)
	for _, c := range changes {
		if _, exists := counts[c.Timestamp]; exists {
			counts[c.Timestamp]++
		} else {
			counts[c.Timestamp] = 1
		}
	}

	for i := len(changes) - 1; i >= 0; i-- {
		c := changes[i]

		check, _ := time.Parse(time.RFC3339, c.Timestamp)
		if check.Before(when) {
			break
		}

		switch c.Operation {
		case util.Addition:
			delete(records, c.ID)
		case util.Modification:
			records[c.ID] = c.Previous
		case util.Deletion:
			records[c.ID] = c.Previous
		}
	}

	return &records, nil
}
