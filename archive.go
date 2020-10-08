package brickhouse

import "github.com/jmoiron/sqlx"

// GetArchive returns a copy of the given schema's records at the given time.
// The timestamp must be in the RFC3339 format, as that is the format used for stamping
// change sets as they are stored.
func GetArchive(db *sqlx.DB, schema string, timestamp string) (map[string][]string, error) {
	records, err := ReadRecords(db, schema, Live)
	if err != nil {
		return nil, err
	}
	changes, err := ReadChanges(db, schema)
	if err != nil {
		return nil, err
	}

	for i := range changes {
		change := changes[i]
		if change.Timestamp == timestamp {
			switch change.Operation {
			case Addition:
				delete(records, change.ID)
			case Modification:
				records[change.ID] = change.Previous
			case Deletion:
				records[change.ID] = change.Previous
			}
		}
	}

	return records, nil
}
