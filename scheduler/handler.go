package scheduler

import (
	"log"

	"github.com/lib/pq"
	"github.com/sr-revops/brickhouse/core"
)

// NewHandler creates a schedulable function that uses the given source to
// pull data, ensure a store exists for it, write the data, compare it
// to previous data, determine the changes between the two, then write those
// changes to the changelog.
func NewHandler(source Source) func() {
	return func() {
		schema := source.GetSchema()
		db, err := source.GetDB()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Loading: %s\n", schema)

		if err := core.WriteStore(db, schema); err != nil {
			log.Fatal(err)
		}

		current, err := core.ReadRecords(db, schema)
		if err != nil {
			if pqErr := err.(*pq.Error); pqErr.Code != "42P01" {
				log.Fatal(err)
			}
		}

		fields, incoming, err := source.GetData()
		if err != nil {
			log.Fatal(err)
		}
		if err := core.WriteRecords(db, schema, fields, incoming); err != nil {
			log.Fatal(err)
		}

		changes := core.GetChanges(current, incoming)
		if changes != nil {
			if err := core.WriteChanges(db, schema, changes); err != nil {
				log.Fatal(err)
			}
		}

		log.Printf("Done: %s (%d records, %d changes)", schema, len(incoming), len(changes))

		db.Close()
		incoming = nil
		current = nil
		changes = nil
	}
}
