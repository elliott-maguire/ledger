package scheduler

import (
	"github.com/lib/pq"
	"github.com/sr-revops/house/core"
)

// NewHandler creates a schedulable function that uses the given source to
// pull data, ensure a store exists for it, write the data, compare it
// to previous data, determine the changes between the two, then write those
// changes to the changelog.
func NewHandler(source Source) func() {
	return func() {
		schema := source.GetSchema()
		uri := source.GetURI()
		if err := core.WriteStore(uri, schema); err != nil {
			return
		}

		current, err := core.ReadRecords(uri, schema)
		if err != nil {
			if pqErr := err.(*pq.Error); pqErr.Code != "42P01" {
				return
			}
		}

		fields, incoming, err := source.GetData()
		if err != nil {
			return
		}
		if err := core.WriteRecords(uri, schema, fields, incoming); err != nil {
			return
		}

		changes := core.GetChanges(*current, *incoming)
		if *changes != nil {
			if err := core.WriteChanges(uri, schema, changes); err != nil {
				return
			}
		}
	}
}
