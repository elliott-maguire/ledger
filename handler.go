package brickhouse

import "log"

// NewHandler creates a schedulable function that uses the given source to
// pull data, ensure a store exists for it, write the data, compare it
// to previous data, determine the changes between the two, then write those
// changes to the changes table.
func NewHandler(source Source) func() {
	return func() {
		name := source.GetName()
		db, err := source.GetDB()
		if err != nil {
			log.Fatal(err)
			return
		}
		fields := source.GetFields()
		store := Store{
			Name:   name,
			DB:     db,
			Fields: fields,
		}

		if err := store.Ensure(); err != nil {
			log.Fatal(err)
			return
		}

		data, err := source.GetData()
		if err != nil {
			log.Fatal(err)
			return
		}
		if err := store.Update(data); err != nil {
			log.Fatal(err)
			return
		}
	}
}
