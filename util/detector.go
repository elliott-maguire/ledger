package util

import (
	"reflect"
	"time"
)

type Dataset map[string]map[string]string

type Operation int

const (
	Addition Operation = iota
	Modification
	Deletion
)

type RowPair struct {
	CurrentValue  map[string]string
	IncomingValue map[string]string
}

type CellPair struct {
	CurrentValue  string
	IncomingValue string
}

type RowChange struct {
	ID            string
	Timestamp     string
	Operation     Operation
	CurrentValue  map[string]string
	IncomingValue map[string]string
}

type CellChange struct {
	ID            string
	Key           string
	Timestamp     string
	Operation     Operation
	CurrentValue  string
	IncomingValue string
}

func DetectChanges(current Dataset, incoming Dataset) *[]Change {
	var rowChanges []RowChange
	var cellChanges []CellChange
	var rowPairs []RowPair
	var cellPairs []CellPair

	for ckey, cval := range current {
		for ikey, ival := range incoming {
			if ckey == ikey {
				delete(current, ckey)
				delete(incoming, ikey)

				if !reflect.DeepEqual(cval, ival) {
					rowPairs = append(rowPairs, RowPair{cval, ival})
				}
			}
		}
	}

	for id, record := range current {
		rowChanges = append(rowChanges, RowChange{
			ID:            id,
			Timestamp:     time.Now().Format(time.RFC3339),
			Operation:     Deletion,
			CurrentValue:  record,
			IncomingValue: nil,
		})
	}

	for id, record := range incoming {
		rowChanges = append(rowChanges, RowChange{
			ID:            id,
			Timestamp:     time.Now().Format(time.RFC3339),
			Operation:     Addition,
			CurrentValue:  nil,
			IncomingValue: record,
		})
	}

	for _, p := range rowPairs {
		for ckey, cval := range p.CurrentValue {
			for ikey, ival := range p.IncomingValue {
				if ckey == ikey {
					delete(p.CurrentValue, ckey)
					delete(p.IncomingValue, ikey)

					if cval != ival {
						cellPairs = append(cellPairs, CellPair{
							CurrentValue:  cval,
							IncomingValue: ival,
						})
					}
				}
			}
		}
	}

	return nil
}
