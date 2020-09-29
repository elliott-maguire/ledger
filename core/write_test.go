package core

import (
	"testing"

	_ "github.com/lib/pq" // postgres driver
)

func TestWriteStore(t *testing.T) {
	uri := "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
	schema := "testing"

	if err := WriteStore(uri, schema); err != nil {
		t.Error(err)
		return
	}
}

func TestWriteRecords(t *testing.T) {
	uri := "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
	schema := "testing"
	fields := []string{"a", "b", "c"}
	records := map[string][]string{
		"foo": {"1", "2", "3"},
		"bar": {"1", "2", "3"},
	}

	if err := WriteRecords(uri, schema, fields, records); err != nil {
		t.Error(err)
		return
	}
}

func TestWriteChanges(t *testing.T) {
	uri := "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
	schema := "testing"

	current := map[string][]string{
		"foo": {"a", "b", "c"},
		"bar": {"a", "b", "c"},
	}
	incoming := map[string][]string{
		"foo": {"c", "b", "a"},
		"baz": {"a", "b", "c"},
	}

	changes := GetChanges(current, incoming)
	if len(changes) != 3 {
		t.Error("failed to get changes properly")
		return
	}

	if err := WriteChanges(uri, schema, changes); err != nil {
		t.Error(err)
		return
	}
}
