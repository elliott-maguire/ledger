package core

import (
	"testing"
	"time"
)

var uri = "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
var schema = "testing"
var fields = []string{"a", "b", "c"}

func TestReadRecords(t *testing.T) {
	_, err := ReadRecords(uri, schema)
	if err != nil {
		t.Error(err)
	}
}

func TestReadChanges(t *testing.T) {
	_, err := ReadChanges(uri, schema)
	if err != nil {
		t.Error(err)
	}
}

func TestReadArchive(t *testing.T) {
	set1 := map[string][]string{
		"foo": {"1", "2", "3"},
		"bar": {"1", "2", "3"},
	}
	set2 := map[string][]string{
		"foo": {"3", "2", "1"},
		"baz": {"1", "2", "3"},
	}
	set3 := map[string][]string{
		"foo": {"1", "2", "2"},
		"baz": {"1", "2", "3"},
		"zoo": {"2", "3", "2"},
	}
	set4 := map[string][]string{
		"baz": {"1", "2", "3"},
		"zoo": {"2", "3", "2"},
	}

	if err := WriteStore(uri, schema); err != nil {
		t.Error(err)
		return
	}

	_, err := handle(map[string][]string{}, set1)
	if err != nil {
		t.Error(err)
	}
	_, err = handle(set1, set2)
	if err != nil {
		t.Error(err)
	}
	timestamp, err := handle(set2, set3)
	if err != nil {
		t.Error(err)
	}
	_, err = handle(set3, set4)
	if err != nil {
		t.Error(err)
	}

	records, err := ReadArchive(uri, schema, timestamp)
	if err != nil {
		t.Error(err)
		return
	}

	if len(records) == 0 {
		t.Error("failed to get records")
		return
	}
}

func handle(a map[string][]string, b map[string][]string) (string, error) {
	if err := WriteRecords(uri, schema, fields, b); err != nil {
		return "", err
	}
	changes := GetChanges(a, b)
	if changes != nil {
		if err := WriteChanges(uri, schema, changes); err != nil {
			return "", err
		}
	}
	time.Sleep(1 * time.Minute)

	return changes[len(changes)-1].Timestamp, nil
}
