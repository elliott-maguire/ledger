package core

import (
	"testing"
)

func TestReadRecords(t *testing.T) {
	uri := "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
	schema := "testing"

	_, err := ReadRecords(uri, schema)
	if err != nil {
		t.Error(err)
	}
}

func TestReadChanges(t *testing.T) {
	uri := "postgres://developer:development@localhost:5432/syllogi_test?sslmode=disable"
	schema := "testing"

	_, err := ReadChanges(uri, schema)
	if err != nil {
		t.Error(err)
	}
}
