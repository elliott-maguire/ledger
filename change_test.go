package brickhouse

import (
	"testing"
)

func TestGetChanges(t *testing.T) {
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
		t.Error("failed")
	}
}
