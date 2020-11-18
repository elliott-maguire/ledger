package tests

import (
	"testing"

	_ "github.com/lib/pq" // postgres driver
	"github.com/sr-revops/brickhouse"
)

func TestUpdate(t *testing.T) {
	d := map[string]interface{}{
		"1": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"2": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"3": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}

	if err := brickhouse.Update(db, "test", &d); err != nil {
		t.Error(err)
	}
}
