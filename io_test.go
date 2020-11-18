package brickhouse

import (
	"log"
	"testing"

	_ "github.com/lib/pq" // postgres driver
)

func TestUpdate(t *testing.T) {
	pool, resource := setup()

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

	if err := Update(db, "test", &d); err != nil {
		t.Error(err)
	}

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	resource.Close()
}
