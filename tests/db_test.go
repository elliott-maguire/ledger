package tests

import (
	"reflect"
	"testing"

	_ "github.com/lib/pq" // postgres driver
	"github.com/sr-revops/brickhouse"
)

func TestEnsure(t *testing.T) {
	if err := brickhouse.Ensure(db, "test"); err != nil {
		t.Error(err)
	}

	q := "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'test';"
	if r, err := db.Exec(q); err != nil {
		t.Error(err)
	} else {
		if c, err := r.RowsAffected(); err != nil {
			t.Error(err)
		} else {
			if c == 0 {
				t.Error("test schema not found")
			}
		}
	}
}

func TestRead(t *testing.T) {
	dIn := map[string]interface{}{
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

	if err := brickhouse.Write(db, "test", brickhouse.Live, &dIn, true); err != nil {
		t.Error(err)
	}

	dOut, err := brickhouse.Read(db, "test", brickhouse.Live)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(dIn, *dOut) {
		t.Error("read failed")
	}
}

func TestWrite(t *testing.T) {
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

	if err := brickhouse.Write(db, "test", brickhouse.Live, &d, true); err != nil {
		t.Error(err)
	}
}
