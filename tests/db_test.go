package tests

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sr-revops/bricks"
)

func TestRead(t *testing.T) {
	label := fmt.Sprintf("read%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/bricks?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	d1 := map[string]interface{}{
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
	d2 := map[string]interface{}{
		"1": map[string]interface{}{
			"a": "zoo",
			"b": "bar",
			"c": "baz",
		},
		"3": map[string]interface{}{
			"a": "bar",
			"b": "baz",
			"c": "zoo",
		},
		"4": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}
	d3 := map[string]interface{}{
		"1": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"d": "foo",
		},
		"3": map[string]interface{}{
			"a": "zar",
			"b": "baz",
			"d": "bar",
		},
		"4": map[string]interface{}{
			"a": "foo",
			"b": "bar",
			"d": "baz",
		},
	}

	if err := bricks.Write(db, label, bricks.Live, d1); err != nil {
		t.Error(err)
	}

	d1Out, err := bricks.Read(db, label, bricks.Live)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(d1, d1Out) {
		fmt.Println(d1)
		fmt.Println(d1Out)
		t.Error("d1 read failed")
	} else {
		t.Log("d1 read succeeded")
	}

	if err := bricks.Write(db, label, bricks.Live, d2); err != nil {
		t.Error(err)
	}

	d2Out, err := bricks.Read(db, label, bricks.Live)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(d2, d2Out) {
		fmt.Println(d2)
		fmt.Println(d2Out)
		t.Error("d2 read failed")
	} else {
		t.Log("d2 read succeeded")
	}

	if err := bricks.Write(db, label, bricks.Live, d3); err != nil {
		t.Error(err)
	}

	d3out, err := bricks.Read(db, label, bricks.Live)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(d3, d3out) {
		fmt.Println(d3)
		fmt.Println(d3out)
		t.Error("d3 read failed")
	} else {
		t.Log("d3 read succeeded")
	}
}

func TestWrite(t *testing.T) {
	label := fmt.Sprintf("write%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/bricks?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

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

	if err := bricks.Write(db, label, bricks.Live, d); err != nil {
		t.Error(err)
	}
}
