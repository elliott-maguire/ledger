package tests

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgres driver
	"github.com/sr-revops/brickhouse"
)

func TestUpdate(t *testing.T) {
	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/brickhouse?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	testData := map[string]interface{}{
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

	if err := brickhouse.Update(db, "testupdate", testData); err != nil {
		t.Error(err)
	}
}

func TestRestore(t *testing.T) {
	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/brickhouse?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	targetTimes := map[string]time.Time{}
	testSets := map[string]map[string]interface{}{
		"base": {
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j.schmoe@gmail.com",
			},
		},
		"cellModification": {
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
			},
		},
		"cellAddition": {
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
				"phone": "012-345-6789",
			},
		},
		"cellDeletion": {
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
		},
		"recordAddition": {
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
		"recordDeletion": {
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
	}

	for operation, data := range testSets {
		if err := brickhouse.Update(db, "TestRestore", data); err != nil {
			t.Error(err)
		}
		targetTimes[operation] = time.Now()
	}

	for operation, targetTime := range targetTimes {
		if outData, err := brickhouse.Restore(db, "TestRestore", targetTime); err != nil {
			t.Error(err)
		} else {
			if !reflect.DeepEqual(outData, testSets[operation]) {
				fmt.Println(outData)
				fmt.Println(testSets[operation])
				t.Errorf("restore failed on %s", operation)
			}
		}
	}
}
