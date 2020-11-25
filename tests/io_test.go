package tests

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/lib/pq" // postgres driver
	"github.com/sr-revops/brickhouse"
)

func TestUpdate(t *testing.T) {
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

	if err := brickhouse.Update(db, "TestUpdate", &testData); err != nil {
		t.Error(err)
	}
}

func TestRestore(t *testing.T) {
	targetTimes := map[string]time.Time{}
	testSets := map[string]map[string]interface{}{
		"base": map[string]interface{}{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j.schmoe@gmail.com",
			},
		},
		"cellModification": map[string]interface{}{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
			},
		},
		"cellAddition": map[string]interface{}{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
				"phone": "012-345-6789",
			},
		},
		"cellDeletion": map[string]interface{}{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
		},
		"recordAddition": map[string]interface{}{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
		"recordDeletion": map[string]interface{}{
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
	}

	for operation, data := range testSets {
		if err := brickhouse.Update(db, "TestRestore", &data); err != nil {
			t.Error(err)
		}
		targetTimes[operation] = time.Now()
	}

	for operation, targetTime := range targetTimes {
		if outData, err := brickhouse.Restore(db, "TestRestore", targetTime); err != nil {
			t.Error(err)
		} else {
			if !reflect.DeepEqual(*outData, testSets[operation]) {
				fmt.Println(*outData)
				fmt.Println(testSets[operation])
				t.Errorf("restore failed on %s", operation)
			}
		}
	}
}