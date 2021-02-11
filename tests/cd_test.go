package tests

import (
	"testing"

	"github.com/elliott-maguire/ledger"
)

func TestCompare(t *testing.T) {
	testSets := []map[string]interface{}{
		{},
		{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j.schmoe@gmail.com",
			},
		},
		{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
			},
		},
		{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"email": "j@protonmail.ch",
				"phone": "012-345-6789",
			},
		},
		{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
		},
		{
			"abc": map[string]interface{}{
				"name":  "Joe Schmoe",
				"phone": "012-345-6789",
			},
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
		{
			"def": map[string]interface{}{
				"name":  "Moe Schloe",
				"phone": "987-654-3210",
			},
		},
	}

	allChanges := make([]ledger.Change, 0)
	lastData := make(map[string]interface{})
	for _, data := range testSets {
		changes := ledger.Compare(lastData, data)
		allChanges = append(allChanges, changes...)
		lastData = data
	}

	if len(allChanges) != 6 {
		t.Error("failed")
	}
}
