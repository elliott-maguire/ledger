package tests

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/elliott-maguire/ledger"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func TestUpdate(t *testing.T) {
	label := fmt.Sprintf("update%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/ledger?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	data := map[string]map[string]interface{}{
		"1": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"2": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"3": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}
}

func TestRepeatUpdate(t *testing.T) {
	label := fmt.Sprintf("update%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/ledger?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	data := map[string]map[string]interface{}{
		"1": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"2": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
		"3": {
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}
}

func TestUpdateFromFile(t *testing.T) {
	label := fmt.Sprintf("update%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/ledger?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	f, err := os.Open("./temp.new.csv")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	raw, err := reader.ReadAll()
	if err != nil {
		t.Error(err)
	}

	key := "Opportunity ID"
	keyIndex := 0
	for i, cell := range raw[0] {
		if cell == key {
			keyIndex = i
		}
	}

	data := make(map[string]map[string]interface{})
	for _, row := range raw {
		record := map[string]interface{}{}
		for i, cell := range row {
			record[raw[0][i]] = cell
		}
		data[row[keyIndex]] = record
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}
}

func TestRepeatUpdateFromFile(t *testing.T) {
	label := fmt.Sprintf("doubleupdate%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/ledger?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	f, err := os.Open("./temp.old.csv")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	raw, err := reader.ReadAll()
	if err != nil {
		t.Error(err)
	}

	key := "Opportunity ID"
	keyIndex := 0
	for i, cell := range raw[0] {
		if cell == key {
			keyIndex = i
		}
	}

	data := make(map[string]map[string]interface{})
	for _, row := range raw {
		record := map[string]interface{}{}
		for i, cell := range row {
			record[raw[0][i]] = cell
		}
		data[row[keyIndex]] = record
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}

	f, err = os.Open("./temp.new.csv")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	reader = csv.NewReader(f)
	raw, err = reader.ReadAll()
	if err != nil {
		t.Error(err)
	}

	key = "Opportunity ID"
	keyIndex = 0
	for i, cell := range raw[0] {
		if cell == key {
			keyIndex = i
		}
	}

	data = make(map[string]map[string]interface{})
	for _, row := range raw {
		record := map[string]interface{}{}
		for i, cell := range row {
			record[raw[0][i]] = cell
		}
		data[row[keyIndex]] = record
	}

	if err := ledger.Update(db, label, data); err != nil {
		t.Error(err)
	}
}

func TestRestore(t *testing.T) {
	label := fmt.Sprintf("restore%d", time.Now().Unix())

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/ledger?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	targetTimes := []time.Time{}
	testSets := []map[string]map[string]interface{}{
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

	for _, data := range testSets {
		if err := ledger.Update(db, label, data); err != nil {
			t.Error(err)
		}
		targetTimes = append(targetTimes, time.Now())
	}

	for i, targetTime := range targetTimes {
		if outData, err := ledger.Restore(db, label, targetTime); err != nil {
			t.Error(err)
		} else {
			if !reflect.DeepEqual(outData, testSets[i]) {
				fmt.Print("out:")
				fmt.Println(outData)
				fmt.Print("in: ")
				fmt.Println(testSets[i])
				fmt.Print("\n")
				t.Errorf("restore failed on %d", i)
			}
		}
	}
}
