package tests

import (
	"encoding/csv"
	"github.com/jmoiron/sqlx"
	"github.com/sr-revops/brickhouse"
	"os"
	"testing"
)

func TestFull(t *testing.T) {
	f, err := os.Open("temp.large.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	raw, err := reader.ReadAll()

	data := map[string]interface{}{}
	for _, row := range raw {
		record := map[string]interface{}{}
		for i, cell := range row {
			record[raw[0][i]] = cell
		}
		data[row[18]] = record
	}

	db, err := sqlx.Open(
		"postgres",
		"postgresql://postgres:dev@localhost:5432/brickhouse?sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	if err := brickhouse.Update(db, "t1", &data); err != nil {
		t.Error(err)
	}
}
