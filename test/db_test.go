package test

import (
	"testing"

	"github.com/sr-revops/brickhouse/util"
)

func TestWriteData(t *testing.T) {
	incoming := []map[string]string{
		{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}
	if err := util.WriteData(db, "test", "test", incoming); err != nil {
		t.Error(err)
	}
}

func TestReadData(t *testing.T) {
	incoming := []map[string]string{
		{
			"a": "foo",
			"b": "bar",
			"c": "baz",
		},
	}
	if err := util.WriteData(db, "test", "test", incoming); err != nil {
		t.Error(err)
	}

	if data, err := util.ReadData(db, "test", "test"); err != nil {
		t.Error(err)
	} else if len(*data) != len(incoming) {
		t.Error("wrong data")
	}
}
