package test

import (
	"testing"
	"time"

	"github.com/sr-revops/brickhouse"
)

func TestEnsure(t *testing.T) {
	s := brickhouse.Store{
		Name:   "test",
		DB:     db,
		Fields: []string{"foo", "bar", "baz"},
	}
	if err := s.Ensure(); err != nil {
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

func TestUpdate(t *testing.T) {
	s := brickhouse.Store{
		Name:   "test",
		DB:     db,
		Fields: []string{"foo", "bar", "baz"},
	}
	if err := s.Ensure(); err != nil {
		t.Error(err)
	}

	r := map[string][]string{
		"1": {"a", "b", "c"},
		"2": {"a", "b", "c"},
	}
	if err := s.Update(r); err != nil {
		t.Error(err)
	}
}

func TestRetrieve(t *testing.T) {
	s := brickhouse.Store{
		Name:   "test",
		DB:     db,
		Fields: []string{"foo", "bar", "baz"},
	}
	if err := s.Ensure(); err != nil {
		t.Error(err)
	}

	r0 := map[string][]string{
		"1": {"a", "b", "c"},
		"2": {"a", "b", "c"},
	}
	r1 := map[string][]string{
		"1": {"c", "b", "a"},
		"2": {"a", "b", "c"},
		"3": {"a", "b", "c"},
	}
	r2 := map[string][]string{
		"1": {"c", "b", "a"},
		"3": {"a", "b", "c"},
	}

	if err := s.Update(r0); err != nil {
		t.Error(err)
	}
	time.Sleep(2 * time.Second)
	if err := s.Update(r1); err != nil {
		t.Error(err)
	}
	time.Sleep(2 * time.Second)
	if err := s.Update(r2); err != nil {
		t.Error(err)
	}
	time.Sleep(2 * time.Second)

	archive, err := s.Retrieve(time.Now().Add(-4 * time.Second))
	if err != nil {
		t.Error(err)
	}

	if len(*archive) != 3 {
		t.Error("failed to get correct archive")
	}
}
