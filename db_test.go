package brickhouse

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgres driver
	"github.com/ory/dockertest"
)

var db *sqlx.DB

func setup() (*dockertest.Pool, *dockertest.Resource) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run(
		"postgres", "12", []string{"POSTGRES_DATABASE=postgres", "POSTGRES_PASSWORD=dev"},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		var err error
		uri := fmt.Sprintf(
			"postgresql://postgres:dev@localhost:%s/postgres?sslmode=disable",
			resource.GetPort("5432/tcp"),
		)
		db, err = sqlx.Open("postgres", uri)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	return pool, resource
}

func TestEnsure(t *testing.T) {
	pool, resource := setup()

	if err := Ensure(db, "test", "foo", "bar", "baz"); err != nil {
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

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	resource.Close()
}

func TestRead(t *testing.T) {
	pool, resource := setup()

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

	if err := Write(db, "test", Live, dIn, true); err != nil {
		t.Error(err)
	}

	dOut, err := Read(db, "test", Live)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(dIn, *dOut) {
		t.Error("read failed")
	}

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	resource.Close()
}

func TestWrite(t *testing.T) {
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

	if err := Write(db, "test", Live, d, true); err != nil {
		t.Error(err)
	}

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	resource.Close()
}
