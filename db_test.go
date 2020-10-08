package brickhouse

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest"
)

var db *sqlx.DB

var (
	fields = []string{"a", "b", "c"}
	set1   = map[string][]string{
		"foo": {"1", "2", "3"},
		"bar": {"1", "2", "3"},
	}
	set2 = map[string][]string{
		"foo": {"3", "2", "1"},
		"baz": {"1", "2", "3"},
	}
	set3 = map[string][]string{
		"foo": {"1", "2", "2"},
		"baz": {"1", "2", "3"},
		"zoo": {"2", "3", "2"},
	}
	set4 = map[string][]string{
		"baz": {"1", "2", "3"},
		"zoo": {"2's", "3's", "2's"},
	}
)

func TestMain(m *testing.M) {
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

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestWriteStore(t *testing.T) {
	schema := "testwritestore"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}
}

func TestWriteRecords(t *testing.T) {
	schema := "testwriterecords"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	if err := WriteRecords(db, schema, Live, fields, set1); err != nil {
		t.Error(err)
		return
	}

	if err := WriteRecords(db, schema, Archive, fields, set1); err != nil {
		t.Error(err)
		return
	}
}

func TestWriteChanges(t *testing.T) {
	schema := "testwritechanges"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	changes := GetChanges(set1, set2)
	if len(changes) != 3 {
		t.Error("failed to get changes properly")
		return
	}

	if err := WriteChanges(db, schema, changes); err != nil {
		t.Error(err)
		return
	}
}

func TestReadRecords(t *testing.T) {
	schema := "testreadrecords"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	if err := WriteRecords(db, schema, Live, fields, set1); err != nil {
		t.Error(err)
		return
	}

	_, err := ReadRecords(db, schema, Live)
	if err != nil {
		t.Error(err)
	}
}

func TestReadChanges(t *testing.T) {
	schema := "testreadchanges"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	changes := GetChanges(set1, set2)
	if len(changes) != 3 {
		t.Error("failed to get changes properly")
		return
	}
	if err := WriteChanges(db, schema, changes); err != nil {
		t.Error(err)
		return
	}

	_, err := ReadChanges(db, schema)
	if err != nil {
		t.Error(err)
	}
}

func TestReadArchive(t *testing.T) {
	schema := "testreadarchive"
	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	if err := WriteStore(db, schema); err != nil {
		t.Error(err)
		return
	}

	handle := func(a map[string][]string, b map[string][]string) (string, error) {
		if err := WriteRecords(db, schema, Live, fields, b); err != nil {
			return "", err
		}
		changes := GetChanges(a, b)
		if changes != nil {
			if err := WriteChanges(db, schema, changes); err != nil {
				return "", err
			}
		}
		time.Sleep(1 * time.Second)

		return changes[len(changes)-1].Timestamp, nil
	}

	_, err := handle(map[string][]string{}, set1)
	if err != nil {
		t.Error(err)
	}
	_, err = handle(set1, set2)
	if err != nil {
		t.Error(err)
	}
	timestamp, err := handle(set2, set3)
	if err != nil {
		t.Error(err)
	}
	_, err = handle(set3, set4)
	if err != nil {
		t.Error(err)
	}

	archiveRecords, err := GetArchive(db, schema, timestamp)
	if err != nil {
		t.Error(err)
	}
	if err := WriteRecords(db, schema, Archive, fields, archiveRecords); err != nil {
		t.Error(err)
		return
	}

	records, err := ReadRecords(db, schema, Archive)
	if err != nil {
		t.Error(err)
		return
	}

	if len(records) == 0 {
		t.Error("failed to get records")
		return
	}
}
