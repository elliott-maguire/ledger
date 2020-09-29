package examples

import (
	"math/rand"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgres driver
	"github.com/sr-revops/house/scheduler"
)

type fooBarSource struct{}

func (s fooBarSource) GetSchema() string {
	return "foobar"
}

func (s fooBarSource) GetDB() (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", "postgresql://localhost:5432/foobar")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s fooBarSource) GetSchedule() string {
	return "* * * * *"
}

func (s fooBarSource) GetData() ([]string, map[string][]string, error) {
	fields := []string{"foo", "bar"}
	records := map[string][]string{
		"1": {getRandomString(8), getRandomString(8)},
		"2": {getRandomString(8), getRandomString(8)},
	}

	return fields, records, nil
}

func getRandomString(n int) string {
	runes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

func main() {
	src := fooBarSource{}

	sch := scheduler.Scheduler{}
	sch.Register(src)

	sch.Start()
}
