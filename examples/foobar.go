package examples

import (
	"math/rand"

	"github.com/sr-revops/house/scheduler"
)

type fooBarSource struct{}

func (s fooBarSource) GetSchema() string {
	return "foobar"
}

func (s fooBarSource) GetURI() string {
	return "postgresql://developer:development@localhost:5432/foobar?sslmode=disable"
}

func (s fooBarSource) GetSchedule() string {
	return "* * * * *"
}

func (s fooBarSource) GetData() ([]string, *map[string][]string, error) {
	fields := []string{"foo", "bar"}
	records := map[string][]string{
		"1": {getRandomString(4), getRandomString(8)},
		"2": {getRandomString(4), getRandomString(8)},
	}

	return fields, &records, nil
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
