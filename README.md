# brickhouse

Brickhouse is a little data warehousing tool that uses set comparison to generate linear atomic change lists for connected data sources. It allows the user to keep a history of any two-dimensional data sets. It is written in Go and runs on top of Postgres.

## Rationale

I wrote this tool to address the need for a centralized history for some of our team's Salesforce reports. I took an open-source approach to ensure clean and flexible design. We wanted to build something in-brickhouse because other SaaS warehousing solutions out there cost a fortune, and for good reason; their features sets are immense, but we just didn't need that much firepower for our use case.

If you find yourself needing to keep track of changes on a given flat data set, this is a great tool. If you have a use case that isn't quite supported but would fit in well, please open up a issue or submit a PR and maybe we can make this into something better!

## Usage

The prerequisites are Go and Postgres. You need the language to define your custom sources, unless you're using another tool that uses configuration files to do the same thing (that's what we do, but that bit isn't open-source), and you need Postgres to send data to.

First thing we need to do is make a struct that we can use to implement the `scheduler.Source` interface.

```go
type fooBarSource struct{}
```

You can put whatever you want in here as far as fields go. For our implementation, we have a title field, a field for the given Salesforce report ID, an interval value for scheduling, and all of those things are tagged for reading out of a YAML. The possibilities are as endless as a struct, so do whatever you need to with it.

Next, we need to write to the interface. It requires four methods: `GetSchema`, which should return the database-safe name of what you want the data for the particular source to be stored under, `GetURI`, which should return a valid Postgres database URI, `GetSchedule`, which should return a valid [cron](https://en.wikipedia.org/wiki/Cron) string for scheduling calls to the source, and then `GetData`, which should return a string array of the field names for the data set, a string array map of the data, the keys being unique identifiers, and an error.

```go
func (s fooBarSource) GetSchema() string {
	return "foobar"
}

func (s fooBarSource) GetURI() string {
	return "postgresql://localhost:5432/foobar"
}

func (s fooBarSource) GetSchedule() string {
	return "* * * * *"
}

func (s fooBarSource) GetData() ([]string, *map[string][]string, error) {
	fields := []string{"foo", "bar"}
	records := map[string][]string{
		"1": {getRandomString(8), getRandomString(8)},
		"2": {getRandomString(8), getRandomString(8)},
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
}
```

This implementation runs every minute and stores random data to demonstrate change tracking. The URI should point to a database that you have set up on your own machine. All we need to do now is add our source to the daemon and run it.

```go
func main() {
	src := fooBarSource{}

	sch := scheduler.Scheduler{}
	sch.Register(src)

	sch.Start()
}
```

Each source has a schema named after it with two tables therein, `records` and `changes`. When each call happens, the existing data is compared to incoming data to evaluate for additions, modifications, and deletions. Those operations are then collected and written to `changes`, and the `records` table is truncated and re-populated with the incoming data.

To retrieve a past version of a given source's data, use the `core.ReadArchive` function, which takes a URI and schema name, as well as an RFC3339 date string, and returns a `map[string][]string` with the data from that date. This is the only mechanism included in the API for traversing the change list of a given source, but can be used in numerous ways. For example, we use it on an API endpoint to pull out CSV or JSON versions of historical data.