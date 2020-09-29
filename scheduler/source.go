package scheduler

// Source is the interface that must be written to for sources to be warehoused.
type Source interface {
	GetSchema() string
	GetURI() string
	GetSchedule() string
	GetData() (fields []string, data map[string][]string, err error)
}
