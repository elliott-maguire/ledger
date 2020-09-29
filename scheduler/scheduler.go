package scheduler

import "github.com/robfig/cron"

// Scheduler stores any user-registered Source implementations and
// exposes a method for running them.
type Scheduler struct {
	Sources []Source
}

// Register adds a Source to the Scheduler.
func (s *Scheduler) Register(source Source) {
	s.Sources = append(s.Sources, source)
}

// Start adds all the registered Sources to the cron scheduler and blocks.
func (s Scheduler) Start() {
	c := cron.New()
	for _, source := range s.Sources {
		c.AddFunc(source.GetSchedule(), NewHandler(source))
	}
	c.Run()
}
