package stream

import (
	"context"
	"time"
)

type Scheduler interface {
	Register(pipeline Pipeline) Scheduler
	On(jobs ...SchedulerJob) Scheduler
	Run(ctx context.Context)
	Stop()
}

type SchedulerJob interface {
	Target() Endpoint
	Interval() time.Duration
	BuildMessage() Message[any]
	Name() string
}
