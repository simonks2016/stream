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
	BuildMessage(int) Message[any]
	Name() string
}

type SchedulerEvent struct {
	Iteration int `json:"iteration"`
	Value     any `json:"value"`
}
