package scheduler

import (
	"time"

	"github.com/simonks2016/stream/stream"
)

type DefaultSchedulerJob struct {
	SchedularName  string
	Duration       time.Duration
	TargetEndPoint stream.Endpoint
	Callback       func() stream.Message[any]
}

func (d *DefaultSchedulerJob) Interval() time.Duration {
	return d.Duration
}

func (d *DefaultSchedulerJob) Target() stream.Endpoint {
	return d.TargetEndPoint
}

func (d *DefaultSchedulerJob) Name() string {
	return d.SchedularName
}

func (d *DefaultSchedulerJob) BuildMessage() stream.Message[any] {
	if d.Callback == nil {
		return stream.Message[any]{}
	}
	return d.Callback()
}
