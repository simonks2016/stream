package scheduler

import (
	"time"

	"github.com/simonks2016/stream/stream"
)

type DefaultSchedulerJob struct {
	SchedularName  string
	Duration       time.Duration
	TargetEndPoint stream.Endpoint
	Callback       func() any
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

func (d *DefaultSchedulerJob) BuildMessage(iteration int) stream.Message[any] {
	return stream.NewMessage[any](
		stream.SchedulerEvent{
			Iteration: iteration,
			Value: func() any {
				if d.Callback != nil {
					return d.Callback()
				}
				return nil
			},
		},
	)
}
