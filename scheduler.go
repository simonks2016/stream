package stream

import (
	"time"

	"github.com/simonks2016/stream/internal/scheduler"
	"github.com/simonks2016/stream/stream"
)

func NewScheduler() stream.Scheduler {
	return scheduler.NewScheduler()
}

func WrapSchedulerJob(
	opts ...scheduler.SchedulerOption,
) stream.SchedulerJob {

	job := &scheduler.DefaultSchedulerJob{
		Duration: time.Minute, // 给个默认值，避免没传 interval
		Callback: nil,
	}

	for _, opt := range opts {
		opt(job)
	}

	if job.SchedularName == "" {
		job.SchedularName = "scheduler-job"
	}
	if job.Duration <= 0 {
		panic("scheduler job interval must be > 0")
	}
	if job.TargetEndPoint.Name == "" {
		panic("scheduler job target endpoint is required")
	}
	if job.TargetEndPoint.Kind != stream.InlineKind {
		panic("scheduler job target endpoint kind must be inline")
	}
	if job.Callback == nil {
		panic("scheduler job message factory is required")
	}

	return job
}

func WithName(name string) scheduler.SchedulerOption {
	return func(o *scheduler.DefaultSchedulerJob) {
		o.SchedularName = name
	}
}

func WithInterval(duration time.Duration) scheduler.SchedulerOption {
	return func(o *scheduler.DefaultSchedulerJob) {
		o.Duration = duration
	}
}

func WithTargetEndPoint(endPoint stream.Endpoint) scheduler.SchedulerOption {
	return func(o *scheduler.DefaultSchedulerJob) {
		o.TargetEndPoint = endPoint
	}
}

func WithMessageFactory[out any](messageFactory func() stream.Message[out]) scheduler.SchedulerOption {
	return func(o *scheduler.DefaultSchedulerJob) {
		o.Callback = func() stream.Message[any] {
			msg := messageFactory()
			return stream.Message[any]{
				Payload:     any(msg.Payload),
				Ts:          msg.Ts,
				Key:         msg.Key,
				WatermarkTs: msg.WatermarkTs,
				IngestTime:  msg.IngestTime,
				SinkTime:    msg.SinkTime,
			}
		}
	}
}
