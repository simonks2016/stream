package stream

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/simonks2016/stream/internal/jobCollector"
	"github.com/simonks2016/stream/internal/scheduler"
	"github.com/simonks2016/stream/stream"
)

/*
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
*/

func NewSchedulerJob[out any](
	duration time.Duration,
	job stream.Job[stream.SchedulerEvent, out],
) stream.JobOption {

	schedulerId := uuid.New().String()
	endpoint := Inline(fmt.Sprintf("sc-%s", schedulerId))
	s1 := scheduler.NewScheduler()
	// 创建一个新的定时任务
	schedularJob := &scheduler.DefaultSchedulerJob{
		Duration:       duration,
		Callback:       nil,
		TargetEndPoint: endpoint,
		SchedularName:  fmt.Sprintf("scheduler-%s", schedulerId),
	}

	handler := func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {

		if msg.IsEmpty() {
			return nil
		}
		// 转化成in类型
		payload, ok := msg.Payload.(stream.SchedulerEvent)
		if !ok {
			var e stream.SchedulerEvent

			actualType := "<nil>"
			if msg.Payload != nil {
				actualType = reflect.TypeOf(msg.Payload).String()
			}

			return fmt.Errorf(
				"payload type mismatch, key=%s, actual_type=%s, expected_type=%T",
				msg.Key,
				actualType,
				e,
			)
		}
		collector := jobCollector.NewJobCollector[out](msg, sink)
		// 处理
		if err := job.Process(ctx, stream.NewMessage[stream.SchedulerEvent](payload), collector); err != nil {
			return err
		}
		// 检查是否有错误
		if errorsList := collector.HasErrors(); len(errorsList) > 0 {
			return errors.Join(errorsList...)
		}
		return nil
	}

	return func(p stream.Pipeline) {
		p.On(endpoint, handler)
		s1.Register(p)
		s1.On(schedularJob)

		// 启动
		p.AddStartupHook(func(ctx context.Context) error {
			s1.Run(ctx)
			return nil
		})
	}

}
