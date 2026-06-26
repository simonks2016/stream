package rolldingWindow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/simonks2016/stream/internal/jobCollector"
	"github.com/simonks2016/stream/stream"
)

type RollingWindow[in any, out any] struct {
	windowDuration          time.Duration
	windowStep              time.Duration
	bucket                  *rollingBucket[in]
	sink                    stream.Sink
	windowTriggeredEndpoint stream.Endpoint
	ingressEndPoint         stream.Endpoint
	job                     stream.Job[[]in, out]
}

func NewRollingWindow[in any, out any](
	ingressEndpoint stream.Endpoint,
	job stream.Job[[]in, out],
	opts ...Option,
) *RollingWindow[in, out] {

	var cfg = RollingWindowConfig{
		WindowDuration: time.Minute,
		WindowStep:     time.Minute,
		JobId:          uuid.New().String(),
	}

	for _, opt := range opts {
		opt(&cfg)
	}
	// 设置窗口触发之后的函数
	windowTriggeredEndpoint := stream.InlineEndpoint(fmt.Sprintf("rolldingWindowJob-%s.windowTriggered", cfg.JobId))

	return &RollingWindow[in, out]{
		windowDuration:          cfg.WindowDuration,
		windowStep:              cfg.WindowStep,
		ingressEndPoint:         ingressEndpoint,
		windowTriggeredEndpoint: windowTriggeredEndpoint,
		job:                     job,
		bucket:                  newRollingBucket[in](cfg.WindowDuration, cfg.WindowStep),
	}
}

func (r *RollingWindow[in, out]) currentWindowKey(now time.Time) windowKey {
	endMs := now.UnixMilli()
	startMs := endMs - r.windowDuration.Milliseconds()

	return windowKey{
		WindowStartMs: startMs,
		WindowEndMs:   endMs,
	}
}

func (r *RollingWindow[in, out]) save() stream.Handler {
	return func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {
		if msg.IsEmpty() {
			return nil
		}

		payload, ok := msg.Payload.(in)
		if !ok {
			var expected in
			actualType := "<nil>"
			if msg.Payload != nil {
				actualType = reflect.TypeOf(msg.Payload).String()
			}
			return fmt.Errorf(
				"rolling window save payload type mismatch, key=%s, actual_type=%s, expected_type=%T",
				msg.Key,
				actualType,
				expected,
			)
		}

		// 当前版本按 processing time 入桶。
		// 后续如果要 event_time，可以给 RollingWindow 增加 timestampExtractor func(in) int64。
		r.bucket.add(time.Now().UnixMilli(), payload)
		return nil
	}
}

func (r *RollingWindow[in, out]) scheduler(ctx context.Context) error {
	if r.sink == nil {
		return errors.New("rolling window sink is nil")
	}

	ticker := time.NewTicker(r.windowStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case now := <-ticker.C:
			wk := r.currentWindowKey(now)
			data := r.bucket.get(wk)
			if len(data) == 0 {
				continue
			}

			msg := stream.NewMessage[any](data)
			msg.Key = fmt.Sprintf("%d-%d", wk.WindowStartMs, wk.WindowEndMs)

			if err := r.sink(r.windowTriggeredEndpoint, msg); err != nil {
				return err
			}
		}
	}
}

func (r *RollingWindow[in, out]) Register(pipeline stream.Pipeline) {
	if r.job == nil {
		panic("rolling window job is nil")
	}

	if r.bucket == nil {
		r.bucket = newRollingBucket[in](r.windowDuration, r.windowStep)
	}

	r.sink = pipeline.Publish

	// 1. ingress endpoint -> save bucket
	pipeline.On(
		r.ingressEndPoint,
		r.save(),
	)

	// 2. storage endpoint -> execute window job
	pipeline.On(
		r.windowTriggeredEndpoint,
		func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {
			if msg.IsEmpty() {
				return nil
			}

			payload, ok := msg.Payload.([]in)
			if !ok {
				var expected []in
				actualType := "<nil>"
				if msg.Payload != nil {
					actualType = reflect.TypeOf(msg.Payload).String()
				}

				return fmt.Errorf(
					"rolling window job payload type mismatch, key=%s, actual_type=%s, expected_type=%T",
					msg.Key,
					actualType,
					expected,
				)
			}

			ingress := stream.NewMessage[[]in](payload)
			collector := jobCollector.NewJobCollector[out](msg, sink)

			if err := r.job.Process(ctx, ingress, collector); err != nil {
				return err
			}

			if errorsList := collector.HasErrors(); len(errorsList) > 0 {
				return errors.Join(errorsList...)
			}

			return nil
		},
	)
}

func (r *RollingWindow[in, out]) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- r.bucket.run(ctx)
	}()

	go func() {
		errCh <- r.scheduler(ctx)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}
