package stream

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/simonks2016/stream/internal/jobCollector"
	"github.com/simonks2016/stream/operator/slidingWindow"
	"github.com/simonks2016/stream/stream"
)

func NewSlidingWindowJob[in any, out any](
	endpoint stream.Endpoint,
	job stream.Job[[]in, out],
	opts ...SlidingWindowOption[in],
) stream.JobOption {

	cfg := &SlidingWindowConfig[in]{
		WindowSize: 1000,
		QueueSize:  100,
		Filter:     func(in) bool { return true },
		KeyFunc:    func(in) string { return "default" },
	}

	for _, opt := range opts {
		opt(cfg)
	}

	onSlidingJobEndpoint := Inline(
		fmt.Sprintf(
			"job-%s.onSliding",
			func() string {
				if cfg.JobId == nil {
					return uuid.New().String()
				}
				return *cfg.JobId
			}()),
	)

	s := slidingWindow.NewSlidingWindowImpl[in, out](
		cfg.WindowSize,
		cfg.QueueSize,
		cfg.Interval,
		onSlidingJobEndpoint,
	)
	// 设置滑动时候的Job
	s.OnSlidingWindowJob(job)

	// Ingress Handler
	handler := func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {

		if msg.IsEmpty() {
			return nil
		}
		// 转化成in类型
		payload, ok := msg.Payload.(in)
		if !ok {
			var e in

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

		var key = "default"
		if cfg.KeyFunc != nil {
			key = cfg.KeyFunc(payload)
		}

		if cfg.Filter != nil {
			if !cfg.Filter(payload) {
				// 初始化收集器
				collector := jobCollector.NewJobCollector[out](msg, sink)
				// Drop Message
				collector.Drop(
					fmt.Sprintf("payload rejected by filter %s", key))
				return nil
			}
		}

		if !s.Add(key, payload) {
			return fmt.Errorf(
				"failed to add payload to sliding window, key=%s,input_type_name=%T",
				msg.Key,
				msg.Payload,
			)
		}
		return nil
	}

	return func(p stream.Pipeline) {
		p.On(endpoint, handler)
		s.Register(p)

		p.AddStartupHook(func(ctx context.Context) error {
			// 运行
			return s.Run(ctx)
		})
	}
}

type SlidingWindowOption[T any] func(config *SlidingWindowConfig[T])

type SlidingWindowConfig[T any] struct {
	KeyFunc    func(T) string
	Filter     func(T) bool
	WindowSize int
	QueueSize  int
	Interval   *time.Duration
	JobId      *string
}

func WithSlidingWindowSize[T any](size int) SlidingWindowOption[T] {
	return func(config *SlidingWindowConfig[T]) {
		config.WindowSize = size
	}
}

func WithSlidingWindowFilterFunc[T any](fn func(T) bool) SlidingWindowOption[T] {
	return func(config *SlidingWindowConfig[T]) {
		config.Filter = fn
	}
}

func WithSlidingWindowKeyFunc[T any](fn func(T) string) SlidingWindowOption[T] {
	return func(config *SlidingWindowConfig[T]) {
		config.KeyFunc = fn
	}
}

func WithSlidingWindowInterval[T any](interval time.Duration) SlidingWindowOption[T] {
	return func(config *SlidingWindowConfig[T]) {
		config.Interval = &interval
	}
}
