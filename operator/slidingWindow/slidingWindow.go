package slidingWindow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/simonks2016/stream/internal/jobCollector"
	"github.com/simonks2016/stream/stream"
)

type SlidingWindow[in any, out any] interface {
	Add(key string, value in) bool
	OnSlidingWindowJob(job stream.Job[[]in, out])
	Run(context.Context) error
	Register(pipeline stream.Pipeline)
}

type WindowEvent[T any] struct {
	Key   string
	Value T
}

type SlidingWindowImpl[in any, out any] struct {
	windowSize int
	windows    map[string]*ringWindow[in]
	queue      chan WindowEvent[in]
	job        stream.Job[[]in, out]
	interval   *time.Duration
	sink       stream.Sink
	endpoint   stream.Endpoint
}

func (s *SlidingWindowImpl[in, out]) Register(pipeline stream.Pipeline) {
	//TODO implement me
	if s.job == nil {
		panic("sliding window job is nil")
	}
	s.sink = pipeline.Publish
	// 注册inline
	pipeline.On(
		s.endpoint,
		func() stream.Handler {
			return func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {

				fmt.Println("OK")
				if msg.IsEmpty() {
					return nil
				}
				// 转化成in类型
				payload, ok := msg.Payload.([]in)
				if !ok {
					var e []in
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
				ingress := stream.NewMessage[[]in](payload)
				//
				collector := jobCollector.NewJobCollector[out](msg, sink)
				// 处理
				if err := s.job.Process(ctx, ingress, collector); err != nil {
					return err
				}
				// 检查是否有错误
				if errorsList := collector.HasErrors(); len(errorsList) > 0 {
					return errors.Join(errorsList...)
				}
				return nil
			}
		}())
}

func NewSlidingWindowImpl[in any, out any](
	size int,
	queueSize int,
	interval *time.Duration,
	targetEndpoint stream.Endpoint,
) SlidingWindow[in, out] {
	if size <= 0 {
		panic("window size must be greater than 0")
	}

	if queueSize <= 0 {
		queueSize = 1024
	}

	return &SlidingWindowImpl[in, out]{
		windowSize: size,
		windows:    make(map[string]*ringWindow[in]),
		queue:      make(chan WindowEvent[in], queueSize),
		interval:   interval,
		endpoint:   targetEndpoint,
	}
}

func (s *SlidingWindowImpl[in, out]) Add(key string, value in) bool {
	select {
	case s.queue <- WindowEvent[in]{
		Key:   key,
		Value: value,
	}:
		return true
	default:
		return false
	}
}

func (s *SlidingWindowImpl[in, out]) OnSlidingWindowJob(job stream.Job[[]in, out]) {
	//TODO implement me
	s.job = job
}

func (s *SlidingWindowImpl[in, out]) Run(ctx context.Context) error {
	var ticker *time.Ticker
	var tickerC <-chan time.Time

	if s.interval != nil {
		ticker = time.NewTicker(*s.interval)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt := <-s.queue:
			s.handleEvent(evt)
		case <-tickerC:
			s.triggerAll()
		}
	}
}

func (s *SlidingWindowImpl[in, out]) handleEvent(evt WindowEvent[in]) {

	w, ok := s.windows[evt.Key]
	if !ok {
		w = &ringWindow[in]{
			buf: make([]in, s.windowSize),
		}
		s.windows[evt.Key] = w
	}

	full, slid := w.add(evt.Value)

	if full && slid && s.sink != nil {
		fmt.Println("Sink")
		err := s.sink(s.endpoint, stream.NewMessage[any](
			w.values(),
			stream.WithKey[any](evt.Key)))
		if err != nil {
			fmt.Printf("failed to add payload to sliding window, key=%s, err=%s", evt.Key, err)
		}
	}
}

func (s *SlidingWindowImpl[in, out]) triggerAll() {
	if s.sink == nil {
		return
	}
	for key, w := range s.windows {
		_ = s.sink(
			s.endpoint,
			stream.NewMessage[any](w.values(), stream.WithKey[any](key)))
	}
}
