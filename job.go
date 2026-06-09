package stream

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/simonks2016/stream/internal/jobCollector"
	"github.com/simonks2016/stream/stream"
)

func NewJob[in any, out any](endpoint stream.Endpoint, job stream.Job[in, out]) stream.JobOption {

	handler := func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {

		if msg.IsEmpty() {
			return nil
		}

		// 转化成in类型
		payload, ok := msg.Payload.(in)
		if !ok {
			var e in
			return fmt.Errorf(
				"payload type mismatch, key=%s,input_type_name=%s,output_type_name=%s",
				msg.Key,
				reflect.TypeOf(msg.Payload).Name(),
				reflect.TypeOf(e).Name())
		}

		ingress := stream.Message[in]{
			Payload:     payload,
			Ts:          msg.Ts,
			Key:         msg.Key,
			WatermarkTs: msg.WatermarkTs,
			IngestTime:  msg.IngestTime,
			SinkTime:    msg.SinkTime,
		}
		// 初始化收集器
		collector := jobCollector.NewJobCollector[out](msg, sink)
		// 处理
		if err := job.Process(ctx, ingress, collector); err != nil {
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
	}
}
