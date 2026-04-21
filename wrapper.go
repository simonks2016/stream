package stream

import (
	"fmt"
	"reflect"

	"context"

	"github.com/simonks2016/stream/stream"
)

func WrapProcessor[I any, O any](processor stream.Processor[I, O]) stream.Handler {
	return func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {

		if msg.IsEmpty() {
			return nil
		}
		payload, ok := msg.Payload.(I)
		if !ok {
			var e I
			return fmt.Errorf(
				"payload type mismatch, key=%s,input_type_name=%s,output_type_name=%s",
				msg.Key,
				reflect.TypeOf(msg.Payload).Name(),
				reflect.TypeOf(e).Name())
		}

		ingress := stream.Message[I]{
			Payload:     payload,
			Ts:          msg.Ts,
			Key:         msg.Key,
			WatermarkTs: msg.WatermarkTs,
			IngestTime:  msg.IngestTime,
			SinkTime:    msg.SinkTime,
		}

		endpoint, out, ok, err := processor.Process(ctx, ingress)
		if err != nil {
			return err
		}

		if ok && endpoint.Kind != stream.NullEndpointKind {
			return sink(endpoint, stream.Message[any]{
				SinkTime:    out.SinkTime,
				IngestTime:  out.IngestTime,
				WatermarkTs: out.WatermarkTs,
				Key:         out.Key,
				Ts:          out.Ts,
				Payload:     out.Payload,
			})
		}
		return nil
	}
}

func WrapMultiOutputProcessor[I any, O any](
	processor stream.MultiOutputProcessor[I, O],
) stream.Handler {
	return func(ctx context.Context, msg stream.Message[any], sink stream.Sink) error {
		if msg.IsEmpty() {
			return nil
		}

		payload, ok := msg.Payload.(I)
		if !ok {
			var zeroI I

			gotType := "<nil>"
			if msg.Payload != nil {
				gotType = reflect.TypeOf(msg.Payload).String()
			}

			wantType := "<unknown>"
			if t := reflect.TypeOf(zeroI); t != nil {
				wantType = t.String()
			}

			return fmt.Errorf(
				"payload type mismatch, key=%s, got=%s, want=%s",
				msg.Key,
				gotType,
				wantType,
			)
		}

		ingress := stream.Message[I]{
			Payload:     payload,
			Ts:          msg.Ts,
			Key:         msg.Key,
			WatermarkTs: msg.WatermarkTs,
			IngestTime:  msg.IngestTime,
			SinkTime:    msg.SinkTime,
		}

		outs, err := processor.Process(ctx, ingress)
		if err != nil {
			return err
		}

		for _, out := range outs {
			if out.Endpoint.Kind == stream.NullEndpointKind {
				continue
			}
			// 发送信息
			if err = sink(out.Endpoint, stream.Message[any]{
				Payload:     out.Message.Payload,
				Ts:          out.Message.Ts,
				Key:         out.Message.Key,
				WatermarkTs: out.Message.WatermarkTs,
				IngestTime:  out.Message.IngestTime,
				SinkTime:    out.Message.SinkTime,
			}); err != nil {
				return err
			}
		}
		return nil
	}
}
