package stream

import (
	"context"
	"fmt"
)

type Processor[in any, out any] interface {
	Process(ctx context.Context, in Message[in]) (Message[out], bool, error)
}

type Handler func(ctx context.Context, msg Message[any], sink Sink) error

func WrapProcessor[I any, O any](processor Processor[I, O]) Handler {
	return func(ctx context.Context, msg Message[any], sink Sink) error {
		payload, ok := msg.Payload.(I)
		if !ok {
			return fmt.Errorf("payload type mismatch, topic=%s", msg.Endpoint.Name)
		}

		in := Message[I]{
			Ts:       msg.Ts,
			Endpoint: msg.Endpoint,
			Payload:  payload,
		}

		out, ok, err := processor.Process(ctx, in)
		if err != nil {
			return err
		}

		if ok {
			return sink(out.Endpoint, Message[any]{
				Ts:       out.Ts,
				Endpoint: out.Endpoint,
				Payload:  out.Payload,
			})
		}
		return nil
	}
}
