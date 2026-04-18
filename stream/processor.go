package stream

import (
	"context"
	"fmt"
)

type Processor[in any, out any] interface {
	Process(ctx context.Context, in Message[in]) (Endpoint, Message[out], bool, error)
}

type Handler func(ctx context.Context, msg Message[any], sink Sink) error

func WrapProcessor[I any, O any](processor Processor[I, O]) Handler {
	return func(ctx context.Context, msg Message[any], sink Sink) error {
		payload, ok := msg.Payload.(I)
		if !ok {
			return fmt.Errorf("payload type mismatch, key=%s", msg.Key)
		}

		in := Message[I]{
			Ts:      msg.Ts,
			Payload: payload,
		}

		endpoint, out, ok, err := processor.Process(ctx, in)
		if err != nil {
			return err
		}

		if ok && endpoint.Kind != NullEndpointKind {
			return sink(endpoint, Message[any]{
				Ts:      out.Ts,
				Payload: out.Payload,
			})
		}
		return nil
	}
}
