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
			return fmt.Errorf("payload type mismatch, key=%s,type_name=%s", msg.Key, reflect.TypeOf(msg.Payload).Name())
		}

		ingress := stream.NewMessage[I](payload)

		endpoint, out, ok, err := processor.Process(ctx, ingress)
		if err != nil {
			return err
		}

		if ok && endpoint.Kind != stream.NullEndpointKind {
			return sink(endpoint, stream.NewMessage[any](out))
		}
		return nil
	}
}
