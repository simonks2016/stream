package stream

import (
	"fmt"

	"github.com/simonks2016/stream/stream"
)

func Bind[T any](fromTopic, toTopic stream.Endpoint, coder stream.Coder[T]) stream.ConnectorBinding {
	return &Binding[T]{
		from:  fromTopic,
		to:    toTopic,
		coder: coder,
	}
}

func (k *Binding[T]) From() stream.Endpoint { return k.from }
func (k *Binding[T]) To() stream.Endpoint   { return k.to }

func (k *Binding[T]) Decode(data []byte) (stream.Message[any], error) {
	msg, err := k.coder.Unmarshal(data)
	if err != nil {
		return stream.Message[any]{}, fmt.Errorf("kafka decode failed: %w", err)
	}

	return stream.Message[any]{
		Endpoint: k.to,
		Key:      msg.Key,
		Payload:  msg.Payload,
	}, nil
}

func (k *Binding[T]) Encode(message stream.Message[any]) ([]byte, bool, error) {
	payload, ok := message.Payload.(T)
	if !ok {
		return nil, false, nil
	}

	raw, err := k.coder.Marshal(stream.Message[T]{
		Endpoint: k.from,
		Key:      message.Key,
		Payload:  payload,
	})
	if err != nil {
		return nil, true, fmt.Errorf("kafka encode failed: %w", err)
	}

	return raw, true, nil
}

type Binding[T any] struct {
	from  stream.Endpoint
	to    stream.Endpoint
	coder stream.Coder[T]
}
