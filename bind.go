package stream

import (
	"fmt"

	"github.com/simonks2016/stream/stream"
)

type BindingOption[T any] func(binding *Binding[T])

func Bind[T any](fromTopic, toTopic stream.Endpoint, coder stream.Coder[T], opts ...BindingOption[T]) stream.ConnectorBinding {
	b := &Binding[T]{
		from:  fromTopic,
		to:    toTopic,
		coder: coder,
		mode:  stream.ReadWrite,
	}

	if len(opts) > 0 {
		for _, opt := range opts {
			opt(b)
		}
	}
	return b
}

func (k *Binding[T]) From() stream.Endpoint { return k.from }
func (k *Binding[T]) To() stream.Endpoint   { return k.to }

func (k *Binding[T]) Decode(data []byte) (stream.Message[any], error) {
	msg, err := k.coder.Unmarshal(data)
	if err != nil {
		return stream.Message[any]{}, fmt.Errorf("kafka decode failed: %w", err)
	}

	return stream.Message[any]{
		Key:         msg.Key,
		Payload:     msg.Payload,
		Ts:          msg.Ts,
		IngestTime:  msg.IngestTime,
		SinkTime:    msg.SinkTime,
		WatermarkTs: msg.WatermarkTs,
	}, nil
}

func (k *Binding[T]) Encode(message stream.Message[any]) ([]byte, bool, error) {
	payload, ok := message.Payload.(T)
	if !ok {
		return nil, false, nil
	}

	raw, err := k.coder.Marshal(stream.Message[T]{
		Key:         message.Key,
		Payload:     payload,
		IngestTime:  message.IngestTime,
		SinkTime:    message.SinkTime,
		WatermarkTs: message.WatermarkTs,
		Ts:          message.Ts,
	})
	if err != nil {
		return nil, true, fmt.Errorf("kafka encode failed: %w", err)
	}

	return raw, true, nil
}

func (k *Binding[T]) Mode() stream.BindingMode {
	return k.mode
}

type Binding[T any] struct {
	from  stream.Endpoint
	to    stream.Endpoint
	coder stream.Coder[T]
	mode  stream.BindingMode
}

func WithBindingMode[T any](m stream.BindingMode) BindingOption[T] {
	return func(binding *Binding[T]) {
		binding.mode = m
	}
}
