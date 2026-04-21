package stream

import (
	"context"
)

type MultiOutputProcessor[I any, O any] interface {
	Process(ctx context.Context, msg Message[I]) ([]Output[O], error)
}

type Output[T any] struct {
	Endpoint Endpoint
	Message  Message[T]
}

type Processor[in any, out any] interface {
	Process(ctx context.Context, in Message[in]) (Endpoint, Message[out], bool, error)
}

type Handler func(ctx context.Context, msg Message[any], sink Sink) error
