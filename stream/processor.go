package stream

import (
	"context"
)

type Processor[in any, out any] interface {
	Process(ctx context.Context, in Message[in]) (Endpoint, Message[out], bool, error)
}

type Handler func(ctx context.Context, msg Message[any], sink Sink) error
