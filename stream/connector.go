package stream

import "context"

type Connector interface {
	Ingest(ctx context.Context, sink Sink) error
	Emit(ctx context.Context, msg Message[any]) error
	Name() string
	Stop()
	Run() error
}
