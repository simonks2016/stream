package stream

import "context"

type ConnectorBindingMode int

type BindingMode int

const (
	ReadOnly BindingMode = iota + 1
	WriteOnly
	ReadWrite
)

type Connector interface {
	Ingest(ctx context.Context, sink Sink) error
	Emit(ctx context.Context, to Endpoint, msg Message[any]) error
	Name() string
	Stop()
	Run() error
	On(...ConnectorBinding) Connector
}

type Coder[T any] interface {
	Unmarshal(data []byte) (Message[T], error)
	Marshal(message Message[T]) ([]byte, error)
}

type ConnectorBinding interface {
	From() Endpoint
	To() Endpoint
	Decode(data []byte) (Message[any], error)
	Encode(msg Message[any]) ([]byte, bool, error)
	Mode() BindingMode
}
