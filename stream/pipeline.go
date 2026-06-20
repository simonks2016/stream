package stream

import "context"

type Pipeline interface {
	AddConnector(c ...Connector)
	Job(opts ...JobOption)
	On(topic Endpoint, handler ...Handler)
	Start() error
	Run() error
	Publish(endpoint Endpoint, msg Message[any]) error
	AddStartupHook(...func(ctx context.Context) error)
}

type JobOption func(p Pipeline)
