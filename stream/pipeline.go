package stream

type Pipeline interface {
	AddConnector(c ...Connector)
	Job(opts ...JobOption)
	On(topic Endpoint, handler ...Handler)
	Start() error
	Run() error
	Publish(endpoint Endpoint, msg Message[any]) error
}

type JobOption func(p Pipeline)
