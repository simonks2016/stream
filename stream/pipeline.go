package stream

type Pipeline interface {
	AddConnector(c ...Connector)
	On(topic Endpoint, handler ...Handler)
	Start() error
	Run() error
	Publish(endpoint Endpoint, msg Message[any]) error
}
