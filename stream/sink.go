package stream

type Sink func(Endpoint Endpoint, msg Message[any]) error
