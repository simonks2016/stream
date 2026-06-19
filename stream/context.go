package stream

import "context"

type JobContext interface {
	context.Context
	State(name string) State
}

type State interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Remove(key string)

	GetList(key string) ([]any, bool)
	SetList(key string, value []any)
	Append(key string, value any)
}
