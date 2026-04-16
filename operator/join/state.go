package join

import "eventBus/stream"

type State struct {
	Key       string
	Message   []stream.Message[any]
	CreatedAt int64
	Emitted   bool
}
