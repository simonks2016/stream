package join

import "github.com/simonks2016/stream/stream"

type State struct {
	Key       string
	Message   []stream.Message[any]
	CreatedAt int64
	Emitted   bool
}
