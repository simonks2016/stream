package join

import "github.com/simonks2016/stream/stream"

type State struct {
	Key       string
	CreatedAt int64
	UpdatedAt int64
	Messages  map[string]stream.Message[any]
}
