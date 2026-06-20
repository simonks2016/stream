package operator

import (
	"context"

	"github.com/simonks2016/stream/stream"
)

type Operator interface {
	Register(pipeline stream.Pipeline)
	Run(ctx context.Context) error
}
