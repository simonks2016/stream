package stream

import (
	"context"

	"github.com/simonks2016/stream/internal/runtime"
	"github.com/simonks2016/stream/stream"
)

func NewPipeline(ctx context.Context) stream.Pipeline {
	return runtime.NewRuntime(ctx, 100)
}
