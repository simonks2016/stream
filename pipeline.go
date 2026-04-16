package main

import (
	"context"
	"eventBus/internal/runtime"
	"eventBus/stream"
)

func NewPipeline(ctx context.Context) stream.Pipeline {
	return runtime.NewRuntime(ctx, 100)
}
