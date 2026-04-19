package stream

import (
	"context"

	"github.com/simonks2016/stream/internal/runtime"
	"github.com/simonks2016/stream/stream"
)

func NewPipeline(ctx context.Context, opts ...PipelineOption) stream.Pipeline {

	allowed_lateness := int64(100)
	if len(opts) > 0 {

		for _, opt := range opts {
			if opt.Name() == "allowed_lateness" {
				al, ok := opt.Value().(int64)
				if ok {
					allowed_lateness = al
				}
			}
		}
	}
	return runtime.NewRuntime(ctx, allowed_lateness)
}

type PipelineOption interface {
	Name() string
	Value() interface{}
}

type customPipelineOption struct {
	name  string
	value interface{}
}

func (o customPipelineOption) Name() string {
	return o.name
}
func (o customPipelineOption) Value() interface{} {
	return o.value
}

func NewPipelineOption(name string, value interface{}) PipelineOption {
	return customPipelineOption{name, value}
}

func WithAllowedLateness(ms int64) PipelineOption {
	return NewPipelineOption("allowed_lateness", ms)
}
