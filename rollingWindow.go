package stream

import (
	"context"
	"time"

	r1 "github.com/simonks2016/stream/operator/rolldingWindow"
	"github.com/simonks2016/stream/stream"
)

func NewRollingWindowJob[in any, out any](
	endpoint stream.Endpoint,
	job stream.Job[[]in, out],
	opts ...RollingWindowOption,
) stream.JobOption {

	var c = RollingWindowConfig{
		WindowDuration: time.Minute,
		WindowStep:     time.Minute,
	}

	for _, o := range opts {
		o(&c)
	}

	rollingWindow := r1.NewRollingWindow[in, out](
		endpoint,
		job,
		r1.WithWindowStep(c.WindowStep),
		r1.WithWindowDuration(c.WindowDuration),
	)

	return func(p stream.Pipeline) {
		rollingWindow.Register(p)
		// 新增启动项
		p.AddStartupHook(func(ctx context.Context) error {
			// 运行
			return rollingWindow.Run(ctx)
		})
	}
}

type RollingWindowOption func(*RollingWindowConfig)

type RollingWindowConfig struct {
	WindowDuration time.Duration
	WindowStep     time.Duration
}

func WithWindowDuration(duration time.Duration) RollingWindowOption {
	return func(config *RollingWindowConfig) {
		config.WindowDuration = duration
	}
}

func WithWindowStep(step time.Duration) RollingWindowOption {
	return func(config *RollingWindowConfig) {
		config.WindowStep = step
	}
}
