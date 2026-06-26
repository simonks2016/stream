package rolldingWindow

import "time"

type Option func(*RollingWindowConfig)

func WithJobId(jobId string) Option {
	return func(config *RollingWindowConfig) {
		config.JobId = jobId
	}
}

func WithWindowDuration(windowDuration time.Duration) Option {
	return func(config *RollingWindowConfig) {
		config.WindowDuration = windowDuration
	}
}

func WithWindowStep(step time.Duration) Option {
	return func(config *RollingWindowConfig) {
		config.WindowStep = step
	}
}
