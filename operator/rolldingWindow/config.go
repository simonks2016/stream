package rolldingWindow

import "time"

type RollingWindowConfig struct {
	WindowDuration time.Duration
	WindowStep     time.Duration
	JobId          string
}
