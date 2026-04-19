package join

type JoinOption func(impl *JoinOperatorImpl)

func WithAllowedLateness(allowedLatenessMS int64) JoinOption {
	return func(impl *JoinOperatorImpl) {
		impl.allowedLatenessMs = allowedLatenessMS
	}
}

func WithWindowDuration(windowDurationMS int64) JoinOption {
	return func(impl *JoinOperatorImpl) {
		impl.windowDurationMS = windowDurationMS
	}
}
