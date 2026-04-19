package join

type JoinOption func(impl *JoinOperatorImpl)

func WithAllowedLateness(allowedLatenessMS int64) JoinOption {
	return func(impl *JoinOperatorImpl) {
		impl.allowedLatenessMs = allowedLatenessMS
	}
}
