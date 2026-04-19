package join

type JoinOption func(impl *JoinOperatorImpl)

func WithAllowedLateness(allowedLateness int64) JoinOption {
	return func(impl *JoinOperatorImpl) {
		impl.allowedLateness = allowedLateness
	}
}
