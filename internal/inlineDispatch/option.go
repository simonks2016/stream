package inlineDispatch

import "log"

type Option func(dispatch *InlineDispatch)

func WithLogger(log *log.Logger) Option {
	return func(dispatch *InlineDispatch) {
		dispatch.logger = log
	}
}
