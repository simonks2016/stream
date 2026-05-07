package runtime

import "log"

type Option func(*Runtime)

func WithLogger(log *log.Logger) Option {
	return func(r *Runtime) {
		if r.inlineDispatch != nil {
			r.inlineDispatch.SetLogger(log)
		}
		r.logger = log
	}
}
