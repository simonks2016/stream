package stream

import (
	"context"
)

type Job[in any, out any] interface {
	Process(context.Context, Message[in], JobCollector[out]) error
}

type JobCollector[out any] interface {
	Drop()
	Collect(Endpoint, out)
	SideOutput(Endpoint, any)
	Record(any)
}
