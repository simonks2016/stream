package stream

import (
	"context"
)

type Job[in any, out any] interface {
	Process(context.Context, Message[in], JobCollector[out]) error
}

type JobCollector[out any] interface {
	Drop(...string)
	Collect(Endpoint, out)
	SideOutput(Endpoint, any)
	Record(any)
	HasErrors() []error
}

// Junction --wasm job.wasm --master_url https://stream.henspark.io/master:8080 --s3_option {}
