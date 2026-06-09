package jobCollector

import (
	"encoding/json"
	"log"

	"github.com/simonks2016/stream/stream"
)

type JobCollector[o any] struct {
	inputMessage stream.Message[any]
	sink         stream.Sink
	errs         []error
	logger       *log.Logger
}

func (j *JobCollector[o]) Collect(endpoint stream.Endpoint, out o) {
	//TODO implement me
	if err := j.sink(
		endpoint,
		stream.NewMessage[any](out),
	); err != nil {
		j.errs = append(j.errs, err)
		return
	}
}

func (j *JobCollector[o]) SideOutput(endpoint stream.Endpoint, out any) {
	//TODO implement me
	if err := j.sink(
		endpoint,
		stream.NewMessage[any](out),
	); err != nil {
		j.errs = append(j.errs, err)
		return
	}
}

func (j *JobCollector[o]) Record(a any) {
	//TODO implement me
	marshal, _ := json.Marshal(a)
	// 记录日志
	j.logger.Printf("Record: %s", string(marshal))
}

func (j *JobCollector[o]) Drop() {
	j.logger.Printf("dropping message[%s]...", j.inputMessage.Key)
	return
}

func (j *JobCollector[o]) HasErrors() []error { return j.errs }

func NewJobCollector[out any](inputMessage stream.Message[any], sink stream.Sink) *JobCollector[out] {
	return &JobCollector[out]{
		sink:         sink,
		logger:       log.Default(),
		inputMessage: inputMessage,
	}
}
