package jobCollector

import (
	"encoding/json"
	"log"
	"strings"
	"time"

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
	if j.logger != nil {
		j.logger.Printf(
			"[COLLECT] key=%s input=%T ingest_ms=%d -> endpoint=%s output=%T emit_ms=%d",
			j.inputMessage.Key,
			j.inputMessage.Payload,
			j.inputMessage.IngestTime,
			endpoint.FormattedName(),
			out,
			time.Now().UnixMilli(),
		)
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
	if j.logger != nil {
		j.logger.Printf(
			"[SIDE OUTPUT] key=%s input=%T ingest_ms=%d -> endpoint=%s output=%T emit_ms=%d",
			j.inputMessage.Key,
			j.inputMessage.Payload,
			j.inputMessage.IngestTime,
			endpoint.FormattedName(),
			out,
			time.Now().UnixMilli(),
		)
	}
}

func (j *JobCollector[o]) Record(a any) {
	//TODO implement me
	marshal, _ := json.Marshal(a)
	// 记录日志
	j.logger.Printf("Record: %s", string(marshal))
}

func (j *JobCollector[o]) Drop(details ...string) {

	details = append(details, "key="+j.inputMessage.Key)

	if j.logger != nil {
		j.logger.Printf(
			"[DROP] %s",
			strings.Join(details, ", "),
		)
	}
}

func (j *JobCollector[o]) HasErrors() []error { return j.errs }

func NewJobCollector[out any](inputMessage stream.Message[any], sink stream.Sink) *JobCollector[out] {
	return &JobCollector[out]{
		sink:         sink,
		logger:       log.Default(),
		inputMessage: inputMessage,
	}
}
