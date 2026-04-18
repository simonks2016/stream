package join

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/simonks2016/stream/stream"
)

type JoinOperator interface {
	Process(ctx context.Context, msg ...stream.Message[any]) (stream.Message[any], error)
}
type JoinFunc func(ctx context.Context, msgs ...stream.Message[any]) (stream.Message[any], error)

type JoinOperatorImpl struct {
	inputs []stream.Endpoint
	output stream.Endpoint

	state map[string]*State
	mu    sync.Mutex

	joinFn JoinFunc

	allowedLateness int64
}

func (j *JoinOperatorImpl) WithJoin(fn JoinFunc) *JoinOperatorImpl {
	j.joinFn = fn
	return j
}

func (j *JoinOperatorImpl) From(endpoints ...stream.Endpoint) *JoinOperatorImpl {
	j.inputs = endpoints
	return j
}

func (j *JoinOperatorImpl) To(endpoint stream.Endpoint) *JoinOperatorImpl {
	j.output = endpoint
	return j
}

func NewJoiner() *JoinOperatorImpl {
	return &JoinOperatorImpl{
		state:           make(map[string]*State),
		allowedLateness: 0,
	}
}

func (j *JoinOperatorImpl) Process(
	ctx context.Context,
	msg stream.Message[any],
	sink stream.Sink,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.joinFn == nil {
		return fmt.Errorf("join function is nil")
	}

	// 1. 迟到判断
	if j.isLate(msg) {
		return nil
	}

	// 2. 取 key
	key := msg.Key
	if key == "" {
		return fmt.Errorf("message key is empty")
	}

	// 3. 取状态
	st, ok := j.state[key]
	if !ok {
		st = &State{
			Key:       key,
			Message:   []stream.Message[any]{},
			CreatedAt: time.Now().UnixMilli(),
		}
		j.state[key] = st
	}

	return nil
}

func (j *JoinOperatorImpl) Register(p stream.Pipeline) error {
	handler := j.Process

	for _, ep := range j.inputs {
		p.On(ep, handler)
	}

	return nil
}
