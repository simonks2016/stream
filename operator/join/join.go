package join

import (
	"context"
	"fmt"
	"github.com/simonks2016/stream/stream"
	"sync"
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
			Key:    key,
			Values: make(map[string]stream.Message[any]),
		}
		j.state[key] = st
	}

	// 4. 写入状态
	source := j.endpointID(msg.Endpoint)
	st.Values[source] = msg

	// 5. 判断是否可以触发
	if !j.shouldEmit(st, msg.WatermarkTs) {
		return nil
	}

	// 6. 组装消息列表
	msgs := make([]stream.Message[any], 0, len(st.Values))
	for _, m := range st.Values {
		msgs = append(msgs, m)
	}

	// 7. 调外部 join 算子
	out, err := j.joinFn(ctx, msgs...)
	if err != nil {
		return err
	}

	// 8. 修正输出 endpoint
	out.Endpoint = j.output

	// 9. 发给下游
	if err := sink(j.output, out); err != nil {
		return err
	}

	st.Emitted = true

	// 10. 清理
	j.cleanup(msg.WatermarkTs)

	return nil
}

func (j *JoinOperatorImpl) Register(p *stream.Pipeline) error {
	handler := j.Process

	for _, ep := range j.inputs {
		p.On(ep, handler)
	}

	return nil
}
