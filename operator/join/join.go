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
type JoinWindowKey func(stream.Message[any]) (key string, windowStartMs int64, windowEndMs int64)

type JoinOperatorImpl struct {
	inputs []stream.Endpoint
	output stream.Endpoint

	state map[string]*State
	mu    sync.Mutex

	joinFn            JoinFunc
	windowKey         JoinWindowKey
	allowedLatenessMs int64
	windowDurationMS  int64
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

func NewJoiner(opts ...JoinOption) *JoinOperatorImpl {
	j := JoinOperatorImpl{
		state:             make(map[string]*State),
		allowedLatenessMs: 0,
		windowDurationMS:  1000,
	}
	for _, opt := range opts {
		opt(&j)
	}
	return &j
}
func (j *JoinOperatorImpl) process(
	ctx context.Context,
	source stream.Endpoint,
	msg stream.Message[any],
	sink stream.Sink,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.joinFn == nil {
		return fmt.Errorf("join function is nil")
	}
	if len(j.inputs) == 0 {
		return fmt.Errorf("join inputs is empty")
	}

	// 1. key 校验
	key := ""
	if j.windowKey != nil {
		keyId, startMs, endMs := j.windowKey(msg)
		key = fmt.Sprintf("%s:%d:%d", keyId, startMs, endMs)
	} else {
		windowStart := (msg.Ts / j.windowDurationMS) * j.windowDurationMS
		windowEnd := windowStart + j.windowDurationMS
		key = fmt.Sprintf("%d:%d", windowStart, windowEnd)
	}
	if key == "" {
		return fmt.Errorf("message key is empty")
	}

	// 2. 如果这条消息本身已经晚于上游 watermark，直接丢弃
	if j.isLate(msg) {
		return nil
	}

	// 3. 获取或创建 state
	st, ok := j.state[key]
	if !ok {
		st = &State{
			Key:       key,
			CreatedAt: time.Now().UnixMilli(),
			UpdatedAt: time.Now().UnixMilli(),
			Messages:  make(map[string]stream.Message[any]),
		}
		j.state[key] = st
	}

	// 4. 存该 source 的消息
	srcID := endpointID(source)
	st.Messages[srcID] = msg
	st.UpdatedAt = time.Now().UnixMilli()

	// 5. 如果没收齐，先返回
	if !j.ready(st) {

		// 顺手清一下已经过期但拼不齐的 state
		j.cleanupLocked(msg.WatermarkTs)
		return nil
	}

	// 6. 按 inputs 顺序取消息，保证 joinFn 参数稳定
	msgs := make([]stream.Message[any], 0, len(j.inputs))
	for _, ep := range j.inputs {
		m, ok := st.Messages[endpointID(ep)]
		if !ok {
			fmt.Println("No Message")
			return nil
		}
		msgs = append(msgs, m)
	}

	// 7. 执行 join
	out, err := j.joinFn(ctx, msgs...)
	if err != nil {
		fmt.Println("Join Error:", err)
		return err
	}

	out.SinkTime = time.Now().UnixMilli()

	// 输出消息的 WatermarkTs 可沿用本轮 join 输入中的最小 watermark
	out.WatermarkTs = minWatermark(msgs)

	// 9. 发到下游
	if err := sink(j.output, out); err != nil {
		fmt.Println("Sink Error:", err)
		return err
	}

	// 10. inner join，一次完成就删
	delete(j.state, key)

	// 11. 再顺手清理旧状态
	j.cleanupLocked(out.WatermarkTs)

	return nil
}

func (j *JoinOperatorImpl) Register(p stream.Pipeline) error {
	if p == nil {
		return fmt.Errorf("pipeline is nil")
	}
	if len(j.inputs) == 0 {
		return fmt.Errorf("join inputs is empty")
	}
	if j.joinFn == nil {
		return fmt.Errorf("join function is nil")
	}

	for _, ep := range j.inputs {
		source := ep

		handler := func(
			ctx context.Context,
			msg stream.Message[any],
			sink stream.Sink,
		) error {
			return j.process(ctx, source, msg, sink)
		}

		p.On(ep, handler)
	}
	return nil
}
func (j *JoinOperatorImpl) WithWindowKey(fn JoinWindowKey) *JoinOperatorImpl {
	j.windowKey = fn
	return j
}
