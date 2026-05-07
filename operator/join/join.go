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

// FilterKey 返回 true 表示保留，返回 false 表示丢弃
type FilterKey func(stream.Message[any]) bool

type JoinOperatorImpl struct {
	inputs []stream.Endpoint
	output stream.Endpoint

	state map[string]*State
	mu    sync.Mutex

	joinFn            JoinFunc
	windowKey         JoinWindowKey
	filterKey         FilterKey
	allowedLatenessMs int64
	windowDurationMS  int64
}

func (j *JoinOperatorImpl) WithJoin(fn JoinFunc) *JoinOperatorImpl {
	j.joinFn = fn
	return j
}

func (j *JoinOperatorImpl) WithFilter(fn FilterKey) *JoinOperatorImpl {
	j.filterKey = fn
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

func (j *JoinOperatorImpl) WithWindowKey(fn JoinWindowKey) *JoinOperatorImpl {
	j.windowKey = fn
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

	if j.filterKey != nil {
		if !j.filterKey(msg) {
			fmt.Printf(
				"[JoinOperator] filter drop message: source=%s, key=%s, ts=%d, wm=%d, payload_type=%T\n",
				endpointID(source),
				msg.Key,
				msg.Ts,
				msg.WatermarkTs,
				msg.Payload,
			)
			return nil
		}
	}

	key := ""
	if j.windowKey != nil {
		keyID, startMs, endMs := j.windowKey(msg)
		key = fmt.Sprintf("%s:%d:%d", keyID, startMs, endMs)
	} else {
		windowStart := (msg.Ts / j.windowDurationMS) * j.windowDurationMS
		windowEnd := windowStart + j.windowDurationMS

		key = fmt.Sprintf("%s:%d:%d", msg.Key, windowStart, windowEnd)
	}

	if key == "" {
		return fmt.Errorf("message key is empty")
	}

	if j.isLate(msg) {
		fmt.Printf(
			"[JoinOperator] late drop message: source=%s, join_key=%s, msg_key=%s, ts=%d, wm=%d, allowed_lateness_ms=%d, payload_type=%T\n",
			endpointID(source),
			key,
			msg.Key,
			msg.Ts,
			msg.WatermarkTs,
			j.allowedLatenessMs,
			msg.Payload,
		)
		return nil
	}

	// 4. 获取或创建 state
	st, ok := j.state[key]
	if !ok {
		now := time.Now().UnixMilli()
		st = &State{
			Key:       key,
			CreatedAt: now,
			UpdatedAt: now,
			Messages:  make(map[string]stream.Message[any]),
		}
		j.state[key] = st
	}

	// 5. 存该 source 的消息
	srcID := endpointID(source)
	st.Messages[srcID] = msg
	st.UpdatedAt = time.Now().UnixMilli()

	// 6. 如果没收齐，先返回
	if !j.ready(st) {
		j.cleanupLocked(msg.WatermarkTs)
		return nil
	}

	// 7. 按 inputs 顺序取消息，保证 joinFn 参数稳定
	msgs := make([]stream.Message[any], 0, len(j.inputs))
	for _, ep := range j.inputs {
		m, ok := st.Messages[endpointID(ep)]
		if !ok {
			fmt.Printf(
				"[JoinOperator] missing message: join_key=%s, missing_source=%s\n",
				key,
				endpointID(ep),
			)
			return nil
		}
		msgs = append(msgs, m)
	}

	// 8. 执行 join
	out, err := j.joinFn(ctx, msgs...)
	if err != nil {
		fmt.Println("[JoinOperator] join error:", err)
		return err
	}

	out.SinkTime = time.Now().UnixMilli()
	out.WatermarkTs = minWatermark(msgs)

	// 9. 发到下游
	if err := sink(j.output, out); err != nil {
		fmt.Println("[JoinOperator] sink error:", err)
		return err
	}

	// 10. inner join，一次完成就删
	delete(j.state, key)

	// 11. 清理旧状态
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
