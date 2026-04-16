package inlineDispatch

import (
	"context"
	"fmt"
	"sync"

	"github.com/panjf2000/ants/v2"

	"eventBus/stream"
)

type task struct {
	ctx     context.Context
	handler stream.Handler
	msg     stream.Message[any]
	sink    stream.Sink
}

type InlineDispatch struct {
	mu    sync.RWMutex
	Route map[string][]stream.Handler

	msgCh chan stream.Message[any]
	pool  *ants.Pool
}

func NewDispatch(bufferSize int, pool *ants.Pool) *InlineDispatch {
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	return &InlineDispatch{
		Route: make(map[string][]stream.Handler),
		msgCh: make(chan stream.Message[any], bufferSize),
		pool:  pool,
	}
}

// On 注册某个 inline topic 对应的 handler
func (d *InlineDispatch) On(topic string, handlers ...stream.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.Route[topic] = append(d.Route[topic], handlers...)
}

// Publish 往内线投递消息
func (d *InlineDispatch) Publish(msg stream.Message[any]) error {
	select {
	case d.msgCh <- msg:
		return nil
	default:
		return fmt.Errorf("inline dispatch queue is full")
	}
}

// Run 开始监听消息并分发
func (d *InlineDispatch) Run(ctx context.Context, sink stream.Sink) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-d.msgCh:
			topic := d.topicOf(msg.Endpoint)

			handlers := d.handlersOf(topic)
			if len(handlers) == 0 {
				continue
			}

			for _, h := range handlers {
				handler := h
				taskMsg := msg // 拷贝一份，避免闭包误用同一个变量

				err := d.pool.Submit(func() {
					// 单个 handler 错误不影响整个 dispatch
					if err := handler(ctx, taskMsg, sink); err != nil {
						fmt.Println(err)
					}
				})
				if err != nil {
					// pool 满了或已关闭时，按你的风格先忽略
					// 后面你可以加日志 / metrics / fallback
					continue
				}

			}
		}
	}
}

// handlersOf 安全读取 handlers
func (d *InlineDispatch) handlersOf(topic string) []stream.Handler {
	d.mu.RLock()
	defer d.mu.RUnlock()

	handlers := d.Route[topic]
	if len(handlers) == 0 {
		return nil
	}

	out := make([]stream.Handler, len(handlers))
	copy(out, handlers)
	return out
}

// topicOf 从 endpoint 中解析 inline topic
func (d *InlineDispatch) topicOf(ep stream.Endpoint) string {
	return ep.Name
}
