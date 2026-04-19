package runtime

import (
	"context"
	"fmt"

	"github.com/simonks2016/stream/internal/connectorDispatch"
	"github.com/simonks2016/stream/internal/inlineDispatch"
	"github.com/simonks2016/stream/internal/watermark"
	"github.com/simonks2016/stream/stream"

	"github.com/panjf2000/ants/v2"
)

type Runtime struct {
	ctx               context.Context
	wm                *watermark.Watermark
	inlineDispatch    *inlineDispatch.InlineDispatch
	connectorDispatch *connectorDispatch.ConnectorDispatch
}

func (r *Runtime) AddConnector(connectors ...stream.Connector) {
	if r.connectorDispatch == nil {
		panic("connectorDispatch is nil")
	}
	if err := r.connectorDispatch.Register(connectors...); err != nil {
		panic(err)
	}
}

func (r *Runtime) On(topic stream.Endpoint, handler ...stream.Handler) {
	if r.inlineDispatch == nil {
		panic("inlineDispatch is nil")
	}
	if topic.Kind != stream.InlineKind {
		panic(fmt.Sprintf("runtime.On only supports inline endpoint, got kind=%v name=%s", topic.Kind, topic.Name))
	}
	r.inlineDispatch.On(topic.Name, handler...)
}

// Publish 允许外部主动往 runtime 投递消息
func (r *Runtime) Publish(endpoint stream.Endpoint, msg stream.Message[any]) error {
	return r.sink(endpoint, msg)
}

func (r *Runtime) Start() error {
	if r.ctx == nil {
		return fmt.Errorf("runtime context is nil")
	}
	if r.inlineDispatch == nil {
		return fmt.Errorf("inlineDispatch is nil")
	}
	if r.connectorDispatch == nil {
		return fmt.Errorf("connectorDispatch is nil")
	}
	if r.wm == nil {
		return fmt.Errorf("watermark is nil")
	}

	// 1. 先启动 connectors（高优先级）
	if err := r.connectorDispatch.Run(r.ctx, r.sink); err != nil {
		return err
	}

	// 2. 启动内线调度器
	go func() {
		_ = r.inlineDispatch.Run(r.ctx, r.sink)
	}()

	return nil
}

func (r *Runtime) Run() error {
	if err := r.Start(); err != nil {
		return err
	}

	<-r.ctx.Done()

	// 如果 connectorDispatch.Run 内部已经自己监听 ctx 并 Stop，
	// 这里可以不再重复 Stop。
	if r.connectorDispatch != nil {
		r.connectorDispatch.Stop()
	}

	return r.ctx.Err()
}

// sink 是 runtime 的统一出口：
// - 统一更新时间推进
// - 决定往 inline 还是 connector 发
func (r *Runtime) sink(endpoint stream.Endpoint, msg stream.Message[any]) error {

	// 统一推进 watermark
	if r.wm != nil {
		msg.WatermarkTs = r.wm.Update(endpoint.EndpointSourceId, msg.Ts)
	}

	switch endpoint.Kind {
	case stream.InlineKind:
		if r.inlineDispatch == nil {
			return fmt.Errorf("inlineDispatch is nil")
		}
		return r.inlineDispatch.Publish(endpoint, msg)

	case stream.ConnectorsKind:
		if r.connectorDispatch == nil {
			return fmt.Errorf("connectorDispatch is nil")
		}
		return r.connectorDispatch.Emit(r.ctx, endpoint, msg)

	default:
		return fmt.Errorf("unknown endpoint kind: %v", endpoint.Kind)
	}
}

func NewRuntime(ctx context.Context, allowedLateness int64) *Runtime {
	pool, err := ants.NewPool(
		100)
	if err != nil {
		panic(err)
	}

	return &Runtime{
		ctx:               ctx,
		wm:                watermark.NewWatermark(allowedLateness),
		inlineDispatch:    inlineDispatch.NewDispatch(1024, pool),
		connectorDispatch: connectorDispatch.NewConnectorDispatch(pool),
	}
}
