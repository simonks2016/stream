package connectorDispatch

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/panjf2000/ants/v2"
	"github.com/simonks2016/stream/stream"
)

type ConnectorDispatch struct {
	mu sync.RWMutex

	connectorMap map[string]stream.Connector
	pool         *ants.Pool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startOnce sync.Once
	stopOnce  sync.Once
	logger    *log.Logger
}

func NewConnectorDispatch(pool *ants.Pool) *ConnectorDispatch {
	return &ConnectorDispatch{
		connectorMap: make(map[string]stream.Connector),
		pool:         pool,
		logger:       log.Default(),
	}
}

// Register 注册 connector
func (d *ConnectorDispatch) Register(connectors ...stream.Connector) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, c := range connectors {
		if c == nil {
			return fmt.Errorf("connector is nil")
		}

		name := c.Name()
		if name == "" {
			return fmt.Errorf("connector name is empty")
		}

		if _, exists := d.connectorMap[name]; exists {
			return fmt.Errorf("connector already registered: %s", name)
		}

		d.connectorMap[name] = c
	}

	return nil
}

// Get 获取 connector
func (d *ConnectorDispatch) Get(name string) (stream.Connector, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	c, ok := d.connectorMap[name]
	return c, ok
}

// Emit 根据 message 的 endpoint 找到对应 connector 发送
func (d *ConnectorDispatch) Emit(ctx context.Context, endpoint stream.Endpoint, msg stream.Message[any]) error {
	connectorName := endpoint.Name

	d.mu.RLock()
	conn, ok := d.connectorMap[connectorName]
	d.mu.RUnlock()
	// if the connector is not exist
	if !ok {
		return fmt.Errorf("connector not found: %s", connectorName)
	}
	return conn.Emit(ctx, endpoint, msg)
}

func (d *ConnectorDispatch) Run(ctx context.Context, sink stream.Sink) error {
	var runErr error

	d.startOnce.Do(func() {
		d.mu.Lock()
		d.ctx, d.cancel = context.WithCancel(ctx)

		connectors := make([]stream.Connector, 0, len(d.connectorMap))
		for _, c := range d.connectorMap {
			connectors = append(connectors, c)
		}
		d.mu.Unlock()

		for _, c := range connectors {
			conn := c

			d.wg.Add(1)
			if err := d.submit(func() {
				defer d.wg.Done()

				// ✅ 1. 先注入 sink
				if err := conn.Ingest(d.ctx, sink); err != nil {
					if d.logger != nil {
						d.logger.Printf("connector ingest failed: %s, err=%v", conn.Name(), err)
					}
					return
				}

				// ✅ 2. 再启动运行
				if err := conn.Run(); err != nil {
					if d.logger != nil {
						d.logger.Printf("connector run failed: %s, err=%v", conn.Name(), err)
					}
				}
			}); err != nil {
				d.wg.Done()
				runErr = fmt.Errorf("submit connector start failed: %s, err=%w", conn.Name(), err)
				return
			}
		}

		// 3. 监听 ctx 关闭，统一 stop
		go func() {
			<-d.ctx.Done()
			d.Stop()
		}()
	})

	return runErr
}

// Stop 停止所有 connector，并等待后台任务退出
func (d *ConnectorDispatch) Stop() {
	d.stopOnce.Do(func() {
		d.mu.Lock()
		if d.cancel != nil {
			d.cancel()
		}

		connectors := make([]stream.Connector, 0, len(d.connectorMap))
		for _, c := range d.connectorMap {
			connectors = append(connectors, c)
		}
		d.mu.Unlock()

		for _, c := range connectors {
			c.Stop()
		}

		d.wg.Wait()
	})
}

// submit 统一封装 ants / goroutine 启动逻辑
func (d *ConnectorDispatch) submit(fn func()) error {
	if d.pool != nil {
		return d.pool.Submit(fn)
	}

	go fn()
	return nil
}
