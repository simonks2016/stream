package websocket

type Option func(*WebSocketConnector)

func WithOnConnected(fn OnConnectedFunc) Option {
	return func(c *WebSocketConnector) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.onConnected = fn
	}
}

func WithOnDispatch(fn WSDispatchFunc) Option {
	return func(c *WebSocketConnector) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.dispatchFn = fn
	}
}
