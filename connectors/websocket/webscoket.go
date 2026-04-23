package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/simonks2016/stream/stream"
)

const (
	DefaultWebSocketConnectorName = "websocket"
	MetaWSSubscribe               = "ws_subscribe"
)

type OnConnectedFunc func(ctx context.Context, conn *websocket.Conn) error
type WSDispatchFunc func(msg stream.Message[map[string]any]) *string

type WebSocketConnector struct {
	ctx    context.Context
	cancel context.CancelFunc

	sink stream.Sink

	mu       sync.RWMutex
	bindings []stream.ConnectorBinding
	rules    map[string]WSSubscriptionBinding

	conn   *websocket.Conn
	dialer *websocket.Dialer

	wg      sync.WaitGroup
	started bool
	logger  *log.Logger

	URL string

	Header            http.Header
	HandshakeTimeout  time.Duration
	ReconnectInterval time.Duration
	ReadBufferSize    int
	WriteBufferSize   int
	MaxMessageSize    int64
	ReadDeadline      time.Duration
	WriteDeadline     time.Duration
	PingInterval      time.Duration
	PongWait          time.Duration

	onConnected OnConnectedFunc
	dispatchFn  WSDispatchFunc

	connected atomic.Bool
}

func NewWebSocketConnector(parent context.Context, url string) *WebSocketConnector {
	ctx, cancel := context.WithCancel(parent)

	return &WebSocketConnector{
		ctx:               ctx,
		cancel:            cancel,
		URL:               url,
		logger:            log.Default(),
		bindings:          make([]stream.ConnectorBinding, 0, 8),
		rules:             make(map[string]WSSubscriptionBinding),
		Header:            make(http.Header),
		HandshakeTimeout:  5 * time.Second,
		ReconnectInterval: 3 * time.Second,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		MaxMessageSize:    8 * 1024 * 1024,
		ReadDeadline:      60 * time.Second,
		WriteDeadline:     10 * time.Second,
		PingInterval:      20 * time.Second,
		PongWait:          60 * time.Second,
	}
}

func (w *WebSocketConnector) Name() string {
	return DefaultWebSocketConnectorName
}

func (w *WebSocketConnector) On(op ...stream.ConnectorBinding) stream.Connector {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, item := range op {
		w.bindings = append(w.bindings, item)

		if wsb, ok := item.(WSSubscriptionBinding); ok {
			key := strings.TrimSpace(wsb.Key())
			if key == "" {
				panic("websocket subscribe binding key is empty")
			}
			if _, exists := w.rules[key]; exists {
				panic("duplicated websocket subscribe binding key: " + key)
			}
			w.rules[key] = wsb
		}
	}

	return w
}

func (w *WebSocketConnector) OnConnected(fn OnConnectedFunc) *WebSocketConnector {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onConnected = fn
	return w
}

func (w *WebSocketConnector) OnDispatch(fn WSDispatchFunc) *WebSocketConnector {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dispatchFn = fn
	return w
}

func (w *WebSocketConnector) Ingest(ctx context.Context, sink stream.Sink) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 已启动就不允许重复调用
	if w.started {
		return fmt.Errorf("websocket connector already started")
	}

	if ctx != nil {
		w.ctx, w.cancel = context.WithCancel(ctx)
	}

	w.sink = sink
	return w.runLocked()
}

func (w *WebSocketConnector) Run() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.runLocked()
}

func (w *WebSocketConnector) runLocked() error {
	if w.started {
		return nil
	}
	if strings.TrimSpace(w.URL) == "" {
		return fmt.Errorf("websocket url is empty")
	}
	if w.ctx == nil {
		w.ctx = context.Background()
	}

	w.dialer = &websocket.Dialer{
		HandshakeTimeout: w.HandshakeTimeout,
		ReadBufferSize:   w.ReadBufferSize,
		WriteBufferSize:  w.WriteBufferSize,
	}

	w.wg.Go(func() {
		w.connectionLoop()
	})

	w.started = true
	return nil
}

func (w *WebSocketConnector) Stop() {
	w.mu.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	conn := w.conn
	w.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	w.wg.Wait()
}

func (w *WebSocketConnector) IsConnected() bool {
	return w.connected.Load()
}

func (w *WebSocketConnector) Emit(ctx context.Context, route stream.Endpoint, msg stream.Message[any]) error {
	w.mu.RLock()
	conn := w.conn
	bindings := append([]stream.ConnectorBinding(nil), w.bindings...)
	w.mu.RUnlock()

	if conn == nil || !w.connected.Load() {
		return fmt.Errorf("websocket is not connected")
	}

	var errs []error
	matched := false

	for _, binding := range bindings {
		if binding.Mode() == stream.ReadOnly {
			continue
		}

		from := binding.From()
		//to := binding.To()

		if !IsWebSocketToInlineMatch(from, route) {
			continue
		}

		payload, ok, err := binding.Encode(msg)
		if err != nil {
			errs = append(errs, fmt.Errorf("encode websocket message failed: %w", err))
			continue
		}
		if !ok {
			continue
		}

		matched = true

		if err := writeWSMessage(conn, payload, w.WriteDeadline); err != nil {
			errs = append(errs, fmt.Errorf("write websocket message failed: %w", err))
		}
	}

	if !matched {
		return fmt.Errorf("message has no routing destination (msg=%s,ts=%d,from=websocket)", msg.Key, msg.Ts)
	}
	if len(errs) > 0 {
		return joinErrors(errs...)
	}
	return nil
}

func (w *WebSocketConnector) connectionLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		if err := w.connectAndServe(); err != nil {
			w.connected.Store(false)
			if w.logger != nil {
				w.logger.Printf("[WebSocketConnector] connection cycle failed: %v", err)
			}
		}

		select {
		case <-w.ctx.Done():
			return
		case <-time.After(w.ReconnectInterval):
		}
	}
}

func (w *WebSocketConnector) connectAndServe() error {
	conn, _, err := w.dialer.DialContext(w.ctx, w.URL, w.Header)
	if err != nil {
		return fmt.Errorf("dial websocket failed: %w", err)
	}

	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()

	w.connected.Store(true)

	if w.logger != nil {
		w.logger.Printf("[WebSocketConnector] connected url=%s", w.URL)
	}

	defer func() {
		w.connected.Store(false)

		w.mu.Lock()
		if w.conn == conn {
			w.conn = nil
		}
		w.mu.Unlock()

		_ = conn.Close()
	}()

	conn.SetReadLimit(w.MaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(w.PongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(w.PongWait))
	})

	w.mu.RLock()
	onConnected := w.onConnected
	w.mu.RUnlock()

	if onConnected != nil {
		if err := onConnected(w.ctx, conn); err != nil {
			return fmt.Errorf("on connected failed: %w", err)
		}
	}

	if err := w.sendSubscriptions(conn); err != nil {
		return fmt.Errorf("send subscriptions failed: %w", err)
	}

	pingDone := make(chan struct{})
	go w.pingLoop(conn, pingDone)

	err = w.readLoop(conn)

	close(pingDone)
	return err
}

func (w *WebSocketConnector) sendSubscriptions(conn *websocket.Conn) error {
	w.mu.RLock()
	rules := make([]WSSubscriptionBinding, 0, len(w.rules))
	for _, v := range w.rules {
		rules = append(rules, v)
	}
	w.mu.RUnlock()

	for _, rule := range rules {
		payload, send, err := rule.Subscribe()
		if err != nil {
			return fmt.Errorf("build subscribe message failed key=%s: %w", rule.Key(), err)
		}

		if !send || len(payload) == 0 {
			continue
		}

		if err := writeWSMessage(conn, payload, w.WriteDeadline); err != nil {
			return fmt.Errorf("send subscribe message failed key=%s: %w", rule.Key(), err)
		}
	}

	return nil
}

func (w *WebSocketConnector) pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	if w.PingInterval <= 0 {
		return
	}

	ticker := time.NewTicker(w.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(w.WriteDeadline))
			if err := conn.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
				return
			}
		}
	}
}

func (w *WebSocketConnector) readLoop(conn *websocket.Conn) error {
	for {
		select {
		case <-w.ctx.Done():
			return nil
		default:
		}

		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read websocket message failed: %w", err)
		}

		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}

		if err := w.dispatchIncoming(data); err != nil {
			if w.logger != nil {
				w.logger.Printf("[WebSocketConnector] dispatch failed: %v", err)
			}
		}
	}
}

func (w *WebSocketConnector) dispatchIncoming(data []byte) error {
	w.mu.RLock()
	sink := w.sink
	rules := make(map[string]WSSubscriptionBinding, len(w.rules))
	for k, v := range w.rules {
		rules[k] = v
	}
	dispatchFn := w.dispatchFn
	w.mu.RUnlock()

	if sink == nil {
		return fmt.Errorf("sink is nil")
	}
	if dispatchFn == nil {
		return fmt.Errorf("websocket dispatch function is nil")
	}

	// 默认统一 decode 为 map[string]any
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("decode websocket message to map[string]any failed: %w", err)
	}

	msg := stream.Message[map[string]any]{
		Payload:    payload,
		Ts:         time.Now().UnixMilli(),
		IngestTime: time.Now().UnixMilli(),
	}

	k1 := dispatchFn(msg)
	if k1 == nil {
		return nil
	}

	key := strings.TrimSpace(*k1)
	if key == "" {
		return fmt.Errorf(fmt.Sprintf("websocket dispatch returned empty rule key(%s)", key))
	}

	rule, ok := rules[key]
	if !ok {
		return fmt.Errorf("websocket dispatch rule key not found: %s", key)
	}

	outMsg := stream.Message[any]{
		Key:        msg.Key,
		Payload:    msg.Payload,
		Ts:         msg.Ts,
		IngestTime: msg.IngestTime,
	}

	if err := sink(rule.To(), outMsg); err != nil {
		return fmt.Errorf("sink websocket message failed key=%s endpoint=%s: %w", key, rule.To().Name, err)
	}

	return nil
}

func IsWebSocketToInlineMatch(from, target stream.Endpoint) bool {
	if from.Kind != stream.ConnectorsKind || target.Kind != stream.ConnectorsKind {
		return false
	}

	return strings.EqualFold(strings.TrimSpace(from.EndpointSourceId), strings.TrimSpace(target.EndpointSourceId))
}

func writeWSMessage(conn *websocket.Conn, payload []byte, timeout time.Duration) error {
	if conn == nil {
		return errors.New("websocket conn is nil")
	}
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func extractAnyMap(v any) map[string]any {

	out := map[string]any{}
	if v == nil {
		return nil
	}

	switch m := v.(type) {
	case map[string]any:
		for k, v1 := range m {
			out[k] = v1
		}
	default:
		fmt.Println(reflect.TypeOf(v).String())
	}

	return out
}

func joinErrors(errs ...error) error {
	filtered := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}

	var sb strings.Builder
	sb.WriteString("multiple errors:")
	for i, err := range filtered {
		sb.WriteString(fmt.Sprintf(" [%d] %v;", i+1, err))
	}
	return errors.New(sb.String())
}
