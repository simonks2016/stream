package httpconnector

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/simonks2016/stream/stream"
)

const (
	DefaultHTTPConnectorName = "http"
	DefaultHTTPErrorEvent    = "evt.sysHttp.Failed"

	MetaURL       = "url"
	MetaMethod    = "method"
	MetaTimeoutMS = "timeout_ms"
	MetaHeaders   = "headers"
	MetaQuery     = "query"
	MetaErrorTo   = "error_to"

	HeaderRequestID = "X-Request-Id"
)

type HttpConnector struct {
	ctx    context.Context
	cancel context.CancelFunc

	sink stream.Sink

	mu       sync.RWMutex
	bindings []stream.ConnectorBinding

	client    *http.Client
	transport *http.Transport

	wg      sync.WaitGroup
	started bool
	logger  *log.Logger

	// pool config
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ExpectContinueTimeout time.Duration
	RequestTimeout        time.Duration
	DisableKeepAlives     bool
	InsecureSkipVerify    bool
	DefaultErrorEndpoint  stream.Endpoint
}

func NewHttpConnector(parent context.Context) *HttpConnector {
	ctx, cancel := context.WithCancel(parent)
	return &HttpConnector{
		ctx:                   ctx,
		cancel:                cancel,
		bindings:              make([]stream.ConnectorBinding, 0, 8),
		logger:                log.Default(),
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		RequestTimeout:        10 * time.Second,
		DisableKeepAlives:     false,
		InsecureSkipVerify:    false,
		DefaultErrorEndpoint: stream.Endpoint{
			Kind:             stream.InlineKind,
			Name:             "evt.sysHttp.Failed",
			Meta:             nil,
			EndpointSourceId: "evt.sysHttp.Failed",
		},
	}
}

func (h *HttpConnector) Name() string {
	return DefaultHTTPConnectorName
}

func (h *HttpConnector) On(op ...stream.ConnectorBinding) stream.Connector {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bindings = append(h.bindings, op...)
	return h
}

func (h *HttpConnector) Ingest(ctx context.Context, sink stream.Sink) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ctx != nil {
		if h.cancel != nil {
			h.cancel()
		}
		h.ctx, h.cancel = context.WithCancel(ctx)
	}

	h.sink = sink
	return h.runLocked()
}

func (h *HttpConnector) Run() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runLocked()
}

func (h *HttpConnector) runLocked() error {
	if h.started {
		return nil
	}
	if h.ctx == nil {
		h.ctx = context.Background()
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	h.transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          h.MaxIdleConns,
		MaxIdleConnsPerHost:   h.MaxIdleConnsPerHost,
		MaxConnsPerHost:       h.MaxConnsPerHost,
		IdleConnTimeout:       h.IdleConnTimeout,
		TLSHandshakeTimeout:   h.TLSHandshakeTimeout,
		ExpectContinueTimeout: h.ExpectContinueTimeout,
		DisableKeepAlives:     h.DisableKeepAlives,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: h.InsecureSkipVerify,
		},
	}

	h.client = &http.Client{
		Transport: h.transport,
		Timeout:   h.RequestTimeout,
	}

	h.started = true
	return nil
}

func (h *HttpConnector) Stop() {
	h.mu.Lock()
	if h.cancel != nil {
		h.cancel()
	}
	transport := h.transport
	h.mu.Unlock()

	if transport != nil {
		transport.CloseIdleConnections()
	}

	h.wg.Wait()
}

func (h *HttpConnector) Emit(ctx context.Context, fromEndpoint stream.Endpoint, msg stream.Message[any]) error {
	h.mu.RLock()
	client := h.client
	bindings := append([]stream.ConnectorBinding(nil), h.bindings...)
	sink := h.sink
	baseCtx := h.ctx
	h.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("http client is nil")
	}
	if sink == nil {
		return fmt.Errorf("sink is nil")
	}

	matched := false
	for _, binding := range bindings {
		if binding.Mode() == stream.ReadOnly {
			continue
		}
		if !IsHTTPBindingMatch(binding.From(), fromEndpoint) {
			continue
		}
		matched = true

		callCtx := ctx
		if callCtx == nil {
			callCtx = baseCtx
		}
		if callCtx == nil {
			callCtx = context.Background()
		}

		requestID := buildRequestID(msg)

		h.wg.Go(func() {
			h.doRequest(
				callCtx,
				binding,
				fromEndpoint,
				msg,
				requestID)
		})
	}

	if !matched {
		return fmt.Errorf("message has no routing destination (msg=%s,ts=%d,from=http)", msg.Key, msg.Ts)
	}
	return nil
}

func (h *HttpConnector) doRequest(
	ctx context.Context,
	binding stream.ConnectorBinding,
	httpEP stream.Endpoint,
	reqMsg stream.Message[any],
	requestID string,
) {
	cfg := h.resolveRequestConfig(httpEP)
	errEP := h.resolveErrorEndpoint(httpEP)

	if strings.TrimSpace(cfg.URL) == "" {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("http endpoint meta.url is empty"), httpEP)
		return
	}

	reqCtx := ctx
	var cancel context.CancelFunc
	if cfg.Timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	u, err := url.Parse(cfg.URL)
	if err != nil {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("parse url failed: %w", err), httpEP)
		return
	}

	var body io.Reader
	switch cfg.Method {
	case http.MethodGet, http.MethodHead:
		q := u.Query()

		for k, v := range cfg.Query {
			q.Set(k, fmt.Sprintf("%v", v))
		}

		data, OK := reqMsg.Payload.(map[string]any)
		if !OK {
			h.emitError(errEP, reqMsg, requestID, fmt.Errorf("the input params must be map[string]any"), httpEP)
			return
		}

		for k, v := range data {
			q.Set(k, fmt.Sprintf("%v", v))
		}

		u.RawQuery = q.Encode()

	default:

		if binding.Encode != nil {
			payload, _, err := binding.Encode(reqMsg)
			if err != nil {
				h.emitError(errEP, reqMsg, requestID, fmt.Errorf("encode request failed: %w", err), httpEP)
				return
			}
			body = bytes.NewReader(payload)
		} else {
			h.emitError(errEP, reqMsg, requestID, fmt.Errorf("the encode funcation is null"), httpEP)
			return
		}
	}

	req, err := http.NewRequestWithContext(reqCtx, cfg.Method, u.String(), body)
	if err != nil {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("build request failed: %w", err), httpEP)
		return
	}

	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set(HeaderRequestID, requestID)

	if cfg.Method != http.MethodGet && cfg.Method != http.MethodHead {
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("http request failed method=%s url=%s: %w", cfg.Method, u.String(), err), httpEP)
		return
	}
	// 关闭输入流
	defer func() { _ = resp.Body.Close() }()
	// 限制5MB
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("read response body failed: %w", err), httpEP)
		return
	}

	if resp.StatusCode >= 400 {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf(
			"http response bad status=%d method=%s url=%s body=%s",
			resp.StatusCode, cfg.Method, u.String(), string(respBytes)), httpEP)
		return
	}

	respMsg, err := binding.Decode(respBytes)
	if err != nil {
		h.emitError(errEP, reqMsg, requestID, fmt.Errorf("decode response failed: %w", err), httpEP)
		return
	}
	// 将请求ID设置为Message Key
	if len(respMsg.Key) <= 0 {
		respMsg.Key = requestID
	}

	now := time.Now().UnixMilli()
	if respMsg.Ts == 0 {
		respMsg.Ts = now
	}
	if respMsg.IngestTime == 0 {
		respMsg.IngestTime = now
	}
	// 设置请求ID到信息当中
	respMsg.Key = requestID

	h.mu.RLock()
	sink := h.sink
	logger := h.logger
	h.mu.RUnlock()

	if sink == nil {
		return
	}

	if err = sink(binding.To(), respMsg); err != nil {
		if logger != nil {
			logger.Printf("[HttpConnector] sink response failed request_id=%s err=%v", requestID, err)
		}
		return
	}

	if logger != nil {
		logger.Printf("[HttpConnector] request success request_id=%s method=%s url=%s", requestID, cfg.Method, u.String())
	}
}

func IsHTTPBindingMatch(from, target stream.Endpoint) bool {
	if from.Kind != stream.ConnectorsKind {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(from.Name), DefaultHTTPConnectorName) {
		return false
	}

	targetURl, ok := target.Meta["url"].(string)
	if !ok {
		return false
	}

	rawURl, ok := from.Meta["url"].(string)
	if !ok {
		return false
	}

	return strings.EqualFold(targetURl, rawURl)
}

func (h *HttpConnector) emitError(
	errEP stream.Endpoint,
	reqMsg stream.Message[any],
	requestID string,
	err error,
	httpEP stream.Endpoint,
) {
	h.mu.RLock()
	sink := h.sink
	logger := h.logger
	h.mu.RUnlock()

	if logger != nil {
		logger.Printf("[HttpConnector] request failed request_id=%s err=%v", requestID, err)
	}

	if sink == nil {
		return
	}

	errMsg := stream.Message[any]{
		Key:        requestID,
		Ts:         time.Now().UnixMilli(),
		IngestTime: time.Now().UnixMilli(),
		Payload: map[string]any{
			"request_id": requestID,
			"origin_key": reqMsg.Key,
			"error":      err.Error(),
			"http": map[string]any{
				"url":    getString(httpEP.Meta, MetaURL, ""),
				"method": strings.ToUpper(getString(httpEP.Meta, MetaMethod, http.MethodPost)),
			},
		},
	}

	_ = sink(errEP, errMsg)
}

func buildRequestID(msg stream.Message[any]) string {
	if strings.TrimSpace(msg.Key) != "" {
		return fmt.Sprintf("%s-%s", msg.Key, uuid.NewString())
	}
	return uuid.NewString()
}

func mergePayloadIntoQuery(q url.Values, payload []byte) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil
	}

	var obj map[string]any
	if err := json.Unmarshal(trimmed, &obj); err == nil && obj != nil {
		for k, v := range obj {
			q.Set(k, formatQueryValue(v))
		}
		return nil
	}

	q.Set("payload", base64.StdEncoding.EncodeToString(trimmed))
	return nil
}

func formatQueryValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int8, int16, int32, int64:
		return fmt.Sprintf("%d", x)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	}
}

func getString(meta map[string]any, key, def string) string {
	if meta == nil {
		return def
	}
	v, ok := meta[key]
	if !ok || v == nil {
		return def
	}
	return fmt.Sprintf("%v", v)
}

func getDuration(meta map[string]any, key string, def time.Duration) time.Duration {
	if meta == nil {
		return def
	}
	v, ok := meta[key]
	if !ok || v == nil {
		return def
	}

	switch x := v.(type) {
	case int:
		return time.Duration(x) * time.Millisecond
	case int64:
		return time.Duration(x) * time.Millisecond
	case float64:
		return time.Duration(int64(x)) * time.Millisecond
	case string:
		if strings.TrimSpace(x) == "" {
			return def
		}
		if d, err := time.ParseDuration(x); err == nil {
			return d
		}
		if n, err := strconv.Atoi(x); err == nil {
			return time.Duration(n) * time.Millisecond
		}
	}
	return def
}

func extractStringMap(v any) map[string]string {
	out := map[string]string{}
	if v == nil {
		return out
	}
	switch m := v.(type) {
	case map[string]string:
		for k, v := range m {
			out[k] = v
		}
	case map[string]any:
		for k, v := range m {
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out
}

func extractAnyMap(v any) map[string]any {
	out := map[string]any{}
	if v == nil {
		return out
	}
	switch m := v.(type) {
	case map[string]any:
		for k, v := range m {
			out[k] = v
		}
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
