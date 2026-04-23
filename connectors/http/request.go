package httpconnector

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/simonks2016/stream/stream"
)

type HTTPRequestConfig struct {
	Method  string
	URL     string
	Timeout time.Duration
	Headers map[string]string
	Query   map[string]any
	ErrorTo string
}

func (h *HttpConnector) resolveRequestConfig(httpEP stream.Endpoint) HTTPRequestConfig {
	cfg := HTTPRequestConfig{
		Method:  http.MethodPost,
		URL:     "",
		Timeout: h.RequestTimeout,
		Headers: map[string]string{},
		Query:   map[string]any{},
		ErrorTo: DefaultHTTPErrorEvent,
	}

	if strings.TrimSpace(fmt.Sprintf("%v", httpEP.Meta[MetaMethod])) != "" {
		cfg.Method = strings.ToUpper(getString(httpEP.Meta, MetaMethod, http.MethodPost))
	}
	if strings.TrimSpace(fmt.Sprintf("%v", httpEP.Meta[MetaURL])) != "" {
		cfg.URL = getString(httpEP.Meta, MetaURL, "")
	}

	if v := getDuration(httpEP.Meta, MetaTimeoutMS, 0); v > 0 {
		cfg.Timeout = v
	}

	if v := getString(httpEP.Meta, MetaErrorTo, ""); strings.TrimSpace(v) != "" {
		cfg.ErrorTo = v
	}

	// headers：endpoint 覆盖默认
	for k, v := range h.defaultHeaders() {
		cfg.Headers[k] = v
	}
	for k, v := range extractStringMap(httpEP.Meta[MetaHeaders]) {
		cfg.Headers[k] = v
	}

	// query：endpoint 覆盖默认
	for k, v := range h.defaultQuery() {
		cfg.Query[k] = v
	}
	for k, v := range extractAnyMap(httpEP.Meta[MetaQuery]) {
		cfg.Query[k] = v
	}

	cfg.Method = strings.ToUpper(strings.TrimSpace(cfg.Method))
	if cfg.Method == "" {
		cfg.Method = http.MethodPost
	}
	return cfg
}

func (h *HttpConnector) defaultHeaders() map[string]string {
	return map[string]string{}
}

func (h *HttpConnector) defaultQuery() map[string]any {
	return map[string]any{}
}

func (h *HttpConnector) resolveErrorEndpoint(httpEP stream.Endpoint) stream.Endpoint {
	// 1. endpoint 自己配置的 error_to 优先
	if httpEP.Meta != nil {
		if v, ok := httpEP.Meta[MetaErrorTo]; ok && v != nil {
			name := strings.TrimSpace(fmt.Sprintf("%v", v))
			if name != "" {
				return stream.Endpoint{
					Kind: stream.InlineKind,
					Name: name,
				}
			}
		}
	}

	// 2. connector 初始化默认值
	if h.DefaultErrorEndpoint.Kind != 0 && strings.TrimSpace(h.DefaultErrorEndpoint.Name) != "" {
		return h.DefaultErrorEndpoint
	}

	// 3. 系统兜底
	return stream.Endpoint{
		Kind: stream.InlineKind,
		Name: "evt.sysHttp.failed",
	}
}
