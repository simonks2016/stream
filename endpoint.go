package stream

import (
	httpconnector "github.com/simonks2016/stream/connectors/http"
	w1 "github.com/simonks2016/stream/connectors/websocket"
	"github.com/simonks2016/stream/stream"
)

func Inline(name string) stream.Endpoint {
	return stream.Endpoint{
		Kind: stream.InlineKind,
		Name: name,
		Meta: map[string]interface{}{
			"topic": name,
		},
		EndpointSourceId: name,
	}
}

func Kafka(topic string) stream.Endpoint {
	return stream.Endpoint{
		Kind: stream.ConnectorsKind,
		Name: "kafka",
		Meta: map[string]any{
			"topic": topic,
		},
		EndpointSourceId: topic,
	}
}

func HttpGet(url string, opts ...HttpConnectorOption) stream.Endpoint {
	e := stream.Endpoint{
		Kind: stream.ConnectorsKind,
		Name: "http",
		Meta: map[string]any{
			"url":    url,
			"method": "get",
		},
		EndpointSourceId: url,
	}

	for _, opt := range opts {
		opt(&e)
	}
	return e
}

func HttpPost(url string, opts ...HttpConnectorOption) stream.Endpoint {
	e := stream.Endpoint{
		Kind: stream.ConnectorsKind,
		Name: "http",
		Meta: map[string]any{
			"url":    url,
			"method": "post",
		},
		EndpointSourceId: url,
	}

	for _, opt := range opts {
		opt(&e)
	}
	return e
}

type HttpConnectorOption func(endpoint *stream.Endpoint)
type WebSocketConnectorOption func(endpoint *stream.Endpoint)

func WithHttpHeader(key, value string) HttpConnectorOption {
	return func(endpoint *stream.Endpoint) {
		if endpoint.Meta == nil {
			endpoint.Meta = make(map[string]any)
		}

		headers, exists := endpoint.Meta[httpconnector.MetaHeaders]
		if !exists {
			rawHeaders := make(map[string]string)
			rawHeaders[key] = value
			endpoint.Meta[httpconnector.MetaHeaders] = rawHeaders
			return
		}

		h, ok := headers.(map[string]string)
		if !ok {
			panic("invalid http header type")
		}

		h[key] = value
		endpoint.Meta[httpconnector.MetaHeaders] = h
	}
}

func WithHttpHeaders(headers map[string]string) HttpConnectorOption {
	return func(endpoint *stream.Endpoint) {
		if endpoint.Meta == nil {
			endpoint.Meta = make(map[string]any)
		}

		rawHeaders := make(map[string]string, len(headers))
		for k, v := range headers {
			rawHeaders[k] = v
		}
		endpoint.Meta[httpconnector.MetaHeaders] = rawHeaders
	}
}

func WithQueryParam(key string, value any) HttpConnectorOption {
	return func(endpoint *stream.Endpoint) {
		if endpoint.Meta == nil {
			endpoint.Meta = make(map[string]any)
		}

		query, ex := endpoint.Meta[httpconnector.MetaQuery]
		if !ex {
			rawQuery := make(map[string]any)
			rawQuery[key] = value
			endpoint.Meta[httpconnector.MetaQuery] = rawQuery
			return
		}

		q, ok := query.(map[string]any)
		if !ok {
			panic("invalid query type")
		}

		q[key] = value
		endpoint.Meta[httpconnector.MetaQuery] = q
	}
}

func WithQuery(query map[string]any) HttpConnectorOption {
	return func(endpoint *stream.Endpoint) {
		if endpoint.Meta == nil {
			endpoint.Meta = make(map[string]any)
		}

		rawQuery := make(map[string]any, len(query))
		for k, v := range query {
			rawQuery[k] = v
		}
		endpoint.Meta[httpconnector.MetaQuery] = rawQuery
	}
}

type WebSocketOption func(*stream.Endpoint)

func WebSocket(name string, opts ...WebSocketOption) stream.Endpoint {
	ep := stream.Endpoint{
		Kind: stream.ConnectorsKind,
		Name: "websocket",
		Meta: map[string]any{
			"key": name,
		},
		EndpointSourceId: name,
	}
	for _, opt := range opts {
		opt(&ep)
	}
	return ep
}
func WithWebSocketParams(params map[string]any) WebSocketOption {

	return func(endpoint *stream.Endpoint) {
		endpoint.Meta[w1.MetaWSSubscribe] = params
	}
}
