package connectors

import (
	"context"

	h1 "github.com/simonks2016/stream/connectors/http"
	"github.com/simonks2016/stream/connectors/kafka"
	w1 "github.com/simonks2016/stream/connectors/websocket"
)

func UseKafka(ctx context.Context, options ...kafka.Option) *kafka.KafkaConnector {

	k := kafka.NewKafkaConnector(ctx)

	for _, opt := range options {
		opt(k)
	}
	return k
}

func UseHttp(ctx context.Context, options ...h1.Option) *h1.HttpConnector {

	k := h1.NewHttpConnector(ctx)

	for _, opt := range options {
		opt(k)
	}
	return k
}

func UseWebsocket(ctx context.Context, url string, opts ...w1.Option) *w1.WebSocketConnector {

	w := w1.NewWebSocketConnector(ctx, url)
	for _, opt := range opts {
		opt(w)
	}
	return w
}
