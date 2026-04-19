package connectors

import (
	"context"

	"github.com/simonks2016/stream/connectors/kafka"
)

func UseKafka(ctx context.Context, options ...kafka.Option) *kafka.KafkaConnector {

	k := kafka.NewKafkaConnector(ctx)

	for _, opt := range options {
		opt(k)
	}
	return k
}
