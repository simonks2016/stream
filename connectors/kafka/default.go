package kafka

import (
	"context"
	"eventBus/stream"
)

type KafkaConnector struct {
}

func (k *KafkaConnector) Ingest(ctx context.Context, sink stream.Sink) error {
	//TODO implement me
	panic("implement me")
}

func (k *KafkaConnector) Emit(ctx context.Context, msg stream.Message[any]) error {
	//TODO implement me
	panic("implement me")
}
