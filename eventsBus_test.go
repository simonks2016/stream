package stream

import (
	"context"
	"encoding/json"
	"log"
	"testing"

	"github.com/simonks2016/stream/connectors"
	"github.com/simonks2016/stream/connectors/kafka"
	"github.com/simonks2016/stream/stream"
)

type JSONCoder[T any] struct {
}

func (j *JSONCoder[T]) Unmarshal(data []byte) (stream.Message[T], error) {

	var j1 T

	if err := json.Unmarshal(data, &j1); err != nil {
		return stream.EmptyMessage[T](), err
	} else {
		return stream.NewMessage[T](j1), nil
	}

}
func (j *JSONCoder[T]) Marshal(msg stream.Message[T]) ([]byte, error) {
	return json.Marshal(msg.Payload)
}

func TestNewPipeline(t *testing.T) {

	type Data struct{}

	var ctx = context.Background()

	p := NewPipeline(ctx)

	p.AddConnector(
		connectors.UseKafka(
			ctx,
			kafka.WithBrokers("127.0.0.1:19092"),
			kafka.WithGroupId("test-kafka"),
			kafka.WithContext(context.Background()),
			kafka.WithLogger(log.Default()),
		).On(
			Bind[Data](Kafka("evt.bookFeature.created"), Inline("evt.bookFeature.created"), &JSONCoder[Data]{}),
			Bind[Data](Kafka("evt.tradeFeature.created"), Inline("evt.tradeFeature.created"), &JSONCoder[Data]{}),
		),
	)

	p.On(
		Inline("evt.bookFeature.created"),
	)

}
