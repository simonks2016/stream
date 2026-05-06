package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/simonks2016/stream/connectors"
	"github.com/simonks2016/stream/stream"
)

type Test1 struct{}

func (t *Test1) Process(ctx context.Context, in stream.Message[string]) (stream.Endpoint, stream.Message[map[string]any], bool, error) {

	return WebSocket("order.place", WithWebSocketParams(map[string]any{})),
		stream.NewMessage[map[string]any](map[string]any{
			"op":     "order",
			"symbol": "BTC-USDT",
			"sz":     "0.565",
			"px":     "86400",
		}),
		true,
		nil
}

type Test2 struct{}

func (t *Test2) Process(ctx context.Context, in stream.Message[map[string]any]) (stream.Endpoint, stream.Message[string], bool, error) {

	fmt.Println(in.Payload)

	return stream.NullEndpoint(),
		stream.EmptyMessage[string](),
		false,
		nil
}

func TestNewPipeline(t *testing.T) {

	var ctx = context.Background()

	p := NewPipeline(ctx)

	p.AddConnector(
		connectors.UseHttp(ctx).On(
			Bind[map[string]any](
				HttpPost("http://127.0.0.1:8080/test"),
				Inline("evt.inference.complete"),
				&HttpCoder{},
			)))

	p.On(
		Inline("evt.inference.complete"),
		WrapProcessor[string, map[string]any](&Test1{}),
	)

	go func() {
		time.Sleep(2 * time.Second)

		if err := p.Publish(
			HttpPost("http://127.0.0.1:8080/test"),
			stream.NewMessage[any](
				map[string]any{
					"symbol": "BTC-USDT",
				}),
		); err != nil {
			t.Fatal(err)
		}

	}()

	_ = p.Run()

}

type HttpCoder struct{}

func (j *HttpCoder) Unmarshal(data []byte) (stream.Message[map[string]any], error) {
	var d1 = make(map[string]any)
	if err := json.Unmarshal(data, &d1); err != nil {
		return stream.EmptyMessage[map[string]any](), err
	} else {
		return stream.NewMessage(d1), nil
	}
}

func (j *HttpCoder) Marshal(msg stream.Message[map[string]any]) ([]byte, error) {
	return json.Marshal(msg.Payload)
}

func NewStringPtr(s string) *string {
	return &s
}
