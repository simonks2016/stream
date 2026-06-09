package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

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

	p.Job(
		NewJob[map[string]any, string](
			Inline("test1"),
			NewDefaultJob(),
		),
		NewJob[string, string](
			Inline("test2"),
			NewDefaultJob2()),
	)

	go func() {
		time.Sleep(2 * time.Second)

		if err := p.Publish(
			Inline("test1"),
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

type DefaultJob struct{}

func (d DefaultJob) Process(ctx context.Context, m stream.Message[map[string]any], j stream.JobCollector[string]) error {
	//TODO implement me

	fmt.Println(m.Payload)

	j.Collect(
		Inline("test2"),
		m.Payload["symbol"].(string),
	)

	return nil

}

func NewDefaultJob() *DefaultJob {

	return &DefaultJob{}

}

type DefaultJob2 struct{}

func (d DefaultJob2) Process(ctx context.Context, m stream.Message[string], j stream.JobCollector[string]) error {

	fmt.Println(m.Payload)
	j.Drop()
	return nil
}
func NewDefaultJob2() *DefaultJob2 {
	return &DefaultJob2{}
}
