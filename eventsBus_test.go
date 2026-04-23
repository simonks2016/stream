package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	w3 "github.com/gorilla/websocket"
	"github.com/simonks2016/stream/connectors"
	"github.com/simonks2016/stream/connectors/websocket"
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
		connectors.UseWebsocket(ctx,
			"ws://127.0.0.1:8765",
			websocket.WithOnConnected(func(ctx context.Context, conn *w3.Conn) error {

				var d1 = make(map[string]any)
				d1["user"] = "111"
				d1["op"] = "login"
				d1["password"] = "OK"

				if data, err := json.Marshal(d1); err != nil {
					return nil
				} else {
					return conn.WriteMessage(w3.TextMessage, data)
				}
			}),
			websocket.WithOnDispatch(func(in stream.Message[map[string]any]) *string {

				op, ok := in.Payload["op"].(string)
				if !ok {
					return nil
				}
				switch strings.ToLower(op) {
				case strings.ToLower("order"):
					return NewStringPtr("order.place")
				case strings.ToLower("positionAndBalance"):
					return NewStringPtr("positionAndBalance")
				default:
					return nil
				}
			}),
		).On(
			WsSubscribe(
				WebSocket("positionAndBalance",
					WithWebSocketParams(map[string]any{
						"user":   "111",
						"op":     "positionAndBalance",
						"symbol": "BTC-USDT",
					})),
				Inline("evt.positionAndBalance.updated")),
			WsSubscribe(
				WebSocket("order.place"),
				Inline("evt.order.placed")),
		),
	)
	p.On(
		Inline("sevt.data.get"),
		WrapProcessor[string, map[string]any](&Test1{}),
	)

	p.On(
		Inline("evt.positionAndBalance.updated"),
		WrapProcessor[map[string]any, string](&Test2{}),
	)
	p.On(
		Inline("evt.order.placed"),
		WrapProcessor[map[string]any, string](&Test2{}),
	)

	NewScheduler().On(
		WrapSchedulerJob(
			WithInterval(time.Minute),
			WithName("schedular"),
			WithTargetEndPoint(Inline("sevt.data.get")),
			WithMessageFactory[string](func() stream.Message[string] { return stream.NewMessage[string]("OK") }),
		),
	).Register(p).Run(ctx)

	if err := p.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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
