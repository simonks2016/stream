package main

import (
	"context"
	"eventBus/stream"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

type Data struct {
	Content string
}

type TestS struct{}

func (t *TestS) Process(ctx context.Context, in stream.Message[Data]) (stream.Message[Data], bool, error) {

	print(in.Payload.Content)

	return stream.NewMessage[Data](
		stream.Inline("test"),
		Data{Content: "hello world"},
	), false, nil
}

func TestCore(t *testing.T) {

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pipeline := NewPipeline(context.Background())
	done := make(chan stream.Message[any], 1)

	pipeline.On(
		stream.Inline("test"),
		stream.WrapProcessor[Data, Data](&TestS{}),
	)

	if err := pipeline.Start(); err != nil {
		t.Fatal(err)
	}

	err := pipeline.Publish(stream.NewMessage[any](
		stream.Inline("test"),
		Data{Content: "hello"},
	))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-done:
		data, ok := got.Payload.(Data)
		if !ok {
			t.Fatalf("unexpected payload type: %T", got.Payload)
		}
		if data.Content != "hello" {
			t.Fatalf("unexpected content: %s", data.Content)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}

}
