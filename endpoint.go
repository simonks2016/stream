package stream

import "github.com/simonks2016/stream/stream"

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
