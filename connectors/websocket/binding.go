package websocket

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/simonks2016/stream/stream"
)

type WSSubscriptionBinding interface {
	stream.ConnectorBinding
	Key() string
	Subscribe() ([]byte, bool, error)
}

type WsSubscribeBinding struct {
	from      stream.Endpoint
	to        stream.Endpoint
	mode      stream.BindingMode
	subscribe map[string]any
}

func (w *WsSubscribeBinding) Key() string {
	return strings.TrimSpace(w.from.EndpointSourceId)
}
func (w *WsSubscribeBinding) From() stream.Endpoint    { return w.from }
func (w *WsSubscribeBinding) To() stream.Endpoint      { return w.to }
func (w *WsSubscribeBinding) Mode() stream.BindingMode { return w.mode }

// Decode 编码成map[string]any
func (w *WsSubscribeBinding) Decode(data []byte) (stream.Message[any], error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return stream.Message[any]{}, err
	}
	return stream.Message[any]{
		Payload:    payload,
		Ts:         time.Now().UnixMilli(),
		IngestTime: time.Now().UnixMilli(),
	}, nil
}

// Encode 转码成[]byte
func (w *WsSubscribeBinding) Encode(msg stream.Message[any]) ([]byte, bool, error) {
	if msg.Payload == nil {
		return nil, false, nil
	}
	raw, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (w *WsSubscribeBinding) Subscribe() ([]byte, bool, error) {

	if len(w.subscribe) == 0 || w.subscribe == nil {
		return nil, false, nil
	}

	raw, err := json.Marshal(w.subscribe)
	if err != nil {
		return nil, false, fmt.Errorf("marshal subscribe payload failed: %w", err)
	}
	return raw, true, nil
}

func NewWsSubscribeBinding(from, to stream.Endpoint, mode stream.BindingMode) *WsSubscribeBinding {

	meta, ok := from.Meta[MetaWSSubscribe].(map[string]any)
	if !ok {
		meta = nil
	}

	return &WsSubscribeBinding{
		from:      from,
		to:        to,
		mode:      mode,
		subscribe: meta,
	}
}
