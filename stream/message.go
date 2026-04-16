package stream

import "time"

type Message[T any] struct {
	Ts          int64    `json:"ts"`
	Payload     T        `json:"payload"`
	Endpoint    Endpoint `json:"endpoint"`
	IngestTime  int64    `json:"ingest_time"`
	WatermarkTs int64    `json:"watermark_ts"` // Watermark参考时间
	SinkTime    int64    `json:"sink_time"`    // 输出时间（离开系统）
	Key         string   `json:"key"`
}

func NewMessage[T any](topic Endpoint, data T) Message[T] {
	now := time.Now().UnixMilli()

	return Message[T]{
		Ts:          now,
		IngestTime:  now,
		WatermarkTs: now,
		SinkTime:    0,
		Payload:     data,
		Endpoint:    topic,
	}
}
func (m *Message[data]) WithWatermarkTs(watermarkTs int64) *Message[data] {
	m.WatermarkTs = watermarkTs
	return m
}
func (m *Message[data]) Finish() *Message[data] {
	m.SinkTime = time.Now().UnixMilli()
	return m
}
func (m *Message[data]) Start() *Message[data] {
	m.IngestTime = time.Now().UnixMilli()
	return m
}
func (m *Message[T]) WithTs(ts int64) *Message[T] {
	m.Ts = ts
	return m
}

// DeriveMessage 派生消息
func DeriveMessage[I any, O any](src *Message[I], endpoint Endpoint, payload O) *Message[O] {
	return &Message[O]{
		Ts:          src.Ts,
		Endpoint:    endpoint,
		Payload:     payload,
		IngestTime:  src.IngestTime,
		WatermarkTs: src.WatermarkTs,
		SinkTime:    0,
	}
}

func EmptyMessage() *Message[any] {
	return &Message[any]{
		Ts:          0,
		Endpoint:    Endpoint{},
		Payload:     nil,
		IngestTime:  0,
		WatermarkTs: 0,
		SinkTime:    0,
	}
}
