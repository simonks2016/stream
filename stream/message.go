package stream

import (
	"github.com/google/uuid"
	"reflect"
	"time"
)

type Message[T any] struct {
	Ts          int64  `json:"ts"`
	Payload     T      `json:"payload"`
	IngestTime  int64  `json:"ingest_time"`
	WatermarkTs int64  `json:"watermark_ts"` // Watermark参考时间
	SinkTime    int64  `json:"sink_time"`    // 输出时间（离开系统）
	Key         string `json:"key"`
}

type MessageOption[T any] func(*Message[T])

func NewMessage[T any](data T, opts ...MessageOption[T]) Message[T] {
	now := time.Now().UnixMilli()

	m := Message[T]{
		Ts:          now,
		IngestTime:  now,
		WatermarkTs: now,
		SinkTime:    0,
		Payload:     data,
	}

	if len(opts) > 0 {
		for _, opt := range opts {
			opt(&m)
		}
	}
	if len(m.Key) <= 0 {
		m.Key = uuid.New().String()
	}
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

func WithTs[T any](ts int64) MessageOption[T] {
	return func(m *Message[T]) {
		m.Ts = ts
	}
}

func WithKey[T any](key string) MessageOption[T] {
	return func(m *Message[T]) {
		m.Key = key
	}
}

// DeriveMessage 派生消息
func DeriveMessage[I any, O any](src *Message[I], payload O) *Message[O] {
	return &Message[O]{
		Ts:          src.Ts,
		Payload:     payload,
		IngestTime:  src.IngestTime,
		WatermarkTs: src.WatermarkTs,
		SinkTime:    0,
	}
}

func EmptyMessage[T any]() Message[T] {

	var t1 T
	return Message[T]{
		Ts:          0,
		Payload:     t1,
		IngestTime:  0,
		WatermarkTs: 0,
		SinkTime:    0,
	}
}

func (e Message[T]) IsEmpty() bool {
	return e.Ts == 0 && e.IngestTime == 0 && e.WatermarkTs == 0 && reflect.ValueOf(e.Payload).IsNil()
}
