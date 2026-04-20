package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/simonks2016/stream/stream"
)

type KafkaConnector struct {
	Brokers []string
	GroupId string

	ctx    context.Context
	cancel context.CancelFunc

	sink stream.Sink

	mu       sync.RWMutex
	bindings []stream.ConnectorBinding

	readers  map[string]*kafka.Reader
	producer *kafka.Writer

	wg      sync.WaitGroup
	started bool
	logger  *log.Logger
}

func NewKafkaConnector(parent context.Context) *KafkaConnector {

	ctx, cancel := context.WithCancel(parent)

	return &KafkaConnector{
		Brokers:  []string{"localhost:9092"},
		GroupId:  "",
		ctx:      ctx,
		cancel:   cancel,
		readers:  make(map[string]*kafka.Reader),
		bindings: make([]stream.ConnectorBinding, 0, 8),
		logger:   log.Default(),
	}
}

func (k *KafkaConnector) Ingest(ctx context.Context, sink stream.Sink) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if ctx != nil {
		if k.cancel != nil {
			k.cancel()
		}
		k.ctx, k.cancel = context.WithCancel(ctx)
	}

	k.sink = sink
	return k.runLocked()
}

func (k *KafkaConnector) Emit(ctx context.Context, target stream.Endpoint, msg stream.Message[any]) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.producer == nil {
		return fmt.Errorf("kafka producer is nil")
	}

	var errs []error
	matched := false

	for _, binding := range k.bindings {
		if binding.Mode() == stream.ReadOnly {
			continue
		}
		from := binding.From()
		to := binding.To()

		// 桥接规则：Kafka(topic) <-> Inline(name)
		if IsKafkaToInlineMatch(from, to, target) {
			continue
		}

		payload, ok, err := binding.Encode(msg)
		if err != nil {
			errs = append(errs, fmt.Errorf("encode binding kafka=%s inline=%s: %w", from.Name, to.Name, err))
			continue
		}
		if !ok {
			continue
		}

		matched = true

		writeCtx := ctx
		if writeCtx == nil {
			writeCtx = k.ctx
		}
		if writeCtx == nil {
			writeCtx = context.Background()
		}

		err = k.producer.WriteMessages(writeCtx, kafka.Message{
			Topic: fmt.Sprintf("%v", target.Meta["topic"]),
			Value: payload,
			Time:  time.Now(),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("write topic=%s: %w", from.Name, err))
		}
	}

	if !matched {
		return fmt.Errorf("message has no routing destination (msg=%s,ts=%d,from=kafka)", msg.Key, msg.Ts)
	}
	if len(errs) > 0 {
		return joinErrors(errs...)
	}
	return nil
}

func (k *KafkaConnector) Name() string {
	return "kafka"
}

func (k *KafkaConnector) Run() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.runLocked()
}

func (k *KafkaConnector) runLocked() error {
	if k.started {
		return nil
	}
	if len(k.Brokers) == 0 {
		return fmt.Errorf("brokers is empty")
	}
	if k.ctx == nil {
		k.ctx = context.Background()
	}

	k.producer = &kafka.Writer{
		Addr:         kafka.TCP(k.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		Compression:  kafka.Snappy,
	}

	// 只为 Kafka <-/-> Inline 的 binding 创建 reader
	for _, binding := range k.bindings {
		if binding.Mode() == stream.WriteOnly {
			continue
		}
		from := binding.From()
		to := binding.To()

		if from.Kind != stream.ConnectorsKind || to.Kind != stream.InlineKind {
			continue
		}

		topic, ok := from.Meta["topic"].(string)
		if !ok || len(topic) <= 0 {
			continue
		}

		if _, exists := k.readers[topic]; exists {
			continue
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        k.Brokers,
			GroupID:        k.GroupId,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        200 * time.Millisecond,
			CommitInterval: time.Second,
			StartOffset:    kafka.LastOffset,
		})

		k.readers[topic] = reader
	}

	for topic, reader := range k.readers {
		k.wg.Add(1)
		go k.consumeLoop(topic, reader)
	}

	k.started = true
	return nil
}

func (k *KafkaConnector) Stop() {
	k.mu.Lock()
	if k.cancel != nil {
		k.cancel()
	}
	k.mu.Unlock()

	k.mu.RLock()
	for topic, reader := range k.readers {
		if err := reader.Close(); err != nil && k.logger != nil {
			k.logger.Printf("[KafkaConnector] close reader failed topic=%s err=%v", topic, err)
		}
	}
	if k.producer != nil {
		if err := k.producer.Close(); err != nil && k.logger != nil {
			k.logger.Printf("[KafkaConnector] close producer failed err=%v", err)
		}
	}
	k.mu.RUnlock()

	k.wg.Wait()
}

func (k *KafkaConnector) On(op ...stream.ConnectorBinding) stream.Connector {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.bindings = append(k.bindings, op...)
	return k
}

func (k *KafkaConnector) consumeLoop(topic string, reader *kafka.Reader) {
	defer k.wg.Done()

	if k.logger != nil {
		k.logger.Printf("[KafkaConnector] consume loop started topic=%s", topic)
	}

	for {
		select {
		case <-k.ctx.Done():
			if k.logger != nil {
				k.logger.Printf("[KafkaConnector] consume loop stopped topic=%s", topic)
			}
			return
		default:
		}

		msg, err := reader.FetchMessage(k.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if k.logger != nil {
				k.logger.Printf("[KafkaConnector] fetch failed topic=%s err=%v", topic, err)
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if err := k.dispatchIncoming(topic, msg.Value); err != nil {
			if k.logger != nil {
				k.logger.Printf("[KafkaConnector] dispatch failed topic=%s err=%v", topic, err)
			}
			// 这里我先保持和你旧实现接近：即使失败也继续commit
			// 后面你要更稳，可以改成成功后再commit
		}

		if err := reader.CommitMessages(k.ctx, msg); err != nil && k.logger != nil {
			k.logger.Printf("[KafkaConnector] commit failed topic=%s offset=%d err=%v", topic, msg.Offset, err)
		}
	}
}

func (k *KafkaConnector) dispatchIncoming(topic string, data []byte) error {
	k.mu.RLock()
	bindings := append([]stream.ConnectorBinding(nil), k.bindings...)
	sink := k.sink
	k.mu.RUnlock()

	if sink == nil {
		return fmt.Errorf("sink is nil")
	}

	var errs []error
	matched := false

	for _, binding := range bindings {
		if binding.Mode() == stream.WriteOnly {
			continue
		}

		from := binding.From()
		to := binding.To()

		if from.Kind != stream.ConnectorsKind || to.Kind != stream.InlineKind {
			continue
		}

		settingTopic, ok := from.Meta["topic"].(string)
		if !ok {
			continue
		}

		if strings.TrimSpace(settingTopic) != topic {
			continue
		}

		matched = true

		msg, err := binding.Decode(data)
		if err != nil {
			errs = append(errs, fmt.Errorf("decode topic=%s -> inline=%s: %w", topic, to.Name, err))
			continue
		}

		if err := sink(to, msg); err != nil {
			errs = append(errs, fmt.Errorf("sink inline=%s: %w", to.Name, err))
			continue
		}
	}

	if !matched {
		return fmt.Errorf("no binding matched kafka topic=%s", topic)
	}
	if len(errs) > 0 {
		return joinErrors(errs...)
	}
	return nil
}

func joinErrors(errs ...error) error {
	filtered := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}

	var sb strings.Builder
	sb.WriteString("multiple errors:")
	for i, err := range filtered {
		sb.WriteString(fmt.Sprintf(" [%d] %v;", i+1, err))
	}
	return errors.New(sb.String())
}

func IsKafkaToInlineMatch(from, to, target stream.Endpoint) bool {

	if from.Kind != stream.ConnectorsKind && to.Kind != stream.ConnectorsKind {
		return false
	} else if target.Kind == stream.InlineKind {
		return false
	}

	var sample = func() stream.Endpoint {
		if from.Kind == stream.ConnectorsKind {
			return from
		}
		return to
	}()
	if strings.ToLower(sample.Name) != "kafka" {
		return false
	}

	fromTopic, ok1 := sample.Meta["topic"].(string)
	toTopic, ok2 := target.Meta["topic"].(string)

	if !ok1 || !ok2 {
		return false
	}
	return strings.EqualFold(fromTopic, toTopic)
}
