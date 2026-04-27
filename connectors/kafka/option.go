package kafka

import (
	"log"
	"time"
)

type Option func(*KafkaConnector)

// WithBrokers 设置brokers地址
func WithBrokers(brokers ...string) Option {
	return func(connector *KafkaConnector) { connector.Brokers = brokers }
}

// WithGroupId 设置group id
func WithGroupId(id string) Option {
	return func(connector *KafkaConnector) { connector.GroupId = id }
}

// WithLogger 设置log
func WithLogger(logger *log.Logger) Option {
	return func(connector *KafkaConnector) {
		connector.logger = logger
	}
}

// WithBatchSize 设置批次大小
func WithBatchSize(batchSize int) Option {
	return func(connector *KafkaConnector) {
		connector.batchSize = batchSize
	}
}

// WithAsync 设置是否异步
func WithAsync(async bool) Option {
	return func(connector *KafkaConnector) {
		connector.async = async
	}
}

// WithBatchTimeout 设置批次时间
func WithBatchTimeout(batchTimeout time.Duration) Option {
	return func(connector *KafkaConnector) {
		connector.batchTimeout = batchTimeout
	}
}
