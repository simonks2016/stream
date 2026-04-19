package kafka

import (
	"log"
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
