package connectors

import "eventBus/connectors/kafka"

func UseKafka(brokers ...string) *kafka.KafkaConnector {

	return &kafka.KafkaConnector{}

}
