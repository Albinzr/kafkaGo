package kafka

import (
	"testing"
)

//You need to fill in your kafka config for test to work
var kafkaConfig = &Config{
	Topic:     "",
	Partition: 0,
	URL:       "",
	GroupID:   "",
	MinBytes:  0,
	MaxBytes:  0,
}

func TestHello(t *testing.T) {
	kafkaConfig.IsKafkaReady()
}
