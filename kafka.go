package kafka

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

//Message := exporting kafka.Message
type Message = kafka.Message

//Reader := exporting kafka.Reader
type Reader = *kafka.Reader

//Config :- kafka config info
type Config struct {
	Topic     string
	Partition int
	URL       string
	GroupID   string
	MinBytes  int
	MaxBytes  int
}

//IsKafkaReady :- Check if kafka is ready for connection
func (c *Config) IsKafkaReady() bool {
	_, err := kafka.DialLeader(context.Background(), "tcp", c.URL, c.Topic, c.Partition)
	fmt.Println("cannot connect to kafka", err)
	if err != nil {
		return false
	}
	return true
}

//Writer :- func to write data to kafka
func (c *Config) Write(message string, callback func(bool)) {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{c.URL},
		Topic:    c.Topic,
		Balancer: &kafka.LeastBytes{},
	})

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
		},
	)

	if err != nil {
		callback(false)
		return
	}

	callback(false)
}

//Reader :- read msg from kafka
func (c *Config) Reader(readMessageCallback func(reader *kafka.Reader, m kafka.Message)) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{c.URL},
		Topic:     c.Topic,
		Partition: c.Partition,
		MinBytes:  c.MinBytes,
		MaxBytes:  c.MaxBytes,
		GroupID:   c.GroupID,
	})
	ctx := context.Background()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		readMessageCallback(r, m)
	}

}

//Commit :- commit msg to kafka
func Commit(r *kafka.Reader, m kafka.Message) {
	ctx := context.Background()
	r.CommitMessages(ctx, m)
}
