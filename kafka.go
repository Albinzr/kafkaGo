package kafka

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

//Message := exporting kafka.Message
type Message = kafka.Message

//Reader := exporting kafka.Reader
type Reader = *kafka.Reader

//Config :- kafka config info
type Config struct {
	Topic                 string
	Partition             int
	URL                   string
	GroupID               string
	MinBytes              int
	MaxBytes              int
	ReadWithLimitFinished bool
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

	callback(true)
}

//WriteBulk :- func to write bulk amount of data to kafka
func (c *Config) WriteBulk(message string, callback func(bool)) {

	var msgArray []kafka.Message

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:    []string{c.URL},
		Topic:      c.Topic,
		Balancer:   &kafka.LeastBytes{},
		BatchBytes: 99999999,
	})

	scanner := bufio.NewScanner(strings.NewReader(message))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024*15)

	for scanner.Scan() {
		msg := scanner.Text()
		kafkaMsg := kafka.Message{Value: []byte(msg)}
		msgArray = append(msgArray, kafkaMsg)
	}

	buf = nil
	var err error
	if len(msgArray) > 0 {
		err = w.WriteMessages(context.Background(),
			msgArray...,
		)
	} else {

		file, _ := os.OpenFile("kafka"+time.Now().String()+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		file.WriteString(message)
		file.Close()

		err = w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte(message)},
		)
	}

	if err != nil {
		fmt.Println("kafka write error", err)
		callback(false)
		return
	}

	fmt.Println("kafka6t")
	callback(true)
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
		// MaxWait:        0,
		// ReadBackoffMin: 0,
		// ReadBackoffMax: 0,
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

//ReaderWithLimit :-  read data with msg limit
func (c *Config) ReaderWithLimit(limit int, readMessageCallback func(reader *kafka.Reader, m kafka.Message)) {
	fmt.Println("reading............")
	c.ReadWithLimitFinished = false
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{c.URL},
		Topic:     c.Topic,
		Partition: c.Partition,
		MinBytes:  c.MinBytes,
		MaxBytes:  c.MaxBytes,
		GroupID:   c.GroupID,
	})

	ctx := context.Background()
	for i := 0; i <= limit; i++ {
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", i)
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		readMessageCallback(r, m)
	}
	defer func() {
		fmt.Println("reading  Finished............")
		c.ReadWithLimitFinished = true
	}()

}
