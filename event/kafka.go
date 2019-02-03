package event

import (
	"context"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafka.Reader
	conf   optionalKafkaConf
}

type optionalKafkaConf struct {
	groupID  string
	minBytes int
	maxBytes int
}

func NewKafkaReader(topic string, brokers []string, options ...KafkaReaderOption) KafkaReader {
	kr := &KafkaReader{}
	for _, option := range options {
		option(kr)
	}
	config := kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  kr.conf.groupID,
		MinBytes: kr.conf.minBytes,
		MaxBytes: kr.conf.maxBytes,
	}
	return KafkaReader{
		reader: kafka.NewReader(config),
	}
}

type KafkaReaderOption func(r *KafkaReader)

func WithConsumerGroup(consumerGroup string) KafkaReaderOption {
	return func(r *KafkaReader) {
		r.conf.groupID = consumerGroup
	}
}

func WithMinBytes(min int) KafkaReaderOption {
	return func(r *KafkaReader) {
		r.conf.minBytes = min
	}
}

func WithMaxBytes(max int) KafkaReaderOption {
	return func(r *KafkaReader) {
		r.conf.maxBytes = max
	}
}

func (kr *KafkaReader) ReadEvent(ctx context.Context) (event Event, err error) {
	msg, err := kr.reader.ReadMessage(ctx)
	if err != nil {
		return event, errors.Wrap(err, "Could not read event: ")
	}

	return Event{Key: msg.Key, Value: msg.Value}, nil
}

func (kr *KafkaReader) Close() error {
	err := kr.reader.Close()

	if err != nil {
		return errors.Wrap(err, "Could not properly close KafkaReader: ")
	}
	return nil
}
