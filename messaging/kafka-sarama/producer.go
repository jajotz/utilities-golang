package kafka_sarama

import (
	"github.com/pkg/errors"
	"time"

	"github.com/Shopify/sarama"
)

func (l *Kafka) Publish(topic, msg string) error {
	producer, err := sarama.NewAsyncProducerFromClient(l.Client)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = producer.Close() }()

	producer.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Key:       nil,
		Value:     sarama.StringEncoder(msg),
		Timestamp: time.Now(),
	}
	return nil
}
