package kafka

import (
	"context"
	"sync"
	"time"
	"utilities-golang/logs"
	"utilities-golang/messaging"

	"github.com/pkg/errors"
	kfk "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/snappy"
	_ "github.com/segmentio/kafka-go/snappy"
)

type (
	Compression string
	kafka       struct {
		option  Option
		log     logs.Logger
		writers map[string]*kfk.Writer
		readers map[string]*kfk.Reader
		mu      sync.Mutex
	}
)

const (
	Snappy = "snappy"
	Gzip   = "gzip"
)

type Option struct {
	Host              []string
	ConsumerGroup     string
	Interval          int
	RequiredAck       int
	QueueCapacity     int
	MinBytes          int
	MaxBytes          int
	HeartbeatInterval time.Duration
	ReadBackoffMin    time.Duration
	ReadBackoffMax    time.Duration
	CommitInterval    time.Duration
	CompressionCodec  Compression
}

func getOption(option *Option) error {
	if len(option.Host) == 0 {
		return errors.New("Host is required!")
	}
	if option.ConsumerGroup == "" {
		return errors.New("ConsumerGroup is required!")
	}
	if option.Interval == 0 {
		option.Interval = 1
	}
	if option.RequiredAck == 0 {
		option.RequiredAck = -1
	}
	if option.QueueCapacity == 0 {
		option.QueueCapacity = 100
	}
	if option.HeartbeatInterval == 0 {
		option.HeartbeatInterval = 3 * time.Second
	}
	if option.ReadBackoffMin == 0 {
		option.ReadBackoffMin = 100 * time.Millisecond
	}
	if option.ReadBackoffMax == 0 {
		option.ReadBackoffMax = 1 * time.Second
	}

	if option.CompressionCodec == "" {
		option.CompressionCodec = Snappy
	}
	if option.CompressionCodec != Snappy && option.CompressionCodec != Gzip {
		return errors.New("Error compression codec type")
	}
	return nil
}

func New(option Option, log logs.Logger) (messaging.Queue, error) {
	err := getOption(&option)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to Initialize Kafka")
	}

	return &kafka{
		option:  option,
		log:     log,
		writers: make(map[string]*kfk.Writer),
		readers: make(map[string]*kfk.Reader),
		mu:      sync.Mutex{},
	}, nil
}

func (k *kafka) Ping() error {
	return nil
}

func (k *kafka) ReadWithContext(ctx context.Context, topic string, callbacks []messaging.CallbackFunc) error {
	if len(callbacks) < 1 {
		return errors.New("At least 1 callbacks is required")
	}

	k.mu.Lock()
	if _, ok := k.readers[topic]; !ok {
		reader := kfk.NewReader(kfk.ReaderConfig{
			Brokers:           k.option.Host,
			GroupID:           k.option.ConsumerGroup,
			Topic:             topic,
			MaxWait:           time.Duration(k.option.Interval) * time.Millisecond,
			QueueCapacity:     k.option.QueueCapacity,
			HeartbeatInterval: k.option.HeartbeatInterval,
			ReadBackoffMin:    k.option.ReadBackoffMin,
			ReadBackoffMax:    k.option.ReadBackoffMax,
			CommitInterval:    k.option.CommitInterval,
			MinBytes:          k.option.MinBytes,
			MaxBytes:          k.option.MaxBytes,
		})
		k.readers[topic] = reader
	}
	k.mu.Unlock()

	reader := k.readers[topic]

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			k.log.Error(err)
			continue
		}

		for _, c := range callbacks {
			if err = c(m.Value); err != nil {
				k.log.Error(err)
			}
		}
	}
}

func (k *kafka) Read(topic string, callbacks []messaging.CallbackFunc) error {
	return k.ReadWithContext(context.Background(), topic, callbacks)
}

func (k *kafka) PublishWithContext(ctx context.Context, topic, message string) error {
	k.mu.Lock()

	var compressionCodec kfk.CompressionCodec
	if k.option.CompressionCodec == Snappy {
		compressionCodec = snappy.NewCompressionCodec()
	} else if k.option.CompressionCodec == Gzip {
		compressionCodec = gzip.NewCompressionCodec()
	} else {
		k.mu.Unlock()
		return errors.New("error compression codec")
	}

	if _, ok := k.writers[topic]; !ok {
		writer := kfk.NewWriter(kfk.WriterConfig{
			Brokers:          k.option.Host,
			Topic:            topic,
			Balancer:         &kfk.Hash{},
			RequiredAcks:     k.option.RequiredAck,
			BatchTimeout:     time.Duration(k.option.Interval) * time.Millisecond,
			CompressionCodec: compressionCodec,
		})
		k.writers[topic] = writer
	}
	k.mu.Unlock()

	w := k.writers[topic]
	if err := w.WriteMessages(context.Background(), kfk.Message{Value: []byte(message)}); err != nil {
		k.log.Error(err)
		return errors.Wrapf(err, "failed to publish message on topic %s", topic)
	}
	return nil
}

func (k *kafka) Publish(topic, message string) error {
	return k.PublishWithContext(context.Background(), topic, message)
}

func (k *kafka) Close() error {
	var err error
	// - close writer
	for _, w := range k.writers {
		if e := w.Close(); e != nil {
			err = e
			k.log.Error(err)
		}
	}

	// - close reader
	for _, r := range k.readers {
		if e := r.Close(); e != nil {
			err = e
			k.log.Error(err)
		}
	}

	return err
}
