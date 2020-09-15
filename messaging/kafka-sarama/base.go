package kafka_sarama

import (
	"sync"
	"time"

	"github.com/jajotz/utilities-golang/logs"
	"github.com/jajotz/utilities-golang/messaging"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

const (
	DefaultConsumerWorker       = 10
	DefaultStrategy             = cluster.StrategyRoundRobin
	DefaultHeartbeat            = 3
	DefaultProducerMaxBytes     = 1000000
	DefaultProducerRetryMax     = 3
	DefaultProducerRetryBackoff = 100
	DefaultMaxWait              = 10 * time.Second
)

type Kafka struct {
	Option            *Option
	Consumer          *cluster.Consumer
	CallbackFunctions map[string][]messaging.CallbackFunc
	Client            sarama.Client
	mu                *sync.Mutex
}

type Option struct {
	Host                 []string
	ConsumerWorker       int
	ConsumerGroup        string
	Strategy             cluster.Strategy
	Heartbeat            int
	ProducerMaxBytes     int
	ProducerRetryMax     int
	ProducerRetryBackOff int
	KafkaVersion         string
	ListTopics           []string
	MaxWait              time.Duration
	Log                  logs.Logger
}

func getOption(option *Option) error {
	if option.KafkaVersion == "" {
		return errors.New("invalid kafka version")
	}

	if option.Log == nil {
		logger, _ := logs.DefaultLog()
		option.Log = logger
	}

	if option.Strategy == "" {
		option.Strategy = DefaultStrategy
	}

	if option.Heartbeat == 0 {
		option.Heartbeat = DefaultHeartbeat
	}

	if option.ConsumerWorker == 0 {
		option.ConsumerWorker = DefaultConsumerWorker
	}

	if option.ProducerMaxBytes == 0 {
		option.ProducerMaxBytes = DefaultProducerMaxBytes
	}

	if option.ProducerRetryMax == 0 {
		option.ProducerRetryMax = DefaultProducerRetryMax
	}

	if option.ProducerRetryBackOff == 0 {
		option.ProducerRetryBackOff = DefaultProducerRetryBackoff
	}

	if option.MaxWait == 0 {
		option.MaxWait = DefaultMaxWait
	}
	return nil
}

func New(option *Option) (messaging.QueueV2, error) {
	var err error
	if err := getOption(option); err != nil {
		return nil, errors.WithStack(err)
	}

	l := Kafka{
		Option:            option,
		CallbackFunctions: make(map[string][]messaging.CallbackFunc),
		mu:                &sync.Mutex{},
	}

	l.Client, err = l.NewClient()
	if err != nil {
		return nil, err
	}

	return &l, nil
}

func (l *Kafka) NewListener(option *Option) (*cluster.Consumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = l.Option.MaxWait
	config.Group.Return.Notifications = true
	config.Group.PartitionStrategy = l.Option.Strategy
	config.Group.Heartbeat.Interval = time.Duration(l.Option.Heartbeat) * time.Second
	brokers := l.Option.Host
	return cluster.NewConsumer(brokers, l.Option.ConsumerGroup, l.Option.ListTopics, config)
}

func (l *Kafka) NewClient() (sarama.Client, error) {
	kfkVersion, err := sarama.ParseKafkaVersion(l.Option.KafkaVersion)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	configProducer := sarama.NewConfig()
	configProducer.Version = kfkVersion
	configProducer.Producer.Return.Errors = true
	configProducer.Producer.Return.Successes = true
	configProducer.Producer.MaxMessageBytes = l.Option.ProducerMaxBytes
	configProducer.Producer.Retry.Max = l.Option.ProducerRetryMax
	configProducer.Producer.Retry.Backoff = time.Duration(l.Option.ProducerRetryBackOff) * time.Millisecond
	return sarama.NewClient(l.Option.Host, configProducer)
}

func (l *Kafka) Close() error {
	if l.Consumer != nil {
		if err := l.Consumer.Close(); err != nil {
			return errors.Wrapf(err, "Failed to Close Consumer")
		}
	}

	if l.Client != nil {
		if err := l.Client.Close(); err != nil {
			return errors.Wrapf(err, "Failed to Close Producer")
		}
	}
	return nil
}
