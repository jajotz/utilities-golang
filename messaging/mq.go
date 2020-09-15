package messaging

import (
	"context"
	"utilities-golang/util"
)

type CallbackFunc func([]byte) error

type Queue interface {
	util.Ping
	ReadWithContext(context.Context, string, []CallbackFunc) error
	Read(string, []CallbackFunc) error
	PublishWithContext(context.Context, string, string) error
	Publish(string, string) error
	Close() error
}

type QueueV2 interface {
	AddTopicListener(string, CallbackFunc)
	Listen()
	Close() error
	Publish(string, string) error
}
