package redis_cluster

import (
	"encoding"
	"fmt"
	gr "github.com/go-redis/redis"
	"github.com/pkg/errors"
	"time"
)

type (
	pipe struct {
		instance gr.Pipeliner
	}
)

func (p *pipe) Set(key string, value interface{}) error {
	return p.SetWithExpiration(key, value, 0)
}

func (p *pipe) SetWithExpiration(key string, value interface{}, expired time.Duration) error {
	return p.instance.Set(key, value, expired).Err()
}

func (p *pipe) Get(key string, object interface{}) error {
	if _, ok := object.(encoding.BinaryUnmarshaler); !ok {
		return errors.New(fmt.Sprintf("failed to get cache with key %s!: redis: can't unmarshal (implement encoding.BinaryUnmarshaler)", key))
	}

	val, err := p.instance.Get(key).Result()

	if err == gr.Nil {
		return errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return errors.Wrapf(err, "failed to get key %s", key)
	}

	if err := object.(encoding.BinaryUnmarshaler).UnmarshalBinary([]byte(val)); err != nil {
		return errors.Wrapf(err, "failed to unmarshal object")
	}

	return nil
}

func (p *pipe) Exec() error {
	_, err := p.instance.Exec()
	return errors.Wrapf(err, "failed to exec pipeline")
}
