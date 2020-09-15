package redis_cluster

import (
	"encoding"
	"fmt"
	"time"
	"utilities-golang/cache"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type (
	Option struct {
		Address      []string
		Password     string
		PoolSize     int
		MinIdleConns int
		ReadOnly     bool
		DialTimeout  time.Duration
		PoolTimeout  time.Duration
		ReadTimeout  time.Duration
		WriteTimeout time.Duration
		MaxConnAge   time.Duration
	}

	redisClusterClient struct {
		r *redis.ClusterClient
	}
)

func New(option *Option) (cache.Cache, error) {
	var client *redis.ClusterClient

	client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        option.Address,
		Password:     option.Password,
		PoolSize:     option.PoolSize,
		PoolTimeout:  option.PoolTimeout,
		ReadTimeout:  option.ReadTimeout,
		WriteTimeout: option.WriteTimeout,
		DialTimeout:  option.DialTimeout,
		MinIdleConns: option.MinIdleConns,
		MaxConnAge:   option.MaxConnAge,
		ReadOnly:     option.ReadOnly,
	})

	if _, err := client.Ping().Result(); err != nil {
		return nil, errors.Wrap(err, "Failed to connect to redis!")
	}

	return &redisClusterClient{r: client}, nil
}

func (c *redisClusterClient) Ping() error {
	if _, err := c.r.Ping().Result(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *redisClusterClient) SetWithExpiration(key string, value interface{}, duration time.Duration) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.Set(key, value, duration).Result(); err != nil {
		return errors.Wrapf(err, "failed to set cache with key %s!", key)
	}

	return nil
}

func (c *redisClusterClient) Set(key string, value interface{}) error {
	if err := check(c); err != nil {
		return err
	}

	return c.SetWithExpiration(key, value, 0)
}

func (c *redisClusterClient) Get(key string, data interface{}) error {
	if _, ok := data.(encoding.BinaryUnmarshaler); !ok {
		return errors.New(fmt.Sprintf("failed to get cache with key %s!: redis: can't unmarshal (implement encoding.BinaryUnmarshaler)", key))
	}

	if err := check(c); err != nil {
		return err
	}

	val, err := c.r.Get(key).Result()

	if err == redis.Nil {
		return errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return errors.Wrapf(err, "failed to get key %s!", key)
	}

	if err := data.(encoding.BinaryUnmarshaler).UnmarshalBinary([]byte(val)); err != nil {
		return err
	}

	return nil
}

func (c *redisClusterClient) Keys(pattern string) ([]string, error) {
	if err := check(c); err != nil {
		return []string{}, err
	}

	return c.r.Keys(pattern).Result()
}

func (c *redisClusterClient) Remove(key string) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.Del(key).Result(); err != nil {
		return errors.Wrapf(err, "failed to remove key %s!", key)
	}

	return nil
}

func (c *redisClusterClient) RemoveByPattern(pattern string, countPerLoop int64) error {
	if err := check(c); err != nil {
		return err
	}

	iteration := 1
	for {
		keys, _, err := c.r.Scan(0, pattern, countPerLoop).Result()
		if err != nil {
			return errors.Wrapf(err, "failed to scan redis pattern %s!", pattern)
		}

		if len(keys) == 0 {
			break
		}

		if _, err := c.r.Del(keys...).Result(); err != nil {
			return errors.Wrapf(err, "failed iteration-%d to remove key with pattern %s", iteration, pattern)
		}

		iteration++
	}

	return nil
}

func (c *redisClusterClient) FlushDatabase() error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.FlushDB().Result(); err != nil {
		return errors.Wrap(err, "failed to flush db!")
	}

	return nil
}

func (c *redisClusterClient) FlushAll() error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.FlushAll().Result(); err != nil {
		return errors.Wrap(err, "failed to flush db!")
	}

	return nil
}

func (c *redisClusterClient) Close() error {
	if err := c.r.Close(); err != nil {
		return errors.Wrap(err, "failed to close redis client")
	}

	return nil
}

func check(c *redisClusterClient) error {
	if c.r == nil {
		return errors.New("redis client is not connected")
	}

	return nil
}

func (c *redisClusterClient) SetZSetWithExpiration(key string, duration time.Duration, data ...redis.Z) error {
	if err := c.SetZSet(key, data...); err != nil {
		return err
	}

	if _, err := c.r.Expire(key, duration).Result(); err != nil {
		c.r.Del(key)
		return errors.Wrapf(err, "failed to zadd cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) SetZSet(key string, data ...redis.Z) error {
	if err := check(c); err != nil {
		return err
	}

	c.r.Del(key)
	if _, err := c.r.ZAdd(key, data...).Result(); err != nil {
		return errors.Wrapf(err, "failed to zadd cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) GetZSet(key string) ([]redis.Z, error) {
	if err := check(c); err != nil {
		return nil, errors.WithStack(err)
	}

	data, err := c.r.ZRangeWithScores(key, 0, -1).Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to run zrange command")
	}

	if len(data) <= 0 {
		return nil, errors.New(fmt.Sprintf("key %s does not exits", key))
	}

	return data, nil
}

func (c *redisClusterClient) HMSetWithExpiration(key string, value map[string]interface{}, ttl time.Duration) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.HMSet(key, value).Result(); err != nil {
		return errors.Wrapf(err, "failed to HMSet cache with key %s!", key)
	}

	if _, err := c.r.Expire(key, ttl).Result(); err != nil {
		c.r.Del(key)
		return errors.Wrapf(err, "failed to HMSet cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) HMSet(key string, value map[string]interface{}) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.HMSet(key, value).Result(); err != nil {
		return errors.Wrapf(err, "failed to HMSet cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) HSetWithExpiration(key, field string, value interface{}, ttl time.Duration) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.HSet(key, field, value).Result(); err != nil {
		return errors.Wrapf(err, "failed to HSet cache with key %s!", key)
	}
	if _, err := c.r.Expire(key, ttl).Result(); err != nil {
		c.r.Del(key)
		return errors.Wrapf(err, "failed to HMSet cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) HSet(key, field string, value interface{}) error {
	if err := check(c); err != nil {
		return err
	}

	if _, err := c.r.HSet(key, field, value).Result(); err != nil {
		return errors.Wrapf(err, "failed to HSet cache with key %s!", key)
	}
	return nil
}

func (c *redisClusterClient) HMGet(key string, fields ...string) ([]interface{}, error) {
	if err := check(c); err != nil {
		return nil, err
	}

	val, err := c.r.HMGet(key, fields...).Result()
	if err == redis.Nil {
		return nil, errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get key %s!", key)
	}

	return val, nil
}

func (c *redisClusterClient) HGetAll(key string) (map[string]string, error) {
	if err := check(c); err != nil {
		return nil, err
	}

	val, err := c.r.HGetAll(key).Result()
	if err == redis.Nil {
		return nil, errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get key %s!", key)
	}

	return val, nil
}

func (c *redisClusterClient) HGet(key, field string, response interface{}) error {
	if _, ok := response.(encoding.BinaryUnmarshaler); !ok {
		return errors.New(fmt.Sprintf("failed to get cache with key %s!: redis: can't unmarshal (implement encoding.BinaryUnmarshaler)", key))
	}

	if err := check(c); err != nil {
		return err
	}

	val, err := c.r.HGet(key, field).Result()
	if err == redis.Nil {
		return errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return errors.Wrapf(err, "failed to get key %s!", key)
	}

	if err := response.(encoding.BinaryUnmarshaler).UnmarshalBinary([]byte(val)); err != nil {
		return err
	}

	return nil
}

func (c *redisClusterClient) MGet(key []string) ([]interface{}, error) {
	if err := check(c); err != nil {
		return nil, err
	}

	val, err := c.r.MGet(key...).Result()
	if err == redis.Nil {
		return nil, errors.Wrapf(err, "key %s does not exits", key)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get key %s!", key)
	}

	return val, nil
}

func (c *redisClusterClient) Client() cache.Cache {
	return c
}

func (c *redisClusterClient) Pipeline() cache.Pipe {
	return &pipe{instance: c.r.Pipeline()}
}
