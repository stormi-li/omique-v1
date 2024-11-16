package omique

import (
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/stormi-li/omiserd-v1"
)

type Client struct {
	opts *redis.Options
}

func (c *Client) NewConsumer(channel, address string) *Consumer {
	return &Consumer{
		configRegister: omiserd.NewClient(c.opts, omiserd.Config).NewRegister(channel, address),
		channel:        channel,
		address:        address,
	}
}

func (c *Client) NewProducer(channel string) *Producer {
	producer := Producer{
		configDiscover: omiserd.NewClient(c.opts, omiserd.Config).NewDiscover(),
		channel:        channel,
		rlock:          sync.RWMutex{},
	}
	return &producer
}
