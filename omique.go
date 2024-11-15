package omique

import (
	"github.com/go-redis/redis/v8"
)

func NewClient(opts *redis.Options) *Client {
	return &Client{opts: opts}
}
