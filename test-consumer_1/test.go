package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	omique "github.com/stormi-li/omique-v1"
)

func main() {
	consumer()
}

var redisAddr = "118.25.196.166:3934"
var password = "12982397StrongPassw0rd"

func consumer() {
	c := omique.NewClient(&redis.Options{Addr: redisAddr, Password: password})
	consumer := c.NewConsumer("consumer_1", "118.25.196.166:5555")
	consumer.ListenAndConsume(100, 1, func(message []byte) {
		fmt.Println(string(message))
		time.Sleep( 100* time.Millisecond)
	})
}
