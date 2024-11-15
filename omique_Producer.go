package omique

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/stormi-li/omiserd-v1"
)

type Producer struct {
	configDiscover *omiserd.Discover
	channel        string
	address        string
	conn           net.Conn
	load           float64
	rlock          sync.RWMutex
	closed         bool
}

func (producer *Producer) Close() {
	producer.configDiscover.Close()
	producer.closed = true
}

func (producer *Producer) monitorLoad() {
	for !producer.closed {
		data := producer.configDiscover.GetData(producer.channel, producer.address)
		if data["load"] != "" {
			producer.load, _ = strconv.ParseFloat(data["load"], 64)
		}
		time.Sleep(1 * time.Second)
	}
}

func (producer *Producer) reConnect() error {
	address, _ := producer.configDiscover.DiscoverByWeight(producer.channel)
	if address == "" {
		producer.conn = nil
		return fmt.Errorf("no message queue service was found")
	}
	conn, err := net.Dial("tcp", address)
	if err == nil {
		producer.rlock.Lock()
		if producer.conn != nil {
			producer.conn.Close()
		}
		producer.conn = conn
		producer.rlock.Unlock()
		producer.address = address
		producer.load = 0
		return nil
	}
	return err
}

func (producer *Producer) Publish(message []byte) error {
	var err error
	retryCount := 0

	//长度前缀协议
	byteMessage := []byte(string(message))
	messageLength := uint32(len(byteMessage))

	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, messageLength)

	for {
		if producer.load > 0.9 {
			producer.reConnect()
			continue
		}
		producer.rlock.RLock()
		if producer.conn != nil {
			_, err = producer.conn.Write(append(lengthBuf, byteMessage...))
		} else {
			err = fmt.Errorf("no message queue service was found")
		}
		producer.rlock.RUnlock()
		if err == nil {
			break
		}
		newErr := producer.reConnect()
		if newErr != nil {
			err = newErr
		}
		time.Sleep(retry_wait_time)
		if retryCount == 10 {
			break
		}
		retryCount++
	}
	return err
}
