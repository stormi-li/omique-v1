package omique

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/stormi-li/omiserd-v1"
)

type Consumer struct {
	configRegister *omiserd.Register
	channel        string
	address        string
	weight         int
	handler        func(message []byte)
	messageChan    chan []byte
	capacity       int
}

func (consumer *Consumer) ListenAndConsume(capacity, weight int, handler func(message []byte)) {
	consumer.configRegister.SetRegisterHandler(func(register *omiserd.Register) {
		register.Data["capacity"] = strconv.Itoa(consumer.capacity)
		register.Data["message_num"] = strconv.Itoa(len(consumer.messageChan))
		register.Data["load"] = fmt.Sprintf("%.2f", float64(len(consumer.messageChan))*1.0/float64(consumer.capacity))
	})
	consumer.capacity = capacity
	consumer.handler = handler
	consumer.messageChan = make(chan []byte, capacity)
	consumer.weight = weight
	go consumer.configRegister.Register(weight)
	consumer.start()
}

func (consumer *Consumer) start() {
	if consumer.handler == nil {
		panic("未添加消息处理器")
	}
	go func() {
		listener, err := net.Listen("tcp", ":"+strings.Split(consumer.address, ":")[1])
		if err != nil {
			panic(err)
		}
		log.Println("omi consumer server: " + consumer.channel + " is running on " + consumer.address)
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go consumer.handleConnection(conn)
		}
	}()
	for {
		msg := <-consumer.messageChan
		consumer.handler(msg)
	}
}

func (consumer *Consumer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// 用于存放拼接的分块数据，支持连续读取消息
	tempBuffer := make([]byte, 0)
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}

		// 将读取的数据追加到缓存
		tempBuffer = append(tempBuffer, buffer[:n]...)

		// 循环解析缓存中的消息
		for {
			// 1. 检查是否有足够的字节来读取消息长度前缀（4字节）
			if len(tempBuffer) < 4 {
				break // 不足以读取长度前缀，等待更多数据
			}

			// 2. 读取消息长度前缀
			messageLength := binary.BigEndian.Uint32(tempBuffer[:4])
			totalLength := 4 + int(messageLength) // 总消息长度=长度前缀+消息体

			// 3. 检查缓存中是否有完整的消息
			if len(tempBuffer) < totalLength {
				break // 不足以读取完整消息体，等待更多数据
			}

			// 4. 提取完整的消息体
			messageBuf := tempBuffer[4:totalLength]

			// 5. 放入消息队列
			consumer.messageChan <- messageBuf

			// 6. 从缓存中移除已处理的消息
			tempBuffer = tempBuffer[totalLength:]
		}
	}
}
