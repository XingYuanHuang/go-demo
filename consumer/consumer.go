package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
)

var consumer sarama.Consumer

// 消费者回调函数
type ConsumerCallback func(data []byte)

// 初始化消费者
func InitConsumer(hosts string) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		fmt.Printf("unable to create kafka client: %p \n", err)
	}

	consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println(err)
	}
}

// 消费者循环
func LoopConsumer(topic string, callback ConsumerCallback) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer partitionConsumer.Close()

	for {
		msg := <-partitionConsumer.Messages()
		if callback != nil {
			callback(msg.Value)
		}
	}
}

func Close() {
	if consumer != nil {
		err := consumer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}
}
