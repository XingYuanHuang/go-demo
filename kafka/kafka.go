package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/XingYuanHuang/go-demo/log"
	"strings"
)

var (
	producer sarama.AsyncProducer
	consumer sarama.Consumer
)

// 消费者回调函数
type ConsumerCallback func(data []byte)

// 初始化消费者
func InitConsumer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		log.Error("unable to create kafka client: %q", err.Error())
		return
	}

	consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Error("unable to create kafka client: %q", err.Error())
		return
	}

	return
}

// 消费者循环
func LoopConsumer(topic string, callback ConsumerCallback) (err error) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Error("error:", err.Error())
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

// 初始化生产者
func InitProducer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		log.Error("unable to create kafka client: %q", err.Error())
		return
	}
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		log.Error("error: %q", err.Error())
		return
	}

	return
}

// 发送消息
func Send(topic, data string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(data)}
	log.Trace("kafka", "Produced message: ["+data+"]")
}

// 关闭
func Close() {
	log.Trace("kafka", "Close")
	if producer != nil {
		err := producer.Close()
		if err != nil {
			log.Error("producer close error: %q", err.Error())
		}
	}

	if consumer != nil {
		err := consumer.Close()
		if err != nil {
			log.Error("consumer close error: %q", err.Error())
		}
	}
}
