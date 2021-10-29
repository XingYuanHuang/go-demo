package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

var (
	producer   sarama.AsyncProducer
	consumer   sarama.Consumer
	timeFormat = "2006-01-02 15:04:05 "
)

// ConsumerCallback 消费者回调函数
type ConsumerCallback func(topic string, data []byte)

// InitConsumer 初始化消费者
func InitConsumer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		fmt.Println(time.Now().Format(timeFormat)+"unable to create kafka client:", err.Error())
		return
	}

	consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println(time.Now().Format(timeFormat) + err.Error())
		return
	}

	return
}

// LoopConsumer 消费者循环
func LoopConsumer(topic string, callback ConsumerCallback) (err error) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println(time.Now().Format(timeFormat) + err.Error())
		return
	}
	defer func(partitionConsumer sarama.PartitionConsumer) {
		err := partitionConsumer.Close()
		if err != nil {
			fmt.Println(time.Now().Format(timeFormat) + err.Error())
		}
	}(partitionConsumer)

	for {
		msg := <-partitionConsumer.Messages()
		if callback != nil {
			callback(topic, msg.Value)
		}
	}
}

// InitProducer 初始化生产者
func InitProducer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		fmt.Println(time.Now().Format(timeFormat)+"unable to create kafka client:", err.Error())
		return
	}
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Println(time.Now().Format(timeFormat) + err.Error())
		return
	}

	return
}

// Send 发送消息
func Send(topic, data string) {

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(data)}
	fmt.Println(time.Now().Format(timeFormat), "topic:"+topic+" Produced message: ["+data+"]")
}

// Close 关闭
func Close() {
	fmt.Println("kafka Close")
	if producer != nil {
		err := producer.Close()
		if err != nil {
			fmt.Println(time.Now().Format(timeFormat)+"producer close error: ", err.Error())
		}
	}

	if consumer != nil {
		err := consumer.Close()
		if err != nil {
			fmt.Println(time.Now().Format(timeFormat)+"consumer close error:", err.Error())
		}
	}
}
func TopicCallBack(topic string, data []byte) {
	fmt.Println(time.Now().Format(timeFormat)+"kafka", "topic:"+topic+" message:"+string(data))
}
