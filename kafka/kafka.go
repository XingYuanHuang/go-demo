package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/XingYuanHuang/go-demo/models"
	"math/rand"
	"strings"
	"time"
)

var (
	producer   sarama.AsyncProducer
	consumer   sarama.Consumer
	timeFormat = "2006-01-02 15:04:05"
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
func TopicCallBack(topic string, data []byte) {
	timeNow := time.Now().Format(timeFormat)
	model := models.KafkaTest{
		Topic:      topic,
		Msg:        string(data),
		CreateTime: timeNow,
	}
	fmt.Println(model)
	if _, err := models.CreateTest(model); err != nil {
		fmt.Println(timeNow+" 添加失败", err.Error())
		return
	}
	fmt.Println(timeNow+" kafka", "Consumer "+"topic:"+topic+" message:"+string(data))
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
	fmt.Println(time.Now().Format(timeFormat), "Produced "+"topic:"+topic+" message: ["+data+"]")
}

// RandSeq 生成随机数据
func RandSeq(n int) string {
	//letters := []rune("abcdefghijklmnopqrstuvwxyz")
	letters := []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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
