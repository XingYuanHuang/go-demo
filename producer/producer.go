package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

var producer sarama.AsyncProducer

// InitProducer 初始化生产者
func InitProducer(hosts string) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		fmt.Println(err.Error())
	}
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Println(err.Error())
	}

}

// Send 发送消息
func Send(topic, data string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(data)}
	fmt.Println("consumer", "Produced message: ["+data+"]")
}

func Close() {
	if producer != nil {
		err := producer.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
