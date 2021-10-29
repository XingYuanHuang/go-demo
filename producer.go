package main

import (
	"github.com/XingYuanHuang/go-demo/kafka"
	"math/rand"
	"time"
)

func main() {

	//每10秒发消息
	loopProducer()
	//signal.Ignore(syscall.SIGHUP)
	//runtime.Goexit()
}
func loopProducer() {
	// 初始化生产生
	err := kafka.InitProducer("47.110.12.65:9092")
	if err != nil {
		panic(err)
	}

	// 关闭
	defer kafka.Close()
	// 发送测试消息
	i := 0
	for {

		kafka.Send("Test", randSeq(6))
		time.Sleep(time.Second * 1)
		i++
	}
}

//生成随机数据
func randSeq(n int) string {
	//letters := []rune("abcdefghijklmnopqrstuvwxyz")
	letters := []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
