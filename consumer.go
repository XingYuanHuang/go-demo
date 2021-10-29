package main

import (
	"github.com/XingYuanHuang/go-demo/kafka"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {

	// 初始化消费者
	err := kafka.InitConsumer("47.110.12.65:9092")
	if err != nil {
		panic(err)
	}
	// 监听
	go func() {
		err = kafka.LoopConsumer("Test", kafka.TopicCallBack)
		if err != nil {
			panic(err)
		}
	}()

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}
