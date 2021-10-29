package main

import (
	"fmt"
	controllers "github.com/XingYuanHuang/go-demo/controller"
	"github.com/XingYuanHuang/go-demo/kafka"
	"github.com/gin-gonic/gin"
	"net/http"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func main() {
	go runConsumer()
	go runProducer()
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "hello word")
	})
	router.GET("/test/list", controllers.ListTest)
	router.POST("/test/sendKafka", controllers.SendTest)
	router.POST("/test/update", controllers.UpdateTest)
	router.POST("/test/delete", controllers.DeleteTest)
	err := router.Run(":8888")
	if err != nil {
		fmt.Println("启动失败，error：", err.Error())
	}
}

// 启动消费者
func runConsumer() {

	// 初始化消费者
	err := kafka.InitConsumer("47.110.12.65:9092")
	if err != nil {
		panic(err)
	}
	fmt.Println("InitConsumer")
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

//启动循环定时生产者
func runProducer() {

	//每10秒发消息
	err := kafka.InitProducer("47.110.12.65:9092")
	if err != nil {
		panic(err)
	}

	// 关闭
	defer kafka.Close()
	// 发送测试消息
	for {

		kafka.Send("Test", kafka.RandSeq(6))
		time.Sleep(time.Second * 10)
	}
	//signal.Ignore(syscall.SIGHUP)
	//runtime.Goexit()
}
