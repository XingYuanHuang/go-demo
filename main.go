package producer

import (
	"os/signal"

	"runtime"
	"syscall"
)

func main() {

	// 初始化生产生
	err := producer.InitProducer("192.168.1.29:9092")
	if err != nil {
		panic(err)
	}

	// 关闭
	defer kafka.Close()

	// 发送测试消息
	kafka.Send("Test", "This is Test Msg")
	kafka.Send("Test", "Hello Guoke")

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}
