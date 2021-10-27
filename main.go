package producer

import (
	"github.com/XingYuanHuang/go-demo/producer"
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
	defer producer.Close()

	// 发送测试消息
	producer.Send("Test", "This is Test Msg")
	producer.Send("Test", "Hello Guoke")

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}
