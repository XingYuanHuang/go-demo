package controllers

import (
	"github.com/XingYuanHuang/go-demo/kafka"
	"github.com/XingYuanHuang/go-demo/models"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

// 定义接收数据的结构体
type TestData struct {
	// binding:"required"修饰的字段，若接收为空值，则报错，是必须字段
	Id    int64  `form:"id" json:"id" uri:"id" xml:"id"`
	Topic string `form:"topic" json:"topic" uri:"topic" xml:"topic"`
	Msg   string `form:"msg" json:"msg" uri:"msg" xml:"msg"`
}

func ListTest(c *gin.Context) {
	page, _ := strconv.Atoi(c.Query("page"))
	if page == 0 {
		page = 1
	}
	var list []models.KafkaTest
	res, rows, err := models.ListTest(list, page)
	if err != nil {
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "list": res, "pageTotal": rows})
	return
}

func SendTest(c *gin.Context) {
	err := kafka.InitProducer("47.110.12.65:9092")
	if err != nil {
		panic(err)
	}

	// 声明接收的变量
	var data TestData
	// 将request的body中的数据，自动按照json格式解析到结构体
	if err := c.ShouldBindJSON(&data); err != nil {
		// 返回错误信息
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}

	var model models.KafkaTest
	//todo 暂时定死
	model.Topic = "Test"
	model.Msg = data.Msg

	kafka.Send(model.Topic, model.Msg)

	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "发送成功"})
	return
}

func UpdateTest(c *gin.Context) {
	// 声明接收的变量
	var data TestData
	// 将request的body中的数据，自动按照json格式解析到结构体
	if err := c.ShouldBindJSON(&data); err != nil {
		// 返回错误信息
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	var model models.KafkaTest
	res, err := models.FindTest(data.Id)
	if err != nil {
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	model.Msg = data.Msg
	if _, err := models.UpdateTest(model, res.Id); err != nil {
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "更新成功"})
	return
}

func DeleteTest(c *gin.Context) {
	// 声明接收的变量
	var data TestData
	// 将request的body中的数据，自动按照json格式解析到结构体
	if err := c.ShouldBindJSON(&data); err != nil {
		// 返回错误信息
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	if _, err := models.DeleteTest(data.Id); err != nil {
		c.JSON(http.StatusMovedPermanently, gin.H{"code:": 1, "msg: ": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "删除成功"})
	return
}
