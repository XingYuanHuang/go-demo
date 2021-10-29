package models

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type KafkaTest struct {
	Id         int64  `gorm:"column:id;primary_key;auto_increment"`
	Topic      string `gorm:"column:topic;type:varchar(255);size(64);not null"`
	Msg        string `gorm:"column:msg;type:tinytext(0);not null"`
	CreateTime string `gorm:"column:create_time;not null"`
}

func (t *KafkaTest) TableName() string {
	return "kafka_test"
}

var (
	addr = "root:huang123@tcp(47.110.12.65:3306)/hxy-test"
)
var Eloquent *gorm.DB

func init() {
	var err error
	//用户名:密码@tcp(数据库ip或域名:端口)/数据库名称?charset=数据库编码&parseTime=True&loc=Local
	dsn := addr + "?charset=utf8&parseTime=True&loc=Local"
	Eloquent, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Printf("models connect error %v", err)
	}
	if Eloquent.Error != nil {
		fmt.Printf("database error %v", Eloquent.Error)
	}

}
func ListTest(datas []KafkaTest, page int) ([]KafkaTest, int64, error) {
	var pageSize = 10
	db := Eloquent
	offset := (page - 1) * pageSize
	result := db.Order("id desc").Offset(offset).Limit(pageSize).Find(&datas)
	return datas, result.RowsAffected, result.Error
}
func CreateTest(data KafkaTest) (int64, error) {
	db := Eloquent
	result := db.Create(&data)
	return data.Id, result.Error
}

func FindTest(id int64) (KafkaTest, error) {
	var model KafkaTest
	db := Eloquent
	result := db.First(&model, id)
	return model, result.Error
}

func UpdateTest(data KafkaTest, id int64) (int64, error) {
	var model KafkaTest
	db := Eloquent
	row := db.First(&model, id)
	if row.Error == nil {
		result := db.Model(&model).Updates(&data)
		return model.Id, result.Error
	}
	return 0, row.Error
}

func DeleteTest(id int64) (int64, error) {
	var model KafkaTest
	db := Eloquent
	result := db.Delete(&model, id)
	return result.RowsAffected, result.Error
}
