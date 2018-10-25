package main

import (
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "time"
)

// 创建kafka生产客户端
func newKafkaClientProducer() *gkafka.Client {
    if kafkaAddr == "" || kafkaTopic == "" {
        panic("incomplete kafka settings")
    }
    kafkaConfig               := gkafka.NewConfig()
    kafkaConfig.Servers        = kafkaAddr
    kafkaConfig.Topics         = kafkaTopic
    return gkafka.NewClient(kafkaConfig)
}

// 向kafka发送日志内容(异步)
// 如果发送失败，那么每隔1秒阻塞重试
func sendToKafka(path string, msgs []string) {
    msg := Message{
        Path : path,
        Msgs : msgs,
        Time : gtime.Now().String(),
        Host : hostname,
    }
    for {
        if content, err := gjson.Encode(msg); err != nil {
            glog.Error(err)
        } else {
            glog.Debug("send to kafka:", kafkaTopic, path, len(content))
            if err := kafkaProducer.SyncSend(&gkafka.Message{Value : content}); err != nil {
                glog.Error(err)
            } else {
                break
            }
        }
        time.Sleep(time.Second)
    }
}
