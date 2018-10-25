package main

import (
    "bytes"
    "fmt"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
    "strings"
)

// 创建kafka客户端
func newKafkaClient(topic ... string) *gkafka.Client {
    if kafkaAddr == "" {
        panic("Incomplete Kafka setting")
    }
    kafkaConfig               := gkafka.NewConfig()
    kafkaConfig.Servers        = kafkaAddr
    kafkaConfig.AutoMarkOffset = false
    kafkaConfig.GroupId        = KAFKA_GROUP_NAME
    if len(topic) > 0 {
        kafkaConfig.Topics = topic[0]
    }
    return gkafka.NewClient(kafkaConfig)
}

// 处理kafka消息(使用自定义的数据结构)
func handlerKafkaMessage(kafkaMsg *gkafka.Message) (err error) {
    defer func() {
        if err == nil {
            kafkaMsg.MarkOffset()
        }
    }()
    msg := &Message{}
    if err := gjson.DecodeTo(kafkaMsg.Value, msg); err == nil {
        buffer := bufferMap.GetOrSetFuncLock(msg.Path, func() interface{} {
            return bytes.NewBuffer(nil)
        }).(*bytes.Buffer)

        gmlock.Lock(msg.Path)
        for _, v := range msg.Msgs {
            buffer.WriteString(fmt.Sprintf("%s [%s]\n", strings.TrimRight(v, "\r\n"), msg.Host))
        }
        gmlock.Unlock(msg.Path)
    } else {
        glog.Error(err)
    }
    return nil
}
