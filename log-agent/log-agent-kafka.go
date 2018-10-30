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
func sendToKafka(path string, msgs []string, offset int64) {
    defer offsetMapSave.Set(path, int(offset) + 1)
    msg := Message{
        Path : path,
        Msgs : msgs,
        Time : gtime.Now().String(),
        Host : hostname,
    }
    for {
        if msgBytes, err := gjson.Encode(msg); err != nil {
            glog.Error(err)
        } else {
            id    := gtime.Nanosecond()
            total := int(len(msgBytes)/sendMaxSize) + 1
            // 如果消息超过限制的大小，那么进行拆包
            for seq := 1; seq <= total; seq++ {
                pkg := Package {
                    Id    : id,
                    Seq   : seq,
                    Total : total,
                }
                pos := (seq - 1)*sendMaxSize
                if seq == total {
                    pkg.Msg = msgBytes[pos : ]
                } else {
                    pkg.Msg = msgBytes[pos : pos + sendMaxSize]
                }
                for {
                    if pkgBytes, err := gjson.Encode(pkg); err != nil {
                        glog.Error(err)
                    } else {
                        glog.Debug("send to kafka:", kafkaTopic, path, len(msgBytes), pos, pos + len(pkg.Msg))
                        if err := kafkaProducer.SyncSend(&gkafka.Message{Value : pkgBytes}); err != nil {
                            glog.Error(err)
                        } else {
                            break
                        }
                        time.Sleep(time.Second)
                    }
                }
            }
            break
        }
    }
}

