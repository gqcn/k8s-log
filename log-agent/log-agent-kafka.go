package main

import (
    "github.com/gogf/gf/g/container/gmap"
    "github.com/gogf/gf/gkafka"
    "github.com/gogf/gf/g/encoding/gjson"
    "github.com/gogf/gf/g/os/glog"
    "github.com/gogf/gf/g/os/gtime"
    "github.com/gogf/gf/g/text/gregex"
    "time"
)

var (
    // kafka消息生产者map
    producers = gmap.NewStringInterfaceMap()
)

// 创建kafka生产客户端
func getKafkaClientProducer(topic string) *gkafka.Client {
    if kafkaAddr == "" {
        panic("incomplete kafka settings")
    }
    return producers.GetOrSetFuncLock(topic, func() interface{} {
        kafkaConfig               := gkafka.NewConfig()
        kafkaConfig.Servers        = kafkaAddr
        kafkaConfig.Topics         = topic
        return gkafka.NewClient(kafkaConfig)
    }).(*gkafka.Client)
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
    topic    := ""
    match, _ := gregex.MatchString(`.+kubernetes\.io~empty\-dir/log.*?/(.+?)/.+`, path)
    if len(match) > 1 {
        topic = match[1]
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
                        start := offsetMapSave.Get(path)
                        if start > int(offset) {
                            start = 0
                        }
                        glog.Debugfln("%s %s,\t%d to %d,\t%d[%d:%d]", topic, path, start, offset, len(msgBytes), pos, pos + len(pkg.Msg))
                        if err := getKafkaClientProducer(topic).SyncSend(&gkafka.Message{Value : pkgBytes}); err != nil {
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

