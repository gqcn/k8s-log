package main

import (
    "bytes"
    "errors"
    "fmt"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gregex"
    "time"
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
    if dryrun {
        kafkaConfig.GroupId = KAFKA_GROUP_NAME_DRYRUN
    }
    if len(topic) > 0 {
        kafkaConfig.Topics = topic[0]
    }
    return gkafka.NewClient(kafkaConfig)
}


// 异步处理topic日志
func handlerKafkaTopic(topic string) {
    kafkaClient := newKafkaClient(topic)
    defer func() {
        kafkaClient.Close()
        topicMap.Remove(topic)
    }()
    // 初始化topic offset
    offsetMap := topicMap.Get(topic).(*gmap.StringIntMap)
    initOffsetMap(topic, offsetMap)
    // 标记kafka指定topic partition的offset
    offsetMap.RLockFunc(func(m map[string]int) {
        for k, v := range m {
            if v > 0 {
                if match, _ := gregex.MatchString(`(.+)\.(\d+)`, k); len(match) == 3 {
                    // 从下一条读取，这里的offset+1
                    glog.Debugfln("mark kafka offset - topic: %s, partition: %s, offset: %d", topic, match[2], v + 1)
                    kafkaClient.MarkOffset(topic, gconv.Int(match[2]), v + 1)
                }
            }
        }
    })
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
            // 记录offset
            key := fmt.Sprintf("%s.%d", topic, msg.Partition)
            if msg.Offset <= offsetMap.Get(key) {
                msg.MarkOffset()
                continue
            }
            handlerChan <- struct{}{}
            go func() {
                if handlerKafkaMessage(msg) == nil {
                    offsetMap.Set(key, msg.Offset)
                }
                <- handlerChan
            }()
        } else {
            glog.Error(err)
            // 如果发生错误，那么退出，
            // 下一次会重新建立连接
            break
        }
    }
}

// 处理kafka消息(使用自定义的数据结构)
func handlerKafkaMessage(kafkaMsg *gkafka.Message) (err error) {
    defer func() {
        if err == nil {
            kafkaMsg.MarkOffset()
        }
    }()
    pkg := &Package{}
    if err = gjson.DecodeTo(kafkaMsg.Value, pkg); err == nil {
        msgBuffer := bytes.NewBuffer(nil)
        if pkg.Total > 1 {
            if pkg.Seq < pkg.Total {
                key := fmt.Sprintf("%d-%d", pkg.Id, pkg.Seq)
                if pkgCache.Contains(key) {
                    glog.Debugfln("pkg already received: %d, seq: %d, total: %d", pkg.Id, pkg.Seq, pkg.Total)
                } else {
                    pkgCache.Set(key, pkg.Msg, 60000)
                }
                return nil
            } else {
                start := gtime.Second()
                for {
                    if gtime.Second() - start > 60 {
                        return errors.New(fmt.Sprintf("incomplete package found: %d", pkg.Id))
                    }
                    // 当重新进入循环后需要清除之前的msgBuffer
                    msgBuffer.Reset()
                    for i := 1; i <= pkg.Total; i++ {
                        // 最后一条消息没有写缓存，这里直接write
                        if i == pkg.Total {
                            msgBuffer.Write(pkg.Msg)
                            goto MsgHandle
                        } else if v := pkgCache.Get(fmt.Sprintf("%d-%d", pkg.Id, i)); v != nil {
                            msgBuffer.Write(v.([]byte))
                        } else {
                            //glog.Debugfln("waiting for pkg to complete: %d, seq: %d, total: %d", pkg.Id, pkg.Seq, pkg.Total)
                            break
                        }
                    }
                    // 如果包不完整，等待1秒后重试，最长等待1分钟
                    time.Sleep(time.Second)
                }
            }
        } else if pkg.Total == 1 {
            msgBuffer.Write(pkg.Msg)
        } else {
            return errors.New(fmt.Sprintf("invalid package: %s", string(kafkaMsg.Value)))
        }

    MsgHandle:
        msg := &Message{}
        if err = gjson.DecodeTo(msgBuffer.Bytes(), msg); err != nil {
            glog.Println(pkg.Id, ":", msgBuffer.String())
            for i := 1; i < pkg.Total; i++ {
                glog.Println(pkg.Id, i, ":", string(pkgCache.Get(fmt.Sprintf("%d-%d", pkg.Id, i)).([]byte)))
            }
            glog.Println(pkg.Id, pkg.Seq, ":", string(pkg.Msg))
            glog.Error(err)
        }
        addToBufferArray(msg)
        // 使用完毕后清理分包缓存，防止内存占用
        for i := 1; i < pkg.Total; i++ {
            pkgCache.Remove(fmt.Sprintf("%d-%d", pkg.Id, i))
        }
    } else {
        glog.Error(err)
    }
    return
}

