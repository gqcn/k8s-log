// 日志搜集客户端，
// 1. 日志搜集到kafka；
// 2. 超过指定大小并已完成搜集的日志进行truncate处理；
//
// Usage:
// log-collector --pattern=xxx,xxx --kafka.url=xxx --kafka.topic=xxx [--maxsize=1024]
//
// eg:
// log-collector --pattern=/var/log/*.log,/var/log/*/*.log --kafka-url=http://127.0.0.1:9092 --kafka-topic=ucenter
package main

import (
    "./collector"
    "gitee.com/johng/gf/g/os/gcmd"
    "gitee.com/johng/gf/g/util/gconv"
)

func main() {
    config := collector.Config{}
    config.KafkaUrl   = gcmd.Option.Get("kafka-url")
    config.KafkaTopic = gcmd.Option.Get("kafka-topic")
    config.Pattern    = gcmd.Option.Get("pattern")
    maxSize          := gcmd.Option.Get("maxsize")
    if maxSize != "" {
        config.MaxSize = gconv.Int(maxSize) * 1024 * 1024
    }
    if c, err := collector.New(config); err != nil {
        panic(err)
    } else {
        c.Run()
    }
}
