// 日志备份工具.
// 1. 定时从kafka获取topic列表；
// 2. 每一个topic通过不同的协程异步消费处理；
// 3. 将kafka对应topic中的消息转储到固定的临时的日志文件中；
// 4. 定时将前一天的数据进行压缩归档并删除(暂时不做处理，后续可以依靠当前程序或者logrotate工具实现)；
// Usage:
// ./log-backupper --kafka-url=127.0.0.1:9092

package main

import (
    "gitee.com/johng/gf/g/os/gcmd"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/container/gset"
    "time"
    "gitee.com/johng/gf/g/encoding/gjson"
    "fmt"
    "gitee.com/johng/gf/g/util/gregx"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/genv"
)

const (
    TOPIC_AUTO_CHECK_INTERVAL = 5 // (秒)kafka topic检测时间间隔
)
var (
    kafkaUrl   string
    kafkaTopic string
    topicSet   = gset.NewStringSet()
)

func main() {
    // input params check
    kafkaUrl = gcmd.Option.Get("kafka-url")
    if kafkaUrl == "" {
        kafkaUrl = genv.Get("kafka-url")
    }
    if kafkaUrl == "" {
        panic("Incomplete Kafka setting")
    }

    kafkaClient := newKafkaClient()
    for {
        if topics, err := kafkaClient.Topics(); err == nil {
            for _, topic := range topics {
                if !topicSet.Contains(topic) {
                    topicSet.Add(topic)
                    go handlerKafkaTopic(topic)
                }
            }
        } else {
            glog.Error(err)
        }
        time.Sleep(TOPIC_AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 异步处理topic日志
func handlerKafkaTopic(topic string) {
    kafkaClient := newKafkaClient(topic)
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
            fmt.Println(msg.Topic, string(msg.Value))
            // 不使用异步，顺序写入
            handlerKafkaMessage(msg)
        } else {
            glog.Error(err)
        }
    }
}

// 创建kafka客户端
func newKafkaClient(topic ... string) *gkafka.Client {
    kafkaConfig        := gkafka.NewConfig()
    kafkaConfig.Servers = kafkaUrl
    if len(topic) > 0 {
        kafkaConfig.Topics = topic[0]
    }

    kafkaConfig.GroupId = "group_" + kafkaTopic + "_backupper"
    return gkafka.NewClient(kafkaConfig)
}

// 处理kafka消息
func handlerKafkaMessage(message *gkafka.Message) {
    if j, err := gjson.DecodeToJson(message.Value); err == nil {
        j.SetViolenceCheck(false)
        content := j.GetString("message")
        msgTime := getTimeFromContent(content)
        if msgTime.IsZero() {
            msgTime = parserFileBeatTime(j.GetString("@timestamp"))
        }
        // 规范：/var/log/medlinker-backup/{AppName}/{YYYY-MM-DD}/{NodeName}_{PodName}.log
        path := fmt.Sprintf("/var/log/medlinker-backup/%s/%s/%s_%s.log",
            j.GetString("fields.appname"),
            msgTime.Format("2006-01-02"),
            j.GetString("fields.hostname"),
            j.GetString("fields.podname"),
        )
        if err := gfile.PutContentsAppend(path, content + "\n"); err != nil {
            glog.Error(err)
        }
    }
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) time.Time {
    match, _ := gregx.MatchString(`(\d{4}-\d{2}-\d{2})[\sT]{0,1}(\d{2}:\d{2}:\d{2}){0,1}\.{0,1}(\d{0,9})([\sZ]{0,1})([\+-]{0,1})([:\d]*)`, content)
    if len(match) >= 3 {
        str := match[1]
        if match[2] != "" {
            str += " " + match[2]
        }
        if t, err := gtime.StrToTime(str); err == nil {
            return t
        }
    }
    return time.Time{}
}

// 解析filebeat的ISODate时间为标准的日期时间对象
func parserFileBeatTime(datetime string) time.Time {
    if t, err := gtime.StrToTime(datetime); err == nil {
        t = t.Add(8 * time.Hour)
        return t
    }
    return time.Time{}
}
