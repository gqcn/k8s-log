// 日志备份工具.
// 1. 定时从kafka获取topic列表；
// 2. 每一个topic通过不同的协程异步消费处理；
// 3. 将kafka对应topic中的消息转储到固定的临时的日志文件中；
// 4. 定时将前一天的数据进行压缩归档并删除(暂时不做处理，后续可以依靠当前程序或者logrotate工具实现)；
// Usage:
// ./log-backupper --kafka-addr=127.0.0.1:9092

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
    "strings"
    "os/exec"
    "os"
)

const (
    TOPIC_AUTO_CHECK_INTERVAL   = 5      // (秒)kafka topic检测时间间隔
    ARCHIVE_AUTO_CHECK_INTERVAL = 86400  // (秒)自动压缩归档检测时间间隔
)
var (
    kafkaAddr   string
    kafkaTopic string
    topicSet   = gset.NewStringSet()
)

func main() {
    kafkaAddr = gcmd.Option.Get("KAFKA_ADDR")
    if kafkaAddr == "" {
        kafkaAddr = genv.Get("KAFKA_ADDR")
    }
    if kafkaAddr == "" {
        panic("Incomplete Kafka setting")
    }
    go handlerArchiveLoop()

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

// 自动归档检查循环，归档使用tar工具实现
func handlerArchiveLoop() {
    for {
        if array , err := gfile.Glob("/var/log/medlinker-backup/*/*.log"); err == nil && len(array) > 0 {
            for _, path := range array {
                if !strings.EqualFold(gfile.Basename(path), gtime.Format("2006-01-02.log")) {
                    archivePath := path + ".tar.bz2"
                    if gfile.Exists(archivePath) {
                        glog.Errorfln("archive for %s already exists", path)
                        continue
                    }
                    if err := os.Chdir(gfile.Dir(path)); err != nil {
                        glog.Error(err)
                        continue
                    }
                    cmd := exec.Command("tar", "-jvcf",  archivePath, gfile.Basename(path), "--remove-files")
                    if err := cmd.Run(); err != nil {
                        glog.Error(err)
                    }
                }
            }
        }
        time.Sleep(ARCHIVE_AUTO_CHECK_INTERVAL*time.Second)
    }
}


// 异步处理topic日志
func handlerKafkaTopic(topic string) {
    kafkaClient := newKafkaClient(topic)
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
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
    kafkaConfig.Servers = kafkaAddr
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
        // 规范：/var/log/medlinker-backup/{AppName/Topic}/{YYYY-MM-DD}.log
        path := fmt.Sprintf("/var/log/medlinker-backup/%s/%s.log", message.Topic, msgTime.Format("2006-01-02"))
        if err := gfile.PutContentsAppend(path, content + "\n"); err != nil {
            glog.Error(err)
        }
    }
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) time.Time {
    match, _ := gregx.MatchString(gtime.TIME_REAGEX_PATTERN, content)
    if len(match) >= 1 {
        if t, err := gtime.StrToTime(match[0]); err == nil {
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
