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
    "time"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/genv"
    "os/exec"
    "os"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/container/gset"
    "gitee.com/johng/gf/g/util/gregex"
    "gitee.com/johng/gf/g/container/gmap"
    "fmt"
)

const (
    LOG_PATH                    = "/var/log/medlinker" // 日志目录
    TOPIC_AUTO_CHECK_INTERVAL   = 5                    // (秒)kafka topic检测时间间隔
    ARCHIVE_AUTO_CHECK_INTERVAL = 3600                 // (秒)自动压缩归档检测时间间隔
    KAFKA_MSG_HANDLER_NUM       = 100                  // 并发的kafka消息消费goroutine数量
)

var (
    debug     bool
    kafkaAddr string
    topicSet    = gset.NewStringSet()
    pathQueues  = gmap.NewStringInterfaceMap()
    handlerSize = gconv.Int(gcmd.Option.Get("handler-size"))
    handlerChan  chan struct{}
)

func main() {
    // 通过启动参数传参
    debug     = gconv.Bool(gcmd.Option.Get("debug"))
    kafkaAddr = gcmd.Option.Get("kafka-addr")
    // 通过环境变量传参
    if kafkaAddr == "" {
        debug       = gconv.Bool(genv.Get("DEBUG"))
        kafkaAddr   = genv.Get("KAFKA_ADDR")
        handlerSize = gconv.Int(genv.Get("HANDLER_SIZE"))
    }
    if kafkaAddr == "" {
        panic("Incomplete Kafka setting")
    }
    if handlerSize == 0 {
        handlerSize = KAFKA_MSG_HANDLER_NUM
    }
    // 用于限制kafka消费异步gorutine数量
    handlerChan = make(chan struct{}, handlerSize)

    glog.SetDebug(debug)

    go handlerArchiveLoop()

    kafkaClient := newKafkaClient()
    defer kafkaClient.Close()
    for {
        if topics, err := kafkaClient.Topics(); err == nil {
            for _, topic := range topics {
                if !topicSet.Contains(topic) {
                    glog.Debugfln("add new topic handle: %s", topic)
                    topicSet.Add(topic)
                    go handlerKafkaTopic(topic)
                }
            }
        } else {
            glog.Error(err)
            break
        }
        time.Sleep(TOPIC_AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 自动归档检查循环，归档使用tar工具实现
func handlerArchiveLoop() {
    for {
        paths, _ := gfile.ScanDir(LOG_PATH, "*.log", true)
        for _, path := range paths {
            // 日志文件超过30天，那么执行归档
            ctime := gtime.Second()
            mtime := gfile.MTime(path)
            if ctime - mtime > 30*86400 {
                archivePath := path + ".tar.bz2"
                existIndex  := 1
                for gfile.Exists(archivePath) {
                    archivePath = fmt.Sprintf("%s.%d.tar.bz2", path, existIndex)
                    existIndex++
                }

                // 进入日志目录
                if err := os.Chdir(gfile.Dir(path)); err != nil {
                    glog.Error(err)
                    continue
                }
                // 执行日志文件归档
                cmd := exec.Command("tar", "-jvcf",  archivePath, gfile.Basename(path))
                glog.Debugfln("tar -jvcf %s %s", archivePath, gfile.Basename(path))
                if err := cmd.Run(); err == nil {
                    if err := gfile.Remove(path); err != nil {
                        glog.Error(err)
                    }
                } else {
                    glog.Error(err)
                }
            }
        }

        time.Sleep(ARCHIVE_AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 异步处理topic日志
func handlerKafkaTopic(topic string) {
    kafkaClient := newKafkaClient(topic)
    defer func() {
        kafkaClient.Close()
        topicSet.Remove(topic)
    }()
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
            //glog.Debugfln("receive topic [%s] msg: %s", topic, string(msg.Value))
            handlerChan <- struct{}{}
            go func() {
                handlerKafkaMessage(msg)
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

// 创建kafka客户端
func newKafkaClient(topic ... string) *gkafka.Client {
    kafkaConfig               := gkafka.NewConfig()
    kafkaConfig.Servers        = kafkaAddr
    kafkaConfig.AutoMarkOffset = false
    if len(topic) > 0 {
        kafkaConfig.Topics  = topic[0]
        kafkaConfig.GroupId = "group_" + topic[0] + "_backupper"
    } else {
        kafkaConfig.GroupId = "group_default_backupper"
    }
    return gkafka.NewClient(kafkaConfig)
}

// 处理kafka消息
func handlerKafkaMessage(kafkaMsg *gkafka.Message) (err error) {
    defer func() {
        if err == nil {
            kafkaMsg.MarkOffset()
        }
    }()
    if j, err := gjson.DecodeToJson(kafkaMsg.Value); err == nil {
        content := j.GetString("message")
        msgTime := getTimeFromContent(content)
        if msgTime.IsZero() {
            msgTime = parserFileBeatTime(j.GetString("@timestamp"))
        }
        path := j.GetString("source")
        if err := gfile.PutContentsAppend(path, content + "\n"); err != nil {
            return err
        }
    }
    return nil
}

// 从内容中解析出日志的时间，并返回对应的日期对象
func getTimeFromContent(content string) time.Time {
    match, _ := gregex.MatchString(gtime.TIME_REAGEX_PATTERN, content)
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
