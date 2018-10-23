// 日志消费转储端.
// 1. 定时从kafka获取完整应用日志topic列表；
// 2. 每一个topic通过不同的协程异步消费处理；
// 3. 将kafka对应topic中的消息转储到固定的日志文件中；
// 4. 定时将30天之前的数据进行压缩归档并删除(原始日志文件保留30天)；

package main

import (
    "bytes"
    "fmt"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gregex"
    "os"
    "os/exec"
    "time"
)

const (
    LOG_PATH                    = "/var/log/medlinker"  // 日志目录
    TOPIC_AUTO_CHECK_INTERVAL   = 5                     // (秒)kafka topic检测时间间隔
    ARCHIVE_AUTO_CHECK_INTERVAL = 3600                  // (秒)自动压缩归档检测时间间隔
    KAFKA_MSG_HANDLER_NUM       = "100"                 // 并发的kafka消息消费goroutine数量
    KAFKA_MSG_SAVE_INTERVAL     = "5"                   // (秒) kafka消息内容批量保存间隔
    KAFKA_OFFSETS_DIR_NAME      = "__backupper_offsets" // 用于保存应用端offsets的目录名称
    DEBUG                       = "true"                // 默认值，是否打开调试信息
)

// kafka消息数据结构
type Message struct {
    Path string   `json:"path"` // 日志文件路径
    Msgs []string `json:"msgs"` // 日志内容(多条)
    Time string   `json:"time"` // 发送时间(客户端搜集时间)
    Host string   `json:"host"` // 节点主机名称
}

var (
    logPath        = genv.Get("LOG_PATH", LOG_PATH)
    // 用于限制kafka消费异步gorutine数量
    handlerChan    = make(chan struct{}, handlerSize)
    bufferMap      = gmap.NewStringInterfaceMap()
    topicMap       = gmap.NewStringInterfaceMap()
    debug          = gconv.Bool(genv.Get("DEBUG", DEBUG))
    handlerSize    = gconv.Int(genv.Get("HANDLER_SIZE", KAFKA_MSG_HANDLER_NUM))
    saveInterval   = gconv.Int(genv.Get("SAVE_INTERVAL", KAFKA_MSG_SAVE_INTERVAL))
    kafkaAddr      = genv.Get("KAFKA_ADDR")
)

func main() {
    if kafkaAddr == "" {
       panic("Incomplete Kafka setting")
    }

    // 是否显示调试信息
    glog.SetDebug(debug)

    // 定时压缩归档任务
    gcron.Add("0 0 3 * * *", handlerArchiveCron)

    go handlerKafkaMessageContent()

    kafkaClient := newKafkaClient()
    defer kafkaClient.Close()
    for {
       if topics, err := kafkaClient.Topics(); err == nil {
           for _, topic := range topics {
               // 只处理新版处理程序处理 *.v2 的topic
               if !topicMap.Contains(topic) && gregex.IsMatchString(`.+\.v2`, topic) {
                   glog.Debugfln("add new topic handle: %s", topic)
                   topicMap.Set(topic, gmap.NewStringIntMap())
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
func handlerArchiveCron() {
    paths, _ := gfile.ScanDir(logPath, "*.log", true)
    for _, path := range paths {
        // 日志文件超过30天，那么执行归档
        if gtime.Second() - gfile.MTime(path) < 30*86400 {
            continue
        }
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
                handlerKafkaMessage(msg)
                // 处理完成再记录到自定义的offset表中(不管是否缓冲处理)
                offsetMap.Set(key, msg.Offset)
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

// 初始化topic offset
func initOffsetMap(topic string, offsetMap *gmap.StringIntMap) {
    for i := 0; i < 100; i++ {
        key  := fmt.Sprintf("%s.%d", topic, i)
        path := fmt.Sprintf("%s/%s/%s", logPath, KAFKA_OFFSETS_DIR_NAME, key)
        if !gfile.Exists(path) {
            break
        }
        offsetMap.Set(key, gconv.Int(gfile.GetContents(path)))
    }
}

// 应用自定义保存当前kafka读取的offset
func dumpOffsetMap(offsetMap *gmap.StringIntMap) {
    offsetMap.RLockFunc(func(m map[string]int) {
        for k, v := range m {
            if v == 0 {
                continue
            }
            path    := fmt.Sprintf("%s/%s/%s", logPath, KAFKA_OFFSETS_DIR_NAME, k)
            content := gconv.String(v)
            gfile.PutContents(path, content)
        }
    })
}

// 异步批量保存kafka日志
func handlerKafkaMessageContent() {
    for {
        time.Sleep(time.Duration(saveInterval)*time.Second)

        // 批量写日志
        for _, path := range bufferMap.Keys() {
            gmlock.Lock(path)
            buffer  := bufferMap.Get(path).(*bytes.Buffer)
            content := buffer.Bytes()
            if len(content) > 0 {
                if err := gfile.PutBinContentsAppend(path, content); err != nil {
                    // 如果日志写失败，等待1秒后继续
                    glog.Error(err)
                    time.Sleep(time.Second)
                } else {
                    glog.Debugfln("bytes written %d \t %s", len(content), path)
                    buffer.Reset()
                }
            }
            gmlock.Unlock(path)
        }

        // 导出topic offset到磁盘保存
        topicMap.RLockFunc(func(m map[string]interface{}) {
            for _, v := range m {
                go dumpOffsetMap(v.(*gmap.StringIntMap))
            }
        })
    }
}

// 创建kafka客户端
func newKafkaClient(topic ... string) *gkafka.Client {
    kafkaConfig               := gkafka.NewConfig()
    kafkaConfig.Servers        = kafkaAddr
    kafkaConfig.AutoMarkOffset = false
    kafkaConfig.GroupId        = "group_log_archiver"
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
            buffer.WriteString(fmt.Sprintf(`%s [%s]%s`, v[0 : len(v) - 1], msg.Host, "\n"))
        }
        gmlock.Unlock(msg.Path)
    } else {
        glog.Error(err)
    }
    return nil
}
