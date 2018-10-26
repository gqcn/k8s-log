// 容器日志搜集客户端，参考方案说明：http://gitlab.dev.okapp.cc:81/foundations/inaction/blob/master/实施/1.2、日志搜集/日志搜集改进.MD
// 1、监控指定目录下的日志文件，记录文件偏移量变化；
// 2、搜集日志文件内容，并批量提交到Kafka；
// 3、按照既定规则清理日志文件；
// 4、每个小时定时执行清理；

package main

import (
    "bytes"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/container/gset"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gregex"
    "os"
    "time"
)

const (
    LOG_PATH          = "/var/log/medlinker"         // 默认值，日志目录绝对路径
    OFFSET_FILE_PATH  = "/var/log/log-agent.offsets" // 默认值，偏移量保存map，用于系统重启时恢复日志偏移量读取，继续文件的搜集位置
    SCAN_INTERVAL     = "10"                         // 默认值，(秒)目录检测间隔，检测新日志文件的创建
    CLEAN_BUFFER_TIME = "86400"                      // 默认值，(秒)当执行清理时，超过多少时间没有更新则执行删除(默认3小时)
    CLEAN_MIN_SIZE    = "1024"                       // 默认值，(byte)日志文件最小容量，超过该大小的日志文件才会执行清理(默认1KB)
    CLEAN_MAX_SIZE    = "1073741824"                 // 默认值，(byte)日志文件最大限制，当清理时执行规则处理；
    SEND_MAX_SIZE     = "10240"                      // 默认值，(byte)每条消息发送时的最大值(包大小限制, 默认10KB)
                                                     // 注意：通过性能测试，kafka在消息为10K时吞吐量达到最大，更大的消息会降低吞吐量，在设计集群的容量时，尤其要考虑这点
    DEBUG             = "true"                       // 默认值，是否打开调试信息
)

// kafka消息包
type Package struct {
    Id    int64  `json:"id"`     // 消息包ID，当被拆包时，多个分包的包id相同
    Seq   int    `json:"seq"`    // 序列号(当消息包被拆时用于整合打包)
    Total int    `json:"total"`  // 总分包数(当只有一个包时，sqp = total = 1)
    Msg   []byte `json:"msg"`    // 消息数据(二进制)
}

// kafka消息数据结构
type Message struct {
    Path string   `json:"path"` // 日志文件路径
    Msgs []string `json:"msgs"` // 日志内容(多条)
    Time string   `json:"time"` // 发送时间(客户端搜集时间)
    Host string   `json:"host"` // 节点主机名称
}

var (
    // 运行时记录日志文件搜集的offset
    offsetMapCache = gmap.NewStringIntMap(true)
    // 真实提交成功的offset，用于持久化保存
    offsetMapSave  = gmap.NewStringIntMap(true)
    handlerSet     = gset.NewStringSet(true)
    // 以下为通过环境变量传递的参数
    logPath        = genv.Get("LOG_PATH", LOG_PATH)
    offsetFilePath = genv.Get("OFFSET_FILE_PATH", OFFSET_FILE_PATH)
    scanInterval   = gconv.TimeDuration(genv.Get("SCAN_INTERVAL", SCAN_INTERVAL))
    bufferTime     = gconv.Int64(genv.Get("CLEAN_BUFFER_TIME", CLEAN_BUFFER_TIME))
    cleanMinSize   = gconv.Int64(genv.Get("CLEAN_MIN_SIZE", CLEAN_MIN_SIZE))
    cleanMaxSize   = gconv.Int64(genv.Get("CLEAN_MAX_SIZE", CLEAN_MAX_SIZE))
    sendMaxSize    = gconv.Int(genv.Get("SEND_MAX_SIZE", SEND_MAX_SIZE))
    debug          = gconv.Bool(genv.Get("DEBUG", DEBUG))
    kafkaAddr      = genv.Get("KAFKA_ADDR")
    kafkaTopic     = genv.Get("KAFKA_TOPIC")
    kafkaProducer  = newKafkaClientProducer()
    hostname, _    = os.Hostname()
)

func main() {
    glog.SetDebug(debug)

    // 初始化偏移量信息
    initOffsetMap()

    // 每秒保存偏移量记录
    gcron.Add("* * * * * *", saveOffsetCron)

    // 每个小时执行清理工作
    gcron.Add("0 0 * * * *", cleanLogCron)

    // 日志文件目录递归监控循环
    for {
        if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
            for _, path := range list {
                if gfile.IsFile(path) && gfile.Ext(path) != ".offsets" {
                    go trackLogFile(path)
                }
            }
        } else {
            glog.Error(err)
        }
        time.Sleep(scanInterval*time.Second)
    }
}

// 初始化偏移量信息
func initOffsetMap() {
    if gfile.Exists(offsetFilePath) {
        content := gfile.GetBinContents(offsetFilePath)
        if j, err := gjson.DecodeToJson(content); err == nil {
            for k, v := range j.ToMap(){
                glog.Debug("init file offset:", k, gconv.Int(v))
                offsetMapCache.Set(k, gconv.Int(v))
                offsetMapSave.Set(k, gconv.Int(v))
            }
        } else {
            glog.Error(err)
        }
    }
}

// 执行对指定日志文件的搜集监听
func trackLogFile(path string) {
    // 日志文件大于0才开始执行监听
    if gfile.Size(path) == 0 {
        return
    }

    // 判断goroutine是否存在
    if handlerSet.Contains(path) {
        return
    }
    handlerSet.Add(path)
    defer handlerSet.Remove(path)

    // offset初始化
    if !offsetMapCache.Contains(path) {
        offsetMapCache.Set(path, 0)
    }
    if !offsetMapSave.Contains(path) {
        offsetMapSave.Set(path, 0)
    }
    glog.Println("add log file track:", path)
    for {
        // 每隔1秒钟检查文件变化(不采用fsnotify通知机制)
        time.Sleep(time.Second)
        if size := int(gfile.Size(path)); size != 0 && size > offsetMapCache.Get(path) {
            msgs    := make([]string, 0)
            buffer  := bytes.NewBuffer(nil)
            msgSize := 0
            offset  := int64(0)
            for {
                content, pos := gfile.GetBinContentsTilCharByPath(path, '\n', int64(offsetMapCache.Get(path)))
                if pos > 0 {
                    offset = pos
                    if buffer.Len() > 0 {
                        // 判断是否多行日志数据，通过正则判断日志行首规则
                        if !gregex.IsMatch(`^\[\D+|^\[\d{4,}|^\d{4,}|^\d+\.|^time=`, content) {
                            buffer.Write(content)
                        } else {
                            if msgSize  + len(content) > sendMaxSize {
                                sendToKafka(path, msgs, pos)
                                msgs    = make([]string, 0)
                                msgSize = 0
                            }
                            msgs     = append(msgs, buffer.String())
                            msgSize += buffer.Len()
                            buffer.Reset()
                            buffer.Write(content)
                        }
                    } else {
                        buffer.Write(content)
                    }
                    offsetMapCache.Set(path, int(pos) + 1)
                } else {
                    if buffer.Len() > 0 {
                        msgs     = append(msgs, buffer.String())
                        msgSize += buffer.Len()
                        buffer.Reset()
                    }
                    break
                }
            }
            // 跳出循环后如果有数据则再次执行发送
            if len(msgs) > 0 {
                sendToKafka(path, msgs, offset)
            }
        }
    }
}

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
        for _, path := range list {
            if !gfile.IsFile(path) || gfile.Size(path) < cleanMinSize {
                continue
            }
            if gtime.Second() - gfile.MTime(path) > bufferTime {
                // 一定内没有任何更新操作，则truncate
                glog.Debug("truncate expired file:", path)
                if err := gfile.Truncate(path, 0); err == nil {
                    offsetMapCache.Remove(path)
                    offsetMapSave.Remove(path)
                } else {
                    glog.Error(err)
                }
            } else {
                // 判断文件大小，超过指定大小则truncate
                if gfile.Size(path) > cleanMaxSize {
                    glog.Debug("truncate size-exceeded file:", path)
                    if err := gfile.Truncate(path, 0); err == nil {
                        offsetMapCache.Remove(path)
                        offsetMapSave.Remove(path)
                    } else {
                        glog.Error(err)
                    }
                } else {
                    glog.Debug("leave alone file:", path)
                }
            }
        }
    } else {
        glog.Error(err)
    }
}

// 定时保存日志文件的offset记录到文件中
func saveOffsetCron() {
    if offsetMapSave.Size() == 0 {
        return
    }
    if content, err := gjson.Encode(offsetMapSave.Clone()); err != nil {
        glog.Error(err)
    } else {
        if err := gfile.PutBinContents(offsetFilePath, content); err != nil {
            glog.Error(err)
        }
    }
}

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
                        glog.Debug("send to kafka:", kafkaTopic, path, len(pkg.Msg))
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
