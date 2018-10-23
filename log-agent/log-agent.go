// 容器日志搜集客户端，参考方案说明：http://gitlab.dev.okapp.cc:81/foundations/inaction/blob/master/实施/1.2、日志搜集/日志搜集改进.MD
// 1、监控指定目录下的日志文件，记录文件偏移量变化；
// 2、搜集日志文件内容，并批量提交到Kafka；
// 3、按照既定规则清理日志文件；
// 4、每天凌晨03:00定时执行清理；

package main

import (
    "bytes"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gregex"
    "time"
)

const (
    LOG_PATH          = "/var/log/medlinker"         // 默认值，日志目录绝对路径
    OFFSET_FILE_PATH  = "/var/log/log-agent.offsets" // 默认值，偏移量保存map，用于系统重启时恢复日志偏移量读取，继续文件的搜集位置
    SCAN_INTERVAL     = "10"                         // 默认值，(秒)目录检测间隔，检测新日志文件的创建
    CLEAN_BUFFER_TIME = "86400"                      // 默认值，(秒)当执行清理时，超过多少时间没有更新则执行删除(默认3小时)
    CLEAN_MAX_SIZE    = "1073741824"                 // 默认值，(byte)日志文件最大限制，当清理时执行规则处理；
    SEND_MAX_SIZE     = "1048576"                    // 默认值，(byte)每条消息发送时的最大值(包大小限制, 默认1MB)
    DEBUG             = "true"                       // 默认值，是否打开调试信息
)

// kafka消息数据结构
type Message struct {
    Path string   `json:"path"` // 日志文件路径
    Msgs []string `json:"msgs"` // 日志内容(多条)
    Time string   `json:"time"` // 发送时间(客户端搜集时间)
}

var (
    // 记录日志文件搜集的offset
    offsetMap      = gmap.NewStringIntMap(true)
    // 以下为通过环境变量传递的参数
    logPath        = genv.Get("LOG_PATH", LOG_PATH)
    offsetFilePath = genv.Get("OFFSET_FILE_PATH", OFFSET_FILE_PATH)
    scanInterval   = gconv.TimeDuration(genv.Get("SCAN_INTERVAL", SCAN_INTERVAL))
    bufferTime     = gconv.Int64(genv.Get("CLEAN_BUFFER_TIME", CLEAN_BUFFER_TIME))
    cleanMaxSize   = gconv.Int(genv.Get("CLEAN_MAX_SIZE", CLEAN_MAX_SIZE))
    sendMaxSize    = gconv.Int(genv.Get("SEND_MAX_SIZE", SEND_MAX_SIZE))
    debug          = gconv.Bool(genv.Get("DEBUG", DEBUG))
    kafkaAddr      = genv.Get("KAFKA_ADDR")
    kafkaTopic     = genv.Get("KAFKA_TOPIC")
    kafkaProducer  = newKafkaClientProducer()
)

func main() {
    glog.SetDebug(debug)

    // 初始化偏移量信息
    initOffsetMap()

    // 每秒保存偏移量记录
    gcron.Add("* * * * * *", saveOffsetCron)

    // 每天凌晨03:00执行清理工作
    gcron.Add("0 0 3 * * *", cleanLogCron)

    // 日志文件目录递归监控循环
    for {
        if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
            for _, path := range list {
                if gfile.IsFile(path) && !offsetMap.Contains(path) {
                    offsetMap.Set(path, 0)
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
                glog.Debug("init file offset:", k, v)
                offsetMap.Set(k, gconv.Int(v))
            }
        } else {
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

// 自动清理日志文件
func cleanLogCron() {
    if list, err := gfile.ScanDir(logPath, "*", true); err == nil {
        for _, path := range list {
            if gtime.Second() - gfile.MTime(path) > bufferTime {
                // 一定内没有任何更新操作，则删除
                gfile.Remove(path)
                offsetMap.Remove(path)
            } else {
                // 判断文件大小，超过指定大小则truncate
                if gfile.Size(path) > int64(cleanMaxSize) {
                    if err := gfile.Truncate(path, 0); err == nil {
                        offsetMap.Remove(path)
                    } else {
                        glog.Error(err)
                    }
                }
            }
        }
    } else {
        glog.Error(err)
    }
}

// 定时保存日志文件的offset记录到文件中
func saveOffsetCron() {
    if offsetMap.Size() == 0 {
        return
    }
    if content, err := gjson.Encode(offsetMap.Clone()); err != nil {
        glog.Error(err)
    } else {
        if err := gfile.PutBinContents(offsetFilePath, content); err != nil {
            glog.Error(err)
        }
    }
}

// 执行对指定日志文件的搜集监听
func trackLogFile(path string) {
    glog.Println("add log file track:", path)
    for {
        // 每隔1秒钟检查文件变化(不采用fsnotify通知机制)
        time.Sleep(time.Second)
        if size := int(gfile.Size(path)); size != 0 && size > offsetMap.Get(path) {
            msgs    := make([]string, 0)
            buffer  := bytes.NewBuffer(nil)
            msgSize := 0
            for {
                content, offset := gfile.GetBinContentsTilCharByPath(path, '\n', int64(offsetMap.Get(path)))
                if offset > 0 {
                    if buffer.Len() > 0 {
                        // 判断是否多行日志数据，通过正则判断日志行首规则
                        if !gregex.IsMatch(`^\[\D+|^\[\d{4,}|^\d{4,}|^\d+\.|^time=`, content) {
                            buffer.Write(content)
                        } else {
                            msgs     = append(msgs, buffer.String())
                            msgSize += buffer.Len()
                            buffer.Reset()
                            buffer.Write(content)
                        }
                        // 在循环中判断包大小
                        if msgSize > sendMaxSize {
                            sendToKafka(path, msgs)
                            msgs    = make([]string, 0)
                            msgSize = 0
                        }
                    } else {
                        buffer.Write(content)
                    }
                    offsetMap.Set(path, int(offset) + 1)
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
                sendToKafka(path, msgs)
            }
        }
    }
}

// 向kafka发送日志内容(异步)
// 如果发送失败，那么每隔1秒阻塞重试
func sendToKafka(path string, msgs []string) {
    msg := Message{
        Path : path,
        Msgs : msgs,
        Time : gtime.Now().String(),
    }
    for {
        if content, err := gjson.Encode(msg); err != nil {
            glog.Error(err)
        } else {
            glog.Debug("send to kafka:", kafkaTopic, path, len(content))
            if err := kafkaProducer.AsyncSend(&gkafka.Message{Value : content}); err != nil {
                glog.Error(err)
            } else {
                break
            }
        }
        time.Sleep(time.Second)
    }
}

