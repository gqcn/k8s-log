// 容器日志搜集客户端.
// 1、监控指定目录下的日志文件，记录文件偏移量变化并持久化存储；
// 2、搜集日志文件内容，并批量提交到Kafka；
// 3、按照既定规则清理日志文件；
// 4、每天凌晨03:00定时执行清理逻辑；

package main

import (
    "bytes"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/container/gset"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gfsnotify"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gmlock"
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
    CLEAN_MAX_SIZE    = "1073741824"                 // 默认值，(byte)日志文件最大限制，当清理时执行规则处理(默认1GB)；
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
    watchedFileSet = gset.NewStringSet(true)
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
                    if !watchedFileSet.Contains(path) {
                        watchedFileSet.Add(path)
                        glog.Println("add log file track:", path)
                        gfsnotify.Add(path, func(event *gfsnotify.Event) {
                            glog.Debugfln(event.String())
                            // 如果日志文件被删除或者重命名，移除监听及记录，以便重新添加监听
                            if event.IsRename() || event.IsRemove() {
                                watchedFileSet.Remove(event.Path)
                                offsetMapCache.Remove(event.Path)
                                gfsnotify.Remove(event.Path)
                                return
                            }
                            checkLogFile(event.Path)
                        })
                        // 第一次添加后需要执行一次内容搜集(不然需要等待下一次文件写入时才开始搜集已有的内容)
                        checkLogFile(path)
                    }
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
            }
        } else {
            glog.Error(err)
        }
    }
}

// 检查文件变化，并将变化的内容提交到kafka
func checkLogFile(path string) {
    // 使用内存锁保证同一时刻只有一个goroutine在执行同一文件的日志搜集
    if gmlock.TryLock(path) {
        defer gmlock.Unlock(path)
    } else {
        return
    }
    msgs    := make([]string, 0)
    buffer  := bytes.NewBuffer(nil)
    msgSize := 0
    offset  := int64(0)
    for {
        content, pos := gfile.GetBinContentsTilCharByPath(path, '\n', int64(offsetMapCache.Get(path)))
        if pos > 0 {
            offset = pos
            if buffer.Len() > 0 {
                /*
                    判断是否多行日志数据，通过正则判断日志行首规则。
                    标准规范格式：       2018-08-08 13:01:55 DEBUG xxx
                    med3-srv-error.log:  [INFO] 2018-06-20 14:09:20 xxx
                    med-search.log:      [2018-05-24 16:10:20] product.ERROR: xxx
                    quiz-go.log:         time="2018-06-20T14:13:11+08:00" level=info msg="xxx"
                    yilian-shop-crm.log: [2018-06-20 14:10:14]  [2.85ms] xxx
                    nginx.log:           10.26.113.161 - - [2018-06-20T10:59:59+08:00] "POST xxx"
                 */
                if !gregex.IsMatch(`^\[[A-Za-z]+|^\[\d{4,}|^\d{4,}|^(\d+\.\d+\.\d+\.\d+)|^time=`, content) {
                    buffer.Write(content)
                } else {
                    if msgSize  + len(content) > sendMaxSize {
                        sendToKafka(path, msgs, offset)
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
            offsetMapCache.Set(path, int(offset) + 1)
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

