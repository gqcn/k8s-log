// 日志搜集客户端，
// 1. 日志搜集到kafka；
// 2. 超过指定大小并已完成搜集的日志进行truncate处理；
//
// Usage:
// log-collector --pattern=xxx,xxx --kafka.url=xxx --kafka.topic=xxx [--maxsize=1024]
//
// eg:
// log-collector --pattern=/var/log/*.log,/var/log/*/*.log --kafka.url=http://127.0.0.1:9092 --kafka.topic=ucenter
package collector

import (
    "strings"
    "errors"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/container/gset"
    "gitee.com/johng/gf/g/os/gtime"
    "time"
    "gitee.com/johng/gf/g/os/gfsnotify"
    "os"
    "gitee.com/johng/gf/g/container/gtype"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/os/glog"
)

const (
    DEFAULT_LOG_DIR_SCAN_INTERVAL  = 5     // (秒)默认日志目录检索间隔
    DEFAULT_LOG_FILE_SCAN_INTERVAL = 1     // (秒)默认日志文件更新检查间隔(当fsnotify未通知时)
    DEFAULT_MAX_LOG_SIZE           = 100   // (MB)默认日志文件限制大小
    DEFAULT_MAX_IDLE_TIME          = 24    // (小时)超过该时间无任何修改操作则取消监控(如果有修改时则马上添加监控)
)

// 搜集对象
type Collector struct {
    Config      Config
    WatchingSet *gset.StringSet
    kafka       *gkafka.Client
    closed      *gtype.Bool
}

// 搜集客户端配置
type Config struct {
    KafkaUrl   string
    KafkaTopic string
    Pattern    string
    MaxSize    int
}

func New(config Config) (*Collector, error) {
    if config.KafkaUrl == "" || config.KafkaTopic == "" {
        return nil, errors.New("kafka setting cannot be empty")
    }
    if config.Pattern == "" {
        return nil, errors.New("pattern cannot be empty")
    }
    // max log file size in bytes
    if config.MaxSize == 0 {
        config.MaxSize = DEFAULT_MAX_LOG_SIZE * 1024 * 1024
    }
    kafkaConfig := gkafka.NewConfig()
    kafkaConfig.Servers = config.KafkaUrl
    kafkaConfig.Topics  = config.KafkaTopic
    c := &Collector{
        Config      : config,
        WatchingSet : gset.NewStringSet(),
        kafka       : gkafka.NewClient(kafkaConfig),
        closed      : gtype.NewBool(),
    }
    return c, nil
}

// 关闭搜集对象
func (c *Collector) Close() {
    c.closed.Set(true)
}

// 开始阻塞执行
func (c *Collector) Run() {
    c.startDirWatchingLoop()
}

// 监控日志目录变化(主要是新增文件)
func (c *Collector) startDirWatchingLoop() {
    for !c.closed.Val() {
        for _, v := range strings.Split(c.Config.Pattern, ",") {
            if list, err := gfile.Glob(v); err == nil {
                for _, path := range list {
                    go c.watchFile(path)
                }
            }
        }
        time.Sleep(DEFAULT_LOG_DIR_SCAN_INTERVAL*time.Second)
    }
}

// 监控日志文件操作
func (c *Collector) watchFile(path string) {
    if c.WatchingSet.Contains(path) {
        return
    }
    c.WatchingSet.Add(path)
    defer c.WatchingSet.Remove(path)

    // fsnotify watch
    watchChan := make(chan struct{}, 10000)
    gfsnotify.Add(path, func(event *gfsnotify.Event) {
        watchChan <- struct{}{}
    })

    // file pointer
    file, err := gfile.OpenWithFlag(path, os.O_RDONLY)
    if err != nil {
        return
    }
    defer file.Close()

    // watch loop
    offset := int64(0)
    update := int64(0)
    buffer := make([]byte, 10240)
    for !c.closed.Val() {
        select {
            case <-watchChan:
                // 同一秒钟只会操作一次，防止频繁的日志事件影响IO性能
                if gtime.Second() == update {
                    continue
                }
                update  = gtime.Second()
                n, _   := file.ReadAt(buffer, offset)
                if n > 0 {
                    lines := c.splitToLines(buffer)
                    if len(lines) > 0 {
                        for i := 0; i < len(lines); i++ {
                            if err := c.sendToKafka(lines[i]); err == nil {
                                offset += int64(len(lines[i]) + 1)
                            } else {
                                glog.Error(err)
                            }
                        }
                    }
                }

            default:
                // 每隔1秒检查日志文件大小及修改时间
                if s, err := file.Stat(); err == nil {
                    if gtime.Second() - s.ModTime().Unix() > DEFAULT_MAX_IDLE_TIME * 3600 {
                        return
                    }
                    if s.Size() > offset {
                        watchChan <- struct{}{}
                        break
                    }
                }
                time.Sleep(DEFAULT_LOG_FILE_SCAN_INTERVAL*time.Second)
        }
    }
}

// 将二进制数据拆分为行
func (c *Collector) splitToLines(data []byte) [][]byte {
    lines := make([][]byte, 0)
    start := 0
    for i := 0; i < len(data); i++ {
        if data[i] == byte('\n') {
            // 多行合并处理，当以空格开头
            if data[start : i][0] == byte(' ') && i > 0 {
                lines[i - 1] = append(lines[i - 1], data[start : i]...)
            } else {
                lines = append(lines, data[start : i])
            }
            start = i + 1
        }
    }
    return lines
}

// 向kafka发送日志数据
func (c *Collector) sendToKafka(data []byte) error {
    glog.Debug(string(data))
    return c.kafka.AsyncSend(&gkafka.Message{Value: data})
}


