// 日志消费转储端.
// 1. 定时从kafka获取完整应用日志topic列表；
// 2. 每一个topic创建不同的协程异步消费端处理；
// 3. 将kafka对应topic中的消息转储到固定的日志文件中(NAS磁盘)；
// 4. 定时将30天之前的数据进行压缩归档并删除(原始日志文件保留30天)；
// 5. 在转储过程中需要对从各分布式节点搜集到的日志内容按照时间进行升序排序:
//     - 时间精确到毫秒
//     - 时间统一转换为UTC时区比较
//     - 缓冲区日志保留60秒的日志内容
//     - 对超过60秒的日志进行转储

package main

import (
    "fmt"
    "gitee.com/johng/gf/g/container/gmap"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/os/gcron"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/util/gregex"
    "time"
)

const (
    LOG_PATH                    = "/var/log/medlinker"         // 日志目录
    TOPIC_AUTO_CHECK_INTERVAL   = 10                           // (秒)kafka topic检测时间间隔
    KAFKA_MSG_HANDLER_NUM       = "100"                        // 并发的kafka消息消费goroutine数量
    KAFKA_MSG_SAVE_INTERVAL     = "5"                          // (秒) kafka消息内容批量保存间隔
    KAFKA_OFFSETS_DIR_NAME      = "__backupper_offsets"        // 用于保存应用端offsets的目录名称
    KAFKA_GROUP_NAME            = "group_log_archiver"         // kafka消费端分组名称
    KAFKA_GROUP_NAME_DRYRUN     = "group_log_archiver_dryrun"  // kafka消费端分组名称(dryrun)
    MAX_BUFFER_TIME_PERFILE     = "60"                         // (秒)缓冲区缓存日志的长度(按照时间衡量)
    MAX_BUFFER_LENGTH_PERFILE   = "100000"                     // 缓存区日志的容量限制，当达到容量时阻塞等待日志写入后再往缓冲区添加日志
    DRYRUN                      = "false"                      // 测试运行，不真实写入文件
    DEBUG                       = "true"                       // 默认值，是否打开调试信息

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
    logPath        = genv.Get("LOG_PATH", LOG_PATH)
    handlerChan    = make(chan struct{}, handlerSize)
    bufferMap      = gmap.NewStringInterfaceMap()
    topicMap       = gmap.NewStringInterfaceMap()
    dryrun         = gconv.Bool(genv.Get("DRYRUN", DRYRUN))
    debug          = gconv.Bool(genv.Get("DEBUG", DEBUG))
    handlerSize    = gconv.Int(genv.Get("HANDLER_SIZE", KAFKA_MSG_HANDLER_NUM))
    saveInterval   = gconv.Int(genv.Get("SAVE_INTERVAL", KAFKA_MSG_SAVE_INTERVAL))
    bufferTime     = gconv.Int64(genv.Get("MAX_BUFFER_TIME_PERFILE", MAX_BUFFER_TIME_PERFILE))
    bufferLength   = gconv.Int(genv.Get("MAX_BUFFER_LENGTH_PERFILE", MAX_BUFFER_LENGTH_PERFILE))
    kafkaAddr      = genv.Get("KAFKA_ADDR")
    kafkaClient    = newKafkaClient()
    pkgCache       = gcache.New()
)

func main() {
    // 是否显示调试信息
    glog.SetDebug(debug)

    // 定时压缩归档任务
    gcron.Add("0 0 3 * * *", handlerArchiveCron)

    // 定时批量写日志到文件
    gcron.Add(fmt.Sprintf(`*/%d * * * * *`, saveInterval), handlerSavingContent)

    for {
       if topics, err := kafkaClient.Topics(); err == nil {
          for _, topic := range topics {
              // 只处理新版处理程序处理 *.v2 的topic
              if !topicMap.Contains(topic) && gregex.IsMatchString(`.+\.v3`, topic) {
                  glog.Debugfln("add new topic handle: %s", topic)
                  topicMap.Set(topic, gmap.NewStringIntMap())
                  go handlerKafkaTopic(topic)
              } else {
                  //glog.Debug("no match topic:", topic)
              }
          }
       } else {
          glog.Error(err)
          break
       }
       time.Sleep(TOPIC_AUTO_CHECK_INTERVAL*time.Second)
    }
}

