// 业务日志解析器.
// 1. 为兼容多种应用端的日志格式，这里划分的日志数据格式粒度比较大，并且很灵活；
// 2. 将kafka中的消息按照自定义的规则处理后转储到ElasticSearch中；

package main

import (
    "strings"
    "time"
    "gitee.com/johng/gf/g/os/gcmd"
    "github.com/olivere/elastic"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/util/gregex"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/os/genv"
    "gitee.com/johng/gf/g/container/gset"
    "gitee.com/johng/gf/g/util/gconv"
    "context"
    "fmt"
)

// ES数据结构
type Message struct {
    Time       string   // 标准格式如：2006-01-02 15:04:05
    Level      string   // 日志级别
    Category   string   // 日志分类
    Content    string   // 日志内容
    PodName    string   // 来源Pod
    HostName   string   // 来源Host
    FilePath   string   // 来源文件路径
}

// 往ES发送数据的队列项
type SendItem struct {
    Id      string
    Index   string
    Type    string
    Content string
    Msg     *gkafka.Message
}

const (
    VERSION                     = "20180807.5" // 版本号
    TOPIC_AUTO_CHECK_INTERVAL   = 5            // (秒)kafka topic检测时间间隔
    DEFAULT_ES_BULK_SIZE        = 1000         // 往ES一次批量发送数据大小(注意大小不要超过15MB)
    DEFAULT_ES_QUEUE_SIZE       = 50           // 没有设置bulk-size时ES并发批量写数据数量(由于ES写数据比较慢，避免es队列满的情况报429错误)
    KAFKA_MSG_HANDLER_NUM       = 10           // 并发的kafka消息消费goroutine数量
)

var (
    // 默认通过启动参数传参
    esUrl       = gcmd.Option.Get("es-url")
    esAuthUser  = gcmd.Option.Get("es-auth-user")
    esAuthPass  = gcmd.Option.Get("es-auth-pass")
    // 控制一次批量发送的消息数量
    esBulkSize  = gconv.Int(gcmd.Option.Get("es-bulk-size"))
    // 控制并发往ES发送的请求数量
    esQueueSize = gconv.Int(gcmd.Option.Get("es-queue-size"))
    // 控制从kafka并发解析的goroutine数量
    handlerSize = gconv.Int(gcmd.Option.Get("handler-size"))
    kafkaAddr   = gcmd.Option.Get("kafka-addr")
    debug       = gconv.Bool(gcmd.Option.Get("debug"))
    topicSet    = gset.NewStringSet()
    esSendQueue  chan *SendItem
    esFailQueue  chan *SendItem
    esTaskQueue  chan struct{}
    kafkaMsgChan chan struct{}

)

func main() {
    glog.Println("Version:", VERSION)
    //gtime.SetTimeZone("CST")
    // 如果启动参数没有传递，那么通过环境变量传参
    if esUrl == "" {
        debug       = gconv.Bool(genv.Get("DEBUG"))
        esUrl       = genv.Get("ES_URL")
        esAuthUser  = genv.Get("ES_AUTH_USER")
        esAuthPass  = genv.Get("ES_AUTH_PASS")
        esBulkSize  = gconv.Int(genv.Get("ES_BULK_SIZE"))
        esQueueSize = gconv.Int(genv.Get("ES_QUEUE_SIZE"))
        handlerSize = gconv.Int(genv.Get("HANDLER_SIZE"))
        kafkaAddr   = genv.Get("KAFKA_ADDR")
    }
    if esUrl == "" {
        panic("Incomplete ElasticSearch setting")
    }
    if kafkaAddr == "" {
        panic("Incomplete Kafka setting")
    }

    if esBulkSize == 0 {
        esBulkSize = DEFAULT_ES_BULK_SIZE
    }
    if esQueueSize == 0 {
        esQueueSize = DEFAULT_ES_QUEUE_SIZE
    }
    if handlerSize == 0 {
        handlerSize = KAFKA_MSG_HANDLER_NUM
    }

    glog.SetDebug(debug)

    // 用于控制往ES的写入速度(最大并发数)
    esTaskQueue  = make(chan struct{}, esQueueSize)

    // 用于限制kafka消费异步gorutine数量
    kafkaMsgChan = make(chan struct{}, handlerSize)

    // bulk发送队列，限制队列大小为bulkSize的10倍
    esSendQueue = make(chan *SendItem, esBulkSize*10)
    esFailQueue = make(chan *SendItem, esBulkSize*10)

    // 异步ES数据队列处理及批量发送处理循环
    go autoSendToESByBulkOrTimerLoop()

    kafkaClient := newKafkaClient()
    defer kafkaClient.Close()
    for {
        if topics, err := kafkaClient.Topics(); err == nil {
            for _, topic := range topics {
                // 只能处理旧版topic，新版处理程序处理*.v2的topic
                if !topicSet.Contains(topic) && !gregex.IsMatchString(`.+\.v2`, topic){
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
            kafkaMsgChan <- struct{}{}
            go func() {
                handlerKafkaMessage(msg)
                <- kafkaMsgChan
            }()
        } else {
            glog.Error(err)
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
        kafkaConfig.GroupId = "group_" + topic[0] + "_analyzer"
    } else {
        kafkaConfig.GroupId = "group_default_analyzer"
    }
    return gkafka.NewClient(kafkaConfig)
}

// 创建ElasticSearch客户端
func newElasticClient() *elastic.Client {
    esOptions := make([]elastic.ClientOptionFunc, 0)
    esOptions  = append(esOptions, elastic.SetURL(esUrl))
    if esAuthUser != "" {
        esOptions = append(esOptions, elastic.SetBasicAuth(esAuthUser, esAuthPass))
    }
    if client, err := elastic.NewClient(esOptions...); err == nil {
        return client
    } else {
        glog.Error(err)
    }
    return nil
}

// 处理kafka消息
func handlerKafkaMessage(kafkaMsg *gkafka.Message) {
    if j, err := gjson.DecodeToJson(kafkaMsg.Value); err == nil {
        msg := &Message {
            Time       : parserFileBeatTime(j.GetString("@timestamp")),
            Content    : j.GetString("message"),
            PodName    : j.GetString("beat.name"),
            HostName   : j.GetString("beat.hostname"),
            FilePath   : j.GetString("source"),
        }
        // 日志分类，格式规范：/var/log/medlinker/{AppName}/{Category}/YYYY-MM-DD.log
        // 兼容已有旧日志格式：/var/log/medlinker/{AppName}/xxx/xxx/xxx/YYYY-MM-DD.log
        array := strings.Split(gfile.Dir(msg.FilePath), "/")
        if len(array) >= 6 {
            // 注意：array[0]为空
            msg.Category = strings.Join(array[5:], "/")
        }
        checkPatternWithMsg(msg)
        // 处理时间中的格式，转换为标准格式
        if t := gtime.NewFromStr(msg.Time); t.IsZero() {
            msg.Time = gtime.Now().Format("Y-m-d H:i:s")
        }
        // 向ElasticSearch发送解析后的数据
        if data, err := gjson.Encode(msg); err == nil {
            index := fmt.Sprintf("log-%s-%s", kafkaMsg.Topic, gtime.NewFromStr(msg.Time).Format("Ymd"))
            esSendQueue <- &SendItem {
                Id      : gconv.String(gtime.Nanosecond()),
                Index   : index,
                Type    : index,
                Content : string(data),
                Msg     : kafkaMsg,
            }
            kafkaMsg.MarkOffset()
        } else {
            glog.Error(err)
        }
    } else {
        glog.Error(err)
    }
}

// 自动往ES批量写入数据的循环goroutine
func autoSendToESByBulkOrTimerLoop() {
    elasticClient := newElasticClient()
    for {
        bulkRequest   := elasticClient.Bulk()
        bulkSendItems := make(map[string]*SendItem)
        for {
            select {
                // 失败队列
                case item := <- esFailQueue:
                    bulkRequest            = bulkRequest.Add(elastic.NewBulkIndexRequest().Index(item.Index).Type(item.Type).Id(item.Id).Doc(item.Content))
                    bulkSendItems[item.Id] = item
                    if bulkRequest.NumberOfActions() == esBulkSize {
                        goto ToSendBulk
                    }

                    // 正常队列
                case item := <- esSendQueue:
                    bulkRequest            = bulkRequest.Add(elastic.NewBulkIndexRequest().Index(item.Index).Type(item.Type).Id(item.Id).Doc(item.Content))
                    bulkSendItems[item.Id] = item
                    if bulkRequest.NumberOfActions() == esBulkSize {
                        goto ToSendBulk
                    }

                default:
                    goto ToSendBulk
            }
        }

    ToSendBulk:
        if bulkRequest.NumberOfActions() > 0 {
            esTaskQueue <- struct{}{}
            // 异步往ES发送bulk批量数据写入请求，使用esTaskQueue控制并发数量
            go func(req *elastic.BulkService) {
                //glog.Debug("time to send bulk:", req.NumberOfActions())
                t1 := gtime.Millisecond()
                bulkResponse, err := req.Do(context.Background())
                if bulkResponse == nil {
                    return
                }
                glog.Debugfln("bulk send cost: %d ms, success: %d, failed: %d, error: %v",
                    gtime.Millisecond() - t1,
                    len(bulkResponse.Succeeded()),
                    len(bulkResponse.Failed()),
                    err)

                // 成功项，标记offset
                for _, v := range bulkResponse.Succeeded() {
                    if item, ok := bulkSendItems[v.Id]; ok {
                        item.Msg.MarkOffset()
                    }
                }

                // 失败项，推入失败队列下次重新发送
                for _, v := range bulkResponse.Failed() {
                    if item, ok := bulkSendItems[v.Id]; ok {
                        esFailQueue <- item
                    }
                }
                <- esTaskQueue
            }(bulkRequest)
        } else {
            time.Sleep(time.Second)
        }
    }
}

// 正则模式匹配。
// 为便于前期兼容旧的日志格式，该方法中存在多种自定义的日志内容匹配规则，后期慢慢规范化之后只会存在一种规则。
func checkPatternWithMsg(msg *Message) {
    // 标准规范格式：2018-08-08 13:01:55 DEBUG xxx
    if match, _ := gregex.MatchString(`^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+([\w\.]+)\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // med3-srv-error.log: [INFO] 2018-06-20 14:09:20 xxx
    if match, _ := gregex.MatchString(`^\[([\w\.]+)\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[2]
        msg.Level   = match[1]
        msg.Content = match[3]
        return
    }
    // med-search.log: [2018-05-24 16:10:20] product.ERROR: xxx
    if match, _ := gregex.MatchString(`^\[([\d\-\:\s]+?)\]\s+([\w\.]+):\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // quiz-go.log: time="2018-06-20T14:13:11+08:00" level=info msg="xxx"
    if match, _ := gregex.MatchString(`^time="(.+?)"\s+level=(.+?)\s+msg="([\w\W]+?)"`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // yilian-shop-crm.log: [2018-06-20 14:10:14]  [2.85ms] xxx
    if match, _ := gregex.MatchString(`^\[([\d\-\:\s]+)\]\s+([\w\W]+)`, msg.Content); len(match) >= 3 {
        msg.Time    = match[1]
        msg.Content = match[2]
        return
    }
    // 什么都没匹配，那么只匹配其中的日期出来，其他不做处理
    // nginx.log: 10.26.113.161 - - [2018-06-20T10:59:59+08:00] "POST xxx"
    if match, _ := gregex.MatchString(`\[([TZ\d\-\:\s\+]+?)\]\s+([\w\W]+)`, msg.Content); len(match) >= 2 {
        msg.Time    = match[1]
        return
    }
}

// 解析filebeat的ISODate时间为标准的日期时间字符串
func parserFileBeatTime(datetime string) string {
    if t, err := gtime.StrToTime(datetime); err == nil {
        t = t.Add(8 * time.Hour)
        return t.Format("2006-01-02 15:04:05")
    }
    return ""
}
