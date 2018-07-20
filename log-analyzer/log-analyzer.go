// 日志解析器.
// 1. 为兼容多种应用端的日志格式，这里划分的日志数据格式粒度比较大，并且很灵活；
// 2. 将kafka中的消息按照自定义的规则处理后转储到ElasticSearch中；
// Usage:
// ./log-analyzer --es-url=http://127.0.0.1:9200 --kafka-addr=127.0.0.1:9092 --kafka-topic=test

package main

import (
    "strings"
    "time"
    "context"
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
)

type Message struct {
    Time       string // 标准格式如：2006-01-02 15:04:05
    Level      string // 日志级别
    Category   string // 日志分类
    Content    string // 日志内容
    PodName    string // 来源Pod
    HostName   string // 来源Host
    FilePath   string // 来源文件路径
}

const (
    TOPIC_AUTO_CHECK_INTERVAL   = 5 // (秒)kafka topic检测时间间隔
)

var (
    // 默认通过启动参数传参
    esUrl      = gcmd.Option.Get("es-url")
    esAuthUser = gcmd.Option.Get("es-auth-user")
    esAuthPass = gcmd.Option.Get("es-auth-pass")
    kafkaAddr  = gcmd.Option.Get("kafka-addr")
    debug      = gconv.Bool(gcmd.Option.Get("debug"))
    topicSet   = gset.NewStringSet()
)

func main() {
    // 如果启动参数没有传递，那么通过环境变量传参
    if esUrl == "" {
        debug      = gconv.Bool(genv.Get("DEBUG"))
        esUrl      = genv.Get("ES_URL")
        esAuthUser = genv.Get("ES_AUTH_USER")
        esAuthPass = genv.Get("ES_AUTH_PASS")
        kafkaAddr  = genv.Get("KAFKA_ADDR")
    }
    if esUrl == "" {
        panic("Incomplete ElasticSearch setting")
    }
    if kafkaAddr == "" {
        panic("Incomplete Kafka setting")
    }

    if debug {
        glog.SetDebug(true)
    }

    for {
        kafkaClient := newKafkaClient()
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
        }
        kafkaClient.Close()
        time.Sleep(TOPIC_AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 异步处理topic日志
func handlerKafkaTopic(topic string) {
    kafkaClient   := newKafkaClient(topic)
    elasticClient := newElasticClient()
    defer func() {
        // elastic客户端不需要关闭，本身是短链接
        kafkaClient.Close()
        topicSet.Remove(topic)
    }()
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
            glog.Debugfln("receive topic [%s] msg: %s", topic, string(msg.Value))
            go handlerKafkaMessage(msg, elasticClient)
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
func handlerKafkaMessage(kafkaMsg *gkafka.Message, elasticClient *elastic.Client) {
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
        if t, err := gtime.StrToTime(msg.Time); err == nil {
            msg.Time =t.Format("2006-01-02 15:04:05")
        }
        // 向ElasticSearch发送解析后的数据
        if data, err := gjson.Encode(msg); err == nil {
            index :=  "log-" + kafkaMsg.Topic
            if _, err := elasticClient.Index().
                Index(index).Type(index).
                BodyString(string(data)).
                Do(context.Background()); err != nil {
                glog.Error(err)
            } else {
                kafkaMsg.MarkOffset()
                glog.Debugfln("pushed to ES, index: %s, body: %s", index, string(data))
            }
        } else {
            glog.Error(err)
        }
    } else {
        glog.Error(err)
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
