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
    "gitee.com/johng/gf/g/util/gregx"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/database/gkafka"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/os/genv"
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
var (
    kafkaClient   *gkafka.Client
    elasticClient *elastic.Client
)

func main() {
    // input params check
    esUrl      := gcmd.Option.Get("es-url")
    esAuthUser := gcmd.Option.Get("es-auth-user")
    esAuthPass := gcmd.Option.Get("es-auth-pass")
    kafkaAddr  := gcmd.Option.Get("kafka-addr")
    kafkaTopic := gcmd.Option.Get("kafka-topic")
    if esUrl == "" {
        esUrl      = genv.Get("ES_URL")
        esAuthUser = genv.Get("ES_AUTH_USER")
        esAuthPass = genv.Get("ES_AUTH_PASS")
        kafkaAddr  = genv.Get("KAFKA_ADDR")
        kafkaTopic = genv.Get("KAFKA_TOPIC")
    }

    if esUrl == "" {
        panic("Incomplete ElasticSearch setting")
    }
    if kafkaAddr == "" || kafkaTopic == "" {
        panic("Incomplete Kafka setting")
    }
    // ElasticSearch client
    esOptions := make([]elastic.ClientOptionFunc, 0)
    esOptions  = append(esOptions, elastic.SetURL(esUrl))
    if esAuthUser != "" {
        esOptions = append(esOptions, elastic.SetBasicAuth(esAuthUser, esAuthPass))
    }
    if client, err := elastic.NewClient(esOptions...); err == nil {
        elasticClient = client
    } else {
        panic(err)
    }
    // Kafka client
    kafkaConfig        := gkafka.NewConfig()
    kafkaConfig.Servers = kafkaAddr
    kafkaConfig.Topics  = kafkaTopic
    kafkaConfig.GroupId = "group_" + kafkaTopic + "_analyzer"
    kafkaClient = gkafka.NewClient(kafkaConfig)

    // 监听处理循环
    for {
        if msg, err := kafkaClient.Receive(); err == nil {
            go handlerKafkaMessage(msg)
        } else {
            glog.Error(err)
        }
    }
}

// 处理kafka消息
func handlerKafkaMessage(message *gkafka.Message) {
    if j, err := gjson.DecodeToJson(message.Value); err == nil {
        j.SetViolenceCheck(false)
        msg := &Message {
            Time       : parserFileBeatTime(j.GetString("@timestamp")),
            Content    : j.GetString("message"),
            PodName    : j.GetString("beat.name"),
            HostName   : j.GetString("beat.hostname"),
            FilePath   : j.GetString("source"),
        }
        // 日志分类，格式规范：/var/log/medlinker/{AppName}/{Category}/YYYY-MM-DD.log
        array := strings.Split(gfile.Dir(msg.FilePath), "/")
        if len(array) > 4 {
            msg.Category = array[4]
        }
        checkPatternWithMsg(msg)
        // 处理时间中的格式，转换为标准格式
        if t, err := gtime.StrToTime(msg.Time); err == nil {
            msg.Time =t.Format("2006-01-02 15:04:05")
        }
        if data, err := gjson.Encode(msg); err == nil {
            //fmt.Println(string(message.Value))
            //b, _ := gparser.VarToJsonIndent(msg)
            //fmt.Println(string(b))
            //fmt.Println()
            sendToElasticSearch(message.Topic, string(data))
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
    if match, _ := gregx.MatchString(`^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+([\w\.]+)\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // med3-srv-error.log: [INFO] 2018-06-20 14:09:20 xxx
    if match, _ := gregx.MatchString(`^\[([\w\.]+)\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[2]
        msg.Level   = match[1]
        msg.Content = match[3]
        return
    }
    // med-search.log: [2018-05-24 16:10:20] product.ERROR: xxx
    if match, _ := gregx.MatchString(`^\[([\d\-\:\s]+?)\]\s+([\w\.]+):\s+([\w\W]+)`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // quiz-go.log: time="2018-06-20T14:13:11+08:00" level=info msg="xxx"
    if match, _ := gregx.MatchString(`^time="(.+?)"\s+level=(.+?)\s+msg="([\w\W]+?)"`, msg.Content); len(match) >= 4 {
        msg.Time    = match[1]
        msg.Level   = match[2]
        msg.Content = match[3]
        return
    }
    // yilian-shop-crm.log: [2018-06-20 14:10:14]  [2.85ms] xxx
    if match, _ := gregx.MatchString(`^\[([\d\-\:\s]+)\]\s+([\w\W]+)`, msg.Content); len(match) >= 3 {
        msg.Time    = match[1]
        msg.Content = match[2]
        return
    }
    // 什么都没匹配，那么只匹配其中的日期出来，其他不做处理
    // nginx.log: 10.26.113.161 - - [2018-06-20T10:59:59+08:00] "POST xxx"
    if match, _ := gregx.MatchString(`\[([TZ\d\-\:\s\+]+?)\]\s+([\w\W]+)`, msg.Content); len(match) >= 2 {
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

// 向ElasticSearch发送解析后的数据
func sendToElasticSearch(topic string, data string) {
    index :=  "log-" + topic
    if _, err := elasticClient.Index().Index(index).Type(index).BodyString(data).Do(context.Background()); err != nil {
        glog.Error(err)
    }
}
