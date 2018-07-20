// 根据指定过期时间自动清除ElasticSearch和备份日志中的数据，过期时间单位为天。
// Usage:
// ./log-cleaner --es-url=http://127.0.0.1:9200 --expire=100

package main

import (
    "gitee.com/johng/gf/g/os/gcmd"
    "gitee.com/johng/gf/g/os/glog"
    "time"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/genv"
    "strings"
    "gitee.com/johng/gf/g/util/gconv"
    "github.com/olivere/elastic"
    "context"
    "fmt"
)

const (
    AUTO_CHECK_INTERVAL = 3600   // (秒)自动检测时间间隔
)

var (
    // 默认通过启动参数传参
    esUrl      = gcmd.Option.Get("es-url")
    esAuthUser = gcmd.Option.Get("es-auth-user")
    esAuthPass = gcmd.Option.Get("es-auth-pass")
    debug      = gconv.Bool(gcmd.Option.Get("debug"))
    expire     = gconv.Int(gcmd.Option.Get("expire"))
)

func main() {
    // 如果启动参数没有传递，那么通过环境变量传参
    if esUrl == "" {
        debug      = gconv.Bool(genv.Get("DEBUG"))
        esUrl      = genv.Get("ES_URL")
        esAuthUser = genv.Get("ES_AUTH_USER")
        esAuthPass = genv.Get("ES_AUTH_PASS")
        expire     = gconv.Int(genv.Get("EXPIRE"))
    }
    if esUrl == "" {
        panic("Incomplete ElasticSearch setting")
    }
    // 默认过期时间100天
    if expire == 0 {
        expire = 100
    }

    if debug {
        glog.SetDebug(true)
    }

    for {
        go cleanExpiredBackupFiles()
        go func() {
            esclient := newElasticClient()
            latest   := gtime.Now().Add(time.Duration(-expire*86400)).String()
            query    := fmt.Sprintf(`"range" : { "Time.keyword" : { "lte" : "%s" } }`, latest)
            if _, err := esclient.DeleteByQuery().QueryString(query).Do(context.Background()); err != nil {
                glog.Error(err)
            }
        }()
        time.Sleep(AUTO_CHECK_INTERVAL*time.Second)
    }
}

// 清除过期的备份日志文件
func cleanExpiredBackupFiles() {
    patterns := make([]string, 0)
    prefix   := "/var/log/medlinker"
    for i := 1; i <= 6; i++ {
        patterns = append(patterns, prefix + strings.Repeat("/*", i) + "/*.tar.bz2")
    }

    paths := make([]string, 0)
    for _, pattern := range patterns {
        if array , _ := gfile.Glob(pattern); len(array) > 0 {
            paths = append(paths, array...)
        }
    }

    for _, path := range paths {
        if gtime.Second() - gfile.MTime(path) >= int64(expire * 86400) {
            if err := gfile.Remove(path); err != nil {
                glog.Error(err)
            } else {
                glog.Debug("removed file:", path)
            }
        }
    }
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