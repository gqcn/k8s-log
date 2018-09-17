package main

import (
    "gitee.com/johng/gf/g"
    "strings"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gfile"
    "fmt"
    "gitee.com/johng/gf/g/util/gregex"
    "gitee.com/johng/gf/g/util/gutil"
)

func main() {
    line := `	Topic: __consumer_offsets	Partition: 49	Leader: 0	Replicas: 0	Isr: 0`
    gutil.Dump(gregex.MatchString(`Topic: (.+)\tPartition: (.+)\tLeader: (.+)\tReplicas: (.+)\tIsr: (.+)`, line))
    return
    ouput := `
__consumer_offsets
crontab
fzapi
grape
grape-http
med-auth
med-d2d
med-d2d-ph
med-push
med-quiz
med-search
med-user
med3-app
med3-app-web
med3-passport
med3-svr
network-medical-faculty
supervisor
`

    for _, line := range strings.Split(ouput, "\n") {
        topic := strings.TrimSpace(line)
        if len(line) == 0 || line[0] == '_' {
            continue
        }
        // Json对象
        j := gjson.NewUnsafe()
        j.Set("version", 1)
        j.Set("partitions.0", g.Map {
            "topic"    : topic,
            "partition": 0,
            "replicas" : []interface{}{0, 1, 2},
        })
        // 更新副本的脚本分开执行，以便于记录到set中
        if b, err := j.ToJson(); err != nil {
            glog.Error(err)
        } else {
            path := "/tmp/update-replica.json"
            fmt.Println(string(b))
            if err := gfile.PutBinContents(path, b); err != nil {
                glog.Error(err)
            } else {

            }
        }
    }

}