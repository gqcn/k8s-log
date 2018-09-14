// 自动增加log搜集中kafka topic的副本
// 使用方式：
// auto-update-replica --runhost=xxxx --nodenum=3 --interval=60 --zookeeper=log-kafka-zookeeper:2181
package main

import (
    "gitee.com/johng/gf/g/os/gproc"
    "gitee.com/johng/gf/g/os/gcmd"
    "fmt"
    "time"
    "gitee.com/johng/gf/g/os/glog"
    "strings"
    "gitee.com/johng/gf/g"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/encoding/gjson"
    "os"
    "gitee.com/johng/gf/g/util/gregex"
)

func main() {
    hostname, _  := os.Hostname()
    runhost      := gcmd.Option.Get("runhost")
    if hostname != runhost {
        glog.Errorfln("can only run at host '%s', current hostname '%s', exit", runhost, hostname)
        return
    }
    // 等待1分钟之后启动，以便kafka已经正常启动
    time.Sleep(time.Minute)
    nodenum      := gconv.Int(gcmd.Option.Get("nodenum", "3"))
    interval     := gconv.TimeDuration(gcmd.Option.Get("interval", "60"))
    zkaddr       := gcmd.Option.Get("zookeeper")
    topicListCmd := fmt.Sprintf("/opt/kafka/bin/kafka-topics.sh --describe --zookeeper %s", zkaddr)
    replicaSlice := make([]int, nodenum)
    for i := 0; i < nodenum; i++ {
        replicaSlice[i] = i
    }
    for {
        if ouput, err := gproc.ShellExec(topicListCmd); err != nil {
            glog.Error(err)
        } else {
            for _, line := range strings.Split(ouput, "\n") {
                line = strings.TrimSpace(line)
                if len(line) == 0 {
                    continue
                }
                match, _ := gregex.MatchString(`Topic: (.+)\tPartition: (.+)\tLeader: (.+)\tReplicas: (.+)\tIsr: (.+)`, line)
                if len(match) != 6 {
                    continue
                }
                topic     := strings.TrimSpace(match[1])
                partition := gconv.Int(strings.TrimSpace(match[2]))
                replicas  := strings.TrimSpace(match[4])
                // 如果副本数已经动态扩容完成，那么不再执行
                if len(replicaSlice) == len(strings.Split(replicas, ",")) {
                    continue
                }

                // Json对象
                j := gjson.NewUnsafe()
                j.Set("version", 1)
                j.Set("partitions.0", g.Map {
                    "topic"    : topic,
                    "partition": partition,
                    "replicas" : replicaSlice,
                })

                // 更新副本的脚本分开执行，以便于记录到set中
                if b, err := j.ToJson(); err != nil {
                    glog.Error(err)
                } else {
                    path        := "/tmp/update-replica.json"
                    jsonContent := string(b)
                    if err := gfile.PutBinContents(path, b); err != nil {
                        glog.Error(err)
                    } else {
                        topicReplicaCmd := fmt.Sprintf(`/opt/kafka/bin/kafka-reassign-partitions.sh --zookeeper %s --reassignment-json-file %s --execute`, zkaddr, path)
                        glog.Debug(jsonContent, topicReplicaCmd)
                        if ouput, err := gproc.ShellExec(topicReplicaCmd); err == nil {
                            glog.Debug(ouput)
                        } else {
                            glog.Error(err)
                        }
                    }
                }
            }
        }
        time.Sleep(interval*time.Second)
    }

}
