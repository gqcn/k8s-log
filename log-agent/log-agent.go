// 日志搜集客户端，与filebeat、logrotate一起用，用于清理不活动的logrotate切分文件。
// 由于运行在容器中，所有变量通过环境变量配置


package main

import (
    "gitee.com/johng/gf/g/os/gfile"
    "time"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gtime"
    "gitee.com/johng/gf/g/util/gconv"
    "gitee.com/johng/gf/g/os/genv"
)

const (
    LOG_PATH      = "/var/log/medlinker" // 默认值，日志目录绝对路径
    SCAN_INTERVAL = "60"                 // 默认值，(秒)目录检测间隔
    EXPIRE_TIME   = "3600"               // 默认值，(秒)当超过多少时间没有更新时执行删除
    DEBUG         = "true"               // 默认值，是否打开调试信息
)

var (
    logPath      = genv.Get("LOG_PATH", LOG_PATH)
    scanInterval = gconv.TimeDuration(genv.Get("SCAN_INTERVAL", SCAN_INTERVAL))
    expireTime   = gconv.Int64(genv.Get("EXPIRE_TIME", EXPIRE_TIME))
    debug        = gconv.Bool(genv.Get("DEBUG", DEBUG))
)

func main() {
    glog.SetDebug(debug)
    for {
        glog.Debug("start scanning...")
        if list, err := gfile.ScanDir(logPath, "*.log.*", true); err == nil {
            for _, path := range list {
                if gtime.Second() - gfile.MTime(path) >= expireTime {
                    if err := gfile.Remove(path); err != nil {
                        glog.Error(err)
                    } else {
                        glog.Debug("removed file:", path)
                    }
                }
            }
        } else {
            glog.Error(err)
        }
        time.Sleep(scanInterval*time.Second)
    }
}
